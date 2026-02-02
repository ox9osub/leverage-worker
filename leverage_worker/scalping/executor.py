"""
스캘핑 실행기 (상태 머신)

WebSocket tick 기반으로 P10 매수 → +0.1% 매도를 반복 실행
"""

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from leverage_worker.websocket.ws_client import RealtimeWSClient

from leverage_worker.scalping.models import ScalpingConfig, ScalpingSignalContext, ScalpingState
from leverage_worker.scalping.price_tracker import PriceRangeTracker
from leverage_worker.trading.broker import KISBroker, OrderResult, OrderSide
from leverage_worker.utils.logger import get_logger

logger = get_logger("scalping.executor")


# KRX 호가 단위 테이블 (ETF 기준: 2,000원 미만 1원, 이상 5원)
_TICK_SIZE_TABLE = [
    (2_000, 1),
    (float("inf"), 5),
]


def round_to_tick_size(price: int, direction: str = "down") -> int:
    """
    KRX 호가 단위에 맞게 가격 반올림

    Args:
        price: 원래 가격
        direction: "down" (매수용, 내림) / "up" (매도용, 올림)
    """
    tick = 1
    for threshold, t in _TICK_SIZE_TABLE:
        if price < threshold:
            tick = t
            break

    if direction == "down":
        return (price // tick) * tick
    else:
        return ((price + tick - 1) // tick) * tick


class ScalpingExecutor:
    """
    스캘핑 매매 상태 머신

    시그널 활성 후 WebSocket tick을 받아 P10 매수 → +0.1% 매도를 반복.
    TP/SL/timeout으로 시그널 수명이 다하면 종료.
    """

    def __init__(
        self,
        stock_code: str,
        stock_name: str,
        config: ScalpingConfig,
        broker: KISBroker,
        strategy_name: str = "scalping_range",
        allocation: float = 100.0,
        ws_client: Optional["RealtimeWSClient"] = None,
    ) -> None:
        self._stock_code = stock_code
        self._stock_name = stock_name
        self._config = config
        self._broker = broker
        self._strategy_name = strategy_name
        self._allocation = allocation
        self._ws_client = ws_client

        # 상태
        self._state = ScalpingState.IDLE
        self._signal_ctx: Optional[ScalpingSignalContext] = None
        self._price_tracker = PriceRangeTracker(
            window_seconds=config.window_seconds,
            max_window_seconds=config.max_window_seconds,
        )

        # 현재 주문 추적
        self._buy_order_id: Optional[str] = None
        self._buy_order_branch: Optional[str] = None
        self._buy_order_price: int = 0
        self._buy_order_qty: int = 0
        self._buy_order_time: Optional[datetime] = None

        self._sell_order_id: Optional[str] = None
        self._sell_order_branch: Optional[str] = None
        self._sell_order_price: int = 0
        self._sell_order_qty: int = 0

        # 포지션 추적 (executor 자체 관리)
        self._held_qty: int = 0
        self._held_avg_price: float = 0.0

        # 쿨다운
        self._cooldown_start: Optional[datetime] = None

        # 체결 확인 스로틀링 (API 호출 제한)
        self._last_order_check_time: Optional[datetime] = None
        self._order_check_interval: float = 3.0  # 초 단위 (3초마다 balance 확인)

        # 스레드 안전
        self._lock = threading.Lock()

    # ──────────────────────────────────────────
    # 외부 인터페이스
    # ──────────────────────────────────────────

    @property
    def state(self) -> ScalpingState:
        return self._state

    @property
    def is_active(self) -> bool:
        return self._state != ScalpingState.IDLE

    @property
    def signal_context(self) -> Optional[ScalpingSignalContext]:
        return self._signal_ctx

    def activate_signal(
        self,
        signal_price: int,
        tp_pct: float,
        sl_pct: float,
        timeout_minutes: int,
    ) -> None:
        """시그널 활성화 → MONITORING 상태 진입"""
        with self._lock:
            if self._state != ScalpingState.IDLE:
                logger.warning(
                    f"[scalping][{self._stock_code}] "
                    f"시그널 무시: 이미 활성 상태 ({self._state.value})"
                )
                return

            self._signal_ctx = ScalpingSignalContext(
                signal_price=signal_price,
                signal_time=datetime.now(),
                tp_pct=tp_pct,
                sl_pct=sl_pct,
                timeout_minutes=timeout_minutes,
            )
            self._price_tracker.reset()
            self._transition(ScalpingState.MONITORING)

            logger.info(
                f"[scalping][{self._stock_name}] 시그널 활성화: "
                f"signal_price={signal_price:,}, "
                f"TP={tp_pct*100:.1f}%({self._signal_ctx.tp_price:,}), "
                f"SL={sl_pct*100:.1f}%({self._signal_ctx.sl_price:,}), "
                f"timeout={timeout_minutes}분"
            )

    def on_tick(self, price: int, timestamp: datetime) -> None:
        """
        WebSocket tick 수신 시 호출

        모든 상태에서 tick을 price_tracker에 누적하고,
        현재 상태에 따라 적절한 핸들러 호출.
        """
        with self._lock:
            if self._state == ScalpingState.IDLE:
                return

            # 항상 price_tracker에 누적
            self._price_tracker.add_tick(timestamp, price)

            # 시그널 수명 만료 체크 (모든 활성 상태에서)
            if self._signal_ctx:
                expired, reason = self._signal_ctx.is_expired(timestamp, price)
                if expired:
                    self._handle_signal_expired(reason, price)
                    return

            # 상태별 핸들러 디스패치
            handlers = {
                ScalpingState.MONITORING: self._handle_monitoring,
                ScalpingState.BUY_PENDING: self._handle_buy_pending,
                ScalpingState.POSITION_HELD: self._handle_position_held,
                ScalpingState.SELL_PENDING: self._handle_sell_pending,
                ScalpingState.COOLDOWN: self._handle_cooldown,
            }
            handler = handlers.get(self._state)
            if handler:
                handler(price, timestamp)

    def process_ws_fill(
        self, order_no: str, filled_qty: int, filled_price: int
    ) -> bool:
        """
        WebSocket 체결통보 처리 (TradingEngine에서 라우팅)

        Args:
            order_no: 주문번호
            filled_qty: 체결 수량
            filled_price: 체결 단가

        Returns:
            True if this executor handled the order, False otherwise
        """
        with self._lock:
            # Check if this is our order
            if order_no != self._buy_order_id and order_no != self._sell_order_id:
                return False

            if order_no == self._buy_order_id:
                return self._handle_ws_buy_fill(filled_qty, filled_price)
            elif order_no == self._sell_order_id:
                return self._handle_ws_sell_fill(filled_qty, filled_price)

        return False

    def _handle_ws_buy_fill(self, filled_qty: int, filled_price: int) -> bool:
        """매수 체결 WS 처리 (증분)"""
        # EC-2: 중복 알림 방지 - remaining_qty 초과 불가
        remaining_qty = self._buy_order_qty - self._held_qty
        actual_fill = min(filled_qty, remaining_qty)

        if actual_fill <= 0:
            logger.debug(
                f"[WS] 중복 체결 알림 무시: order={self._buy_order_id}, "
                f"filled_qty={filled_qty}, already_held={self._held_qty}"
            )
            return True  # Handled (not an error)

        # Update position (cumulative)
        self._update_position(self._held_qty + actual_fill, filled_price)

        # Check if fully filled
        if self._held_qty >= self._buy_order_qty:
            # Full fill → immediate sell
            self._clear_buy_order()
            self._place_sell_order()
            logger.info(
                f"[WS] 전량 매수 체결 (+{actual_fill}주) → 즉시 매도: "
                f"{self._held_qty}주 @ {self._held_avg_price:,.0f}원"
            )
            return True

        # Partial fill → POSITION_HELD
        if self._state == ScalpingState.BUY_PENDING:
            self._transition(ScalpingState.POSITION_HELD)
            logger.info(
                f"[WS] 부분 매수 체결 (+{actual_fill}주): "
                f"{self._held_qty}/{self._buy_order_qty}주 @ {self._held_avg_price:,.0f}원"
            )
        else:
            # POSITION_HELD에서 추가 체결
            logger.info(
                f"[WS] 추가 매수 체결 (+{actual_fill}주): "
                f"{self._held_qty}/{self._buy_order_qty}주 @ {self._held_avg_price:,.0f}원"
            )

        return True

    def _handle_ws_sell_fill(self, filled_qty: int, filled_price: int) -> bool:
        """매도 체결 WS 처리 (증분)"""
        # EC-2: 중복 알림 방지 - 이미 COOLDOWN이면 무시
        if self._state == ScalpingState.COOLDOWN:
            logger.debug(f"[WS] 중복 매도 체결 알림 무시 (already in COOLDOWN)")
            return True

        if filled_qty >= self._sell_order_qty:
            # Full sell fill
            pnl = int((filled_price - self._held_avg_price) * filled_qty)
            self._record_cycle_complete(pnl)
            self._clear_sell_order()
            self._clear_position()
            self._cooldown_start = datetime.now()
            self._transition(ScalpingState.COOLDOWN)
            logger.info(
                f"[WS] 전량 매도 체결: {filled_qty}주 @ {filled_price:,}원, "
                f"손익: {pnl:,}원"
            )
            return True

        # Partial sell (keep waiting)
        logger.debug(
            f"[WS] 부분 매도: {filled_qty}/{self._sell_order_qty}주 @ {filled_price:,}원"
        )
        return True

    def deactivate(self) -> None:
        """강제 종료 (일간 청산, 긴급 정지 등)"""
        with self._lock:
            logger.info(f"[scalping][{self._stock_name}] 강제 종료 시작")
            self._cleanup_all_orders()
            if self._held_qty > 0:
                self._market_sell_all("강제 종료")
            self._reset_to_idle()

    # ──────────────────────────────────────────
    # 상태 핸들러
    # ──────────────────────────────────────────

    def _handle_monitoring(self, price: int, timestamp: datetime) -> None:
        """MONITORING: P10 조건 확인 후 매수 주문"""
        if not self._price_tracker.is_ready(self._config.min_ticks_for_trade):
            return

        # 동적/고정 윈도우에서 P10 계산
        window = self._price_tracker.get_current_window_seconds(
            adaptive=self._config.adaptive_window
        )
        p10 = self._price_tracker.get_percentile(
            self._config.percentile_threshold,
            window_seconds=window,
        )
        if p10 is None:
            return

        # 호가 단위 맞춤 (매수: 내림)
        buy_price = round_to_tick_size(p10, direction="down")

        # 추세 필터: 하락추세일 때 매수 보류
        if self._config.trend_filter_enabled:
            uptick_ratio = self._price_tracker.get_uptick_ratio(
                window_seconds=window
            )
            if (
                uptick_ratio is not None
                and uptick_ratio < self._config.min_uptick_ratio
            ):
                return

        # 매수 조건: P10이 시그널 가격 이하
        if buy_price > self._signal_ctx.signal_price:
            return

        # 매수 수량 결정 (예수금 기반 allocation 적용)
        try:
            buyable_qty, _ = self._broker.get_buyable_quantity(
                self._stock_code, buy_price
            )
            if buyable_qty > 0:
                quantity = int(buyable_qty * (self._allocation / 100))
                if quantity < 1:
                    quantity = 1
                logger.info(
                    f"[scalping][{self._stock_name}] 수량 계산: "
                    f"매수가능={buyable_qty}주, allocation={self._allocation}%, "
                    f"주문수량={quantity}주"
                )
            else:
                quantity = self._config.position_size
                logger.warning(
                    f"[scalping][{self._stock_name}] 매수가능 수량 0 → "
                    f"fallback position_size={quantity}"
                )
        except Exception as e:
            quantity = self._config.position_size
            logger.warning(
                f"[scalping][{self._stock_name}] 수량 계산 실패({e}) → "
                f"fallback position_size={quantity}"
            )

        # 매수 주문
        result = self._broker.place_limit_order(
            stock_code=self._stock_code,
            side=OrderSide.BUY,
            quantity=quantity,
            price=buy_price,
        )

        if result.success:
            self._buy_order_id = result.order_id
            self._buy_order_branch = result.order_branch
            self._buy_order_price = buy_price
            self._buy_order_qty = quantity
            self._buy_order_time = timestamp
            self._last_order_check_time = None  # 첫 체결 확인 즉시 실행
            self._transition(ScalpingState.BUY_PENDING)
            logger.info(
                f"[scalping][{self._stock_name}] 매수 주문: "
                f"{buy_price:,}원 x {quantity}주 (P{self._config.percentile_threshold:.0f})"
            )
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 매수 주문 실패: {result.message}"
            )

    def _handle_buy_pending(self, price: int, timestamp: datetime) -> None:
        """BUY_PENDING: REST 폴백 + 타임아웃 처리 (WS가 우선)"""
        if not self._buy_order_id:
            self._transition(ScalpingState.MONITORING)
            return

        # 타임아웃 확인 (매 틱)
        if self._buy_order_time:
            elapsed = (timestamp - self._buy_order_time).total_seconds()
            if elapsed >= self._config.buy_timeout_seconds:
                logger.info(
                    f"[scalping][{self._stock_name}] 매수 타임아웃 "
                    f"({elapsed:.0f}초) → 취소 후 재시도"
                )
                self._cancel_buy_and_return_to_monitoring()
                return

        # WS 정상이면 REST 폴링 스킵
        if self._ws_client and self._ws_client.is_order_notice_active:
            return

        # REST fallback: throttle 적용
        if self._last_order_check_time:
            elapsed = (timestamp - self._last_order_check_time).total_seconds()
            if elapsed < self._order_check_interval:
                return
        self._last_order_check_time = timestamp

        # REST 체결 확인
        filled_qty, unfilled_qty = self._broker.get_order_status(
            self._buy_order_id,
            stock_code=self._stock_code,
            order_qty=self._buy_order_qty,
            side=OrderSide.BUY,
        )

        if filled_qty > self._held_qty:
            new_fills = filled_qty - self._held_qty
            self._update_position(filled_qty, self._buy_order_price)
            logger.info(f"[REST 폴백] 매수 체결: +{new_fills}주")

        if unfilled_qty == 0 and filled_qty > 0:
            # Full fill
            self._clear_buy_order()
            self._place_sell_order()
        elif filled_qty > 0:
            # Partial fill
            self._transition(ScalpingState.POSITION_HELD)
        if self._buy_order_time:
            elapsed = (timestamp - self._buy_order_time).total_seconds()
            if elapsed >= self._config.buy_timeout_seconds:
                logger.info(
                    f"[scalping][{self._stock_name}] 매수 타임아웃 "
                    f"({elapsed:.0f}초) → 취소 후 재시도"
                )
                self._cancel_buy_and_return_to_monitoring()

    def _handle_position_held(self, price: int, timestamp: datetime) -> None:
        """
        POSITION_HELD: 부분 체결 후 TP 모니터링

        - 매수 주문 남아있으면 추가 체결 확인 (WS 우선, REST 폴백)
        - 현재가 ≥ +0.1% 도달 시 매수 취소 + 전량 매도
        - SL 체크 (매 틱)
        """
        # 매수 주문 남아있으면 추가 체결 확인 (REST 폴백만)
        if self._buy_order_id:
            # WS 정상이면 REST 스킵
            if not (self._ws_client and self._ws_client.is_order_notice_active):
                # REST fallback (throttled)
                if self._last_order_check_time:
                    elapsed = (timestamp - self._last_order_check_time).total_seconds()
                    if elapsed < self._order_check_interval:
                        pass  # 다음 틱 대기
                    else:
                        self._last_order_check_time = timestamp
                        filled_qty, unfilled_qty = self._broker.get_order_status(
                            self._buy_order_id,
                            stock_code=self._stock_code,
                            order_qty=self._buy_order_qty,
                            side=OrderSide.BUY,
                        )
                        if filled_qty > self._held_qty:
                            new_fills = filled_qty - self._held_qty
                            self._update_position(filled_qty, self._buy_order_price)
                            logger.info(f"[REST 폴백] 추가 매수 체결: +{new_fills}주")

                        if unfilled_qty == 0:
                            self._clear_buy_order()
                else:
                    self._last_order_check_time = timestamp

        if self._held_qty <= 0:
            self._transition(ScalpingState.MONITORING)
            return

        # +0.1% 매도 조건 확인
        sell_target = self._calculate_sell_price(self._held_avg_price)
        if price >= sell_target:
            logger.info(
                f"[scalping][{self._stock_name}] TP 도달: "
                f"현재가 {price:,} >= 목표가 {sell_target:,}"
            )
            # 매수 주문 남아있으면 먼저 취소
            if self._buy_order_id:
                self._cancel_buy_order()

                # 취소 후 최종 체결량 재확인 (취소 중 체결 가능)
                final_filled, _ = self._broker.get_order_status(
                    self._buy_order_id,
                    stock_code=self._stock_code,
                    order_qty=self._buy_order_qty,
                    side=OrderSide.BUY,
                )
                if final_filled > self._held_qty:
                    self._update_position(final_filled, self._buy_order_price)
                    logger.info(f"[취소 중 체결] +{final_filled - self._held_qty}주")
                self._clear_buy_order()

            # 부분체결 상황 → 시장가 매도
            self._market_sell_all("부분체결 TP 도달")

        # SL 체크 (매 틱)
        if self._signal_ctx and price <= self._signal_ctx.sl_price:
            logger.warning(f"[scalping][{self._stock_name}] SL 도달 → 시장가 매도")
            if self._buy_order_id:
                self._cancel_buy_order()
                self._clear_buy_order()
            self._market_sell_all("SL 도달")

    def _handle_sell_pending(self, price: int, timestamp: datetime) -> None:
        """SELL_PENDING: REST 폴백 + SL 모니터링 (WS가 우선)"""
        if not self._sell_order_id:
            if self._held_qty > 0:
                self._transition(ScalpingState.POSITION_HELD)
            else:
                self._transition(ScalpingState.COOLDOWN)
                self._cooldown_start = timestamp
            return

        # SL 체크 (매 틱, API 호출 없음)
        if self._signal_ctx and price <= self._signal_ctx.sl_price:
            logger.warning(f"[scalping] 매도 대기 중 SL 도달 → 시장가 전환")
            self._cancel_sell_order()
            self._market_sell_all("매도 대기 중 SL 도달")
            return

        # WS 정상이면 REST 폴링 스킵
        if self._ws_client and self._ws_client.is_order_notice_active:
            return

        # REST 폴백: 스로틀링 적용
        if self._last_order_check_time:
            elapsed = (timestamp - self._last_order_check_time).total_seconds()
            if elapsed < self._order_check_interval:
                return
        self._last_order_check_time = timestamp

        # REST 체결 확인
        filled_qty, unfilled_qty = self._broker.get_order_status(
            self._sell_order_id,
            stock_code=self._stock_code,
            order_qty=self._sell_order_qty,
            side=OrderSide.SELL,
        )

        if unfilled_qty == 0 and filled_qty > 0:
            # Full sell fill
            pnl = int((self._sell_order_price - self._held_avg_price) * filled_qty)
            logger.info(
                f"[scalping][{self._stock_name}] 매도 체결 (REST 폴백): "
                f"{self._sell_order_price:,}원 x {filled_qty}주, 손익: {pnl:,}원"
            )
            self._record_cycle_complete(pnl)
            self._clear_sell_order()
            self._clear_position()
            self._cooldown_start = timestamp
            self._transition(ScalpingState.COOLDOWN)

    def _handle_cooldown(self, price: int, timestamp: datetime) -> None:
        """COOLDOWN: 사이클 쿨다운 후 MONITORING 복귀"""
        if self._cooldown_start is None:
            self._cooldown_start = timestamp

        elapsed = (timestamp - self._cooldown_start).total_seconds()
        if elapsed < self._config.cooldown_seconds:
            return

        # 최대 사이클 수 확인
        if self._signal_ctx and self._signal_ctx.cycle_count >= self._config.max_cycles:
            logger.info(
                f"[scalping][{self._stock_name}] 최대 사이클 도달 "
                f"({self._signal_ctx.cycle_count}/{self._config.max_cycles})"
            )
            self._reset_to_idle()
            return

        # 시그널 유효 → MONITORING 복귀
        self._cooldown_start = None
        self._transition(ScalpingState.MONITORING)

    # ──────────────────────────────────────────
    # 시그널 만료 처리
    # ──────────────────────────────────────────

    def _handle_signal_expired(self, reason: str, current_price: int) -> None:
        """시그널 수명 만료 시 정리"""
        logger.info(f"[scalping][{self._stock_name}] 시그널 만료: {reason}")

        # SL 도달인 경우
        is_sl = "SL" in reason

        # 미체결 주문 정리
        self._cleanup_all_orders()

        # 포지션 보유 시 처리
        if self._held_qty > 0:
            if is_sl:
                self._market_sell_all(f"시그널 SL: {reason}")
            else:
                # TP 또는 타임아웃: 포지션 없으면 바로 종료, 있으면 시장가 매도
                self._market_sell_all(f"시그널 만료: {reason}")
        else:
            self._log_signal_summary()
            self._reset_to_idle()

    # ──────────────────────────────────────────
    # 주문 관리 헬퍼
    # ──────────────────────────────────────────

    def _place_sell_order(self) -> None:
        """보유 수량 전량 매도 주문 (+0.1%)"""
        if self._held_qty <= 0:
            self._transition(ScalpingState.COOLDOWN)
            self._cooldown_start = datetime.now()
            return

        sell_price = self._calculate_sell_price(self._held_avg_price)

        result = self._broker.place_limit_order(
            stock_code=self._stock_code,
            side=OrderSide.SELL,
            quantity=self._held_qty,
            price=sell_price,
        )

        if result.success:
            self._sell_order_id = result.order_id
            self._sell_order_branch = result.order_branch
            self._sell_order_price = sell_price
            self._sell_order_qty = self._held_qty
            self._last_order_check_time = None  # 첫 체결 확인 즉시 실행
            self._transition(ScalpingState.SELL_PENDING)
            logger.info(
                f"[scalping][{self._stock_name}] 매도 주문: "
                f"{sell_price:,}원 x {self._held_qty}주 "
                f"(매수가 {self._held_avg_price:,.0f} + {self._config.sell_profit_pct*100:.1f}%)"
            )
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 매도 주문 실패: {result.message} → 시장가 매도"
            )
            self._market_sell_all("지정가 매도 실패 → 시장가")

    def _market_sell_all(self, reason: str) -> None:
        """시장가 전량 매도"""
        if self._held_qty <= 0:
            self._log_signal_summary()
            self._reset_to_idle()
            return

        logger.info(
            f"[scalping][{self._stock_name}] 시장가 매도: "
            f"{self._held_qty}주 ({reason})"
        )

        result = self._broker.place_market_order(
            stock_code=self._stock_code,
            side=OrderSide.SELL,
            quantity=self._held_qty,
        )

        if result.success:
            # 시장가는 즉시 체결로 간주
            pnl = 0  # 시장가는 정확한 체결가를 모르므로 0으로 기록
            self._record_cycle_complete(pnl)
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 시장가 매도 실패: {result.message}"
            )

        self._clear_position()
        self._clear_sell_order()
        self._log_signal_summary()
        self._reset_to_idle()

    def _cancel_buy_order(self) -> bool:
        """매수 주문 취소"""
        if not self._buy_order_id or not self._buy_order_branch:
            return True

        success = self._broker.cancel_order(
            order_id=self._buy_order_id,
            order_branch=self._buy_order_branch,
            quantity=self._buy_order_qty,
        )
        if success:
            logger.info(f"[scalping][{self._stock_name}] 매수 주문 취소 완료")
        else:
            logger.warning(
                f"[scalping][{self._stock_name}] 매수 주문 취소 실패 (체결되었을 수 있음)"
            )
        return success

    def _cancel_sell_order(self) -> bool:
        """매도 주문 취소"""
        if not self._sell_order_id or not self._sell_order_branch:
            return True

        success = self._broker.cancel_order(
            order_id=self._sell_order_id,
            order_branch=self._sell_order_branch,
            quantity=self._sell_order_qty,
        )
        if success:
            logger.info(f"[scalping][{self._stock_name}] 매도 주문 취소 완료")
        else:
            logger.warning(
                f"[scalping][{self._stock_name}] 매도 주문 취소 실패 (체결되었을 수 있음)"
            )
        return success

    def _cancel_buy_and_return_to_monitoring(self) -> None:
        """매수 취소 후 MONITORING 복귀 (취소 중 체결 처리 포함)"""
        self._cancel_buy_order()

        # 취소 후 최종 체결 상태 확인
        if self._buy_order_id:
            filled_qty, _ = self._broker.get_order_status(
                self._buy_order_id,
                stock_code=self._stock_code,
                order_qty=self._buy_order_qty,
                side=OrderSide.BUY,
            )
            if filled_qty > 0:
                self._update_position(filled_qty, self._buy_order_price)
                self._clear_buy_order()
                # 체결분이 있으면 매도 진행
                self._place_sell_order()
                return

        self._clear_buy_order()
        self._transition(ScalpingState.MONITORING)

    def _cleanup_all_orders(self) -> None:
        """모든 미체결 주문 취소"""
        if self._buy_order_id:
            self._cancel_buy_order()
            # 취소 중 체결 확인
            filled_qty, _ = self._broker.get_order_status(
                self._buy_order_id,
                stock_code=self._stock_code,
                order_qty=self._buy_order_qty,
                side=OrderSide.BUY,
            )
            if filled_qty > self._held_qty:
                self._update_position(filled_qty, self._buy_order_price)
            self._clear_buy_order()

        if self._sell_order_id:
            self._cancel_sell_order()
            # 취소 중 체결 확인
            filled_qty, _ = self._broker.get_order_status(
                self._sell_order_id,
                stock_code=self._stock_code,
                order_qty=self._sell_order_qty,
                side=OrderSide.SELL,
            )
            if filled_qty > 0:
                pnl = int((self._sell_order_price - self._held_avg_price) * filled_qty)
                remaining = self._held_qty - filled_qty
                self._held_qty = max(remaining, 0)
                if self._signal_ctx:
                    self._signal_ctx.total_pnl += pnl
            self._clear_sell_order()

    # ──────────────────────────────────────────
    # 포지션 / 상태 관리 헬퍼
    # ──────────────────────────────────────────

    def _update_position(self, total_filled_qty: int, fill_price: int) -> None:
        """매수 체결에 따른 포지션 업데이트 (가중평균가 계산)"""
        if total_filled_qty <= self._held_qty:
            return  # 이미 반영됨

        new_qty = total_filled_qty - self._held_qty
        if self._held_qty > 0:
            # 가중평균
            total_cost = self._held_avg_price * self._held_qty + fill_price * new_qty
            self._held_avg_price = total_cost / total_filled_qty
        else:
            self._held_avg_price = float(fill_price)
        self._held_qty = total_filled_qty

    def _calculate_sell_price(self, avg_price: float) -> int:
        """매도 목표가 계산 (+0.1%, 호가 단위 올림)"""
        raw = avg_price * (1 + self._config.sell_profit_pct)
        return round_to_tick_size(int(raw), direction="up")

    def _clear_buy_order(self) -> None:
        self._buy_order_id = None
        self._buy_order_branch = None
        self._buy_order_price = 0
        self._buy_order_qty = 0
        self._buy_order_time = None

    def _clear_sell_order(self) -> None:
        self._sell_order_id = None
        self._sell_order_branch = None
        self._sell_order_price = 0
        self._sell_order_qty = 0

    def _clear_position(self) -> None:
        self._held_qty = 0
        self._held_avg_price = 0.0

    def _record_cycle_complete(self, pnl: int) -> None:
        """매매 사이클 완료 기록"""
        if self._signal_ctx:
            self._signal_ctx.cycle_count += 1
            self._signal_ctx.total_pnl += pnl
            self._signal_ctx.total_trades += 1

    def _log_signal_summary(self) -> None:
        """시그널 종료 시 요약 로그"""
        if not self._signal_ctx:
            return
        ctx = self._signal_ctx
        logger.info(
            f"[scalping][{self._stock_name}] 시그널 종료 요약: "
            f"사이클={ctx.cycle_count}, 총매매={ctx.total_trades}회, "
            f"누적손익={ctx.total_pnl:,}원"
        )

    def _reset_to_idle(self) -> None:
        """IDLE 상태로 리셋"""
        self._signal_ctx = None
        self._clear_buy_order()
        self._clear_sell_order()
        self._clear_position()
        self._cooldown_start = None
        self._price_tracker.reset()
        self._transition(ScalpingState.IDLE)

    def _transition(self, new_state: ScalpingState) -> None:
        """상태 전환 (로그 포함)"""
        old_state = self._state
        self._state = new_state
        if old_state != new_state:
            logger.debug(
                f"[scalping][{self._stock_name}] "
                f"상태 전환: {old_state.value} → {new_state.value}"
            )

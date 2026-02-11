"""
스캘핑 실행기 (상태 머신)

WebSocket tick 기반으로 P10 매수 → +0.1% 매도를 반복 실행
"""

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from leverage_worker.notification.daily_report import DailyReportGenerator
    from leverage_worker.trading.position_manager import PositionManager
    from leverage_worker.websocket.ws_client import RealtimeWSClient

from leverage_worker.notification.slack_notifier import SlackNotifier
from leverage_worker.scalping.boundary_tracker import AdaptiveBoundaryTracker
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
        slack_notifier: Optional[SlackNotifier] = None,
        position_manager: Optional["PositionManager"] = None,
        trading_db: Optional["TradingDatabase"] = None,
        report_generator: Optional["DailyReportGenerator"] = None,
    ) -> None:
        self._stock_code = stock_code
        self._stock_name = stock_name
        self._config = config
        self._broker = broker
        self._strategy_name = strategy_name
        self._allocation = allocation
        self._ws_client = ws_client
        self._slack = slack_notifier
        self._position_manager = position_manager
        self._db = trading_db
        self._report_generator = report_generator

        # 상태
        self._state = ScalpingState.IDLE
        self._signal_ctx: Optional[ScalpingSignalContext] = None

        # Dynamic boundary tracker (틱 기반, range 0.1%~0.15% 1초 유지 시 DIP)
        self._boundary_tracker = AdaptiveBoundaryTracker(
            boundary_window_ticks=config.boundary_window_ticks,
            max_boundary_breaches=config.max_boundary_breaches,
            min_consecutive_downticks=config.min_consecutive_downticks,
            dip_margin_pct=config.dip_margin_pct,
            lower_history_size=config.lower_history_size,
            min_boundary_range_pct=config.min_boundary_range_pct,
            max_boundary_range_pct=config.max_boundary_range_pct,
            boundary_hold_seconds=config.boundary_hold_seconds,
            boundary_window_seconds=config.boundary_window_seconds,
            percentile_threshold=config.percentile_threshold,
        )

        # DEPRECATED: Old time-based tracker (backward compatibility)
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
        self._sell_order_time: Optional[datetime] = None  # 매도 주문 시간
        self._last_sell_fill_time: Optional[datetime] = None  # 마지막 매도 체결 시간

        # 매도 체결 누적 추적
        self._sold_qty: int = 0          # 현재 매도 주문 누적 체결 수량
        self._sold_pnl: int = 0          # 현재 매도 주문 누적 PnL (부분 매도 합산)

        # 포지션 추적 (executor 자체 관리)
        self._held_qty: int = 0
        self._held_avg_price: float = 0.0

        # 쿨다운
        self._cooldown_start: Optional[datetime] = None

        # 체결 확인 스로틀링 (API 호출 제한)
        self._last_order_check_time: Optional[datetime] = None
        self._order_check_interval: float = 1.0  # 초 단위 (1초마다 balance 확인)

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
            # NEW: boundary tracker 리셋
            self._boundary_tracker.reset()
            # DEPRECATED: old tracker reset (backward compatibility)
            self._price_tracker.reset()
            self._transition(ScalpingState.MONITORING)

            logger.info(
                f"[scalping][{self._stock_name}] 시그널 활성화: "
                f"signal_price={signal_price:,}, "
                f"TP={tp_pct*100:.1f}%({self._signal_ctx.tp_price:,}), "
                f"SL={sl_pct*100:.1f}%({self._signal_ctx.sl_price:,}), "
                f"timeout={timeout_minutes}분"
            )

            # Slack notification
            if self._slack:
                try:
                    self._slack.notify_signal(
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        signal_type="BUY",
                        price=signal_price,
                        strategy_name=self._strategy_name,
                        reason=(
                            f"스캘핑 시작 "
                            f"(TP=*{self._signal_ctx.tp_price:,}원*/{tp_pct*100:.1f}%, "
                            f"SL=*{self._signal_ctx.sl_price:,}원*/{sl_pct*100:.1f}%)"
                        ),
                        strategy_win_rate=None,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

    def activate_limit_order(
        self,
        buy_price: int,
        sell_price: int,
        timeout_seconds: int,
        quantity: int = 0,
    ) -> bool:
        """
        지정가 매수 즉시 실행 (boundary_tracker 미사용)

        main_beam_1 전략용: 신호 발생 즉시 지정가 매수 주문

        Args:
            buy_price: 지정가 매수 가격 (prev_close * 0.999)
            sell_price: 지정가 매도 가격 (buy_price * 1.001)
            timeout_seconds: 타임아웃 (초) - 매수 후 시간 기반 손절
            quantity: 매수 수량 (0이면 allocation 기반 자동 계산)

        Returns:
            주문 성공 여부
        """
        with self._lock:
            if self._state != ScalpingState.IDLE:
                logger.warning(
                    f"[scalping][{self._stock_code}] "
                    f"limit_order 무시: 이미 활성 상태 ({self._state.value})"
                )
                return False

            # allocation 기반 수량 계산
            if quantity <= 0:
                buyable_qty, _ = self._broker.get_buyable_quantity(
                    self._stock_code, buy_price
                )
                if buyable_qty > 0:
                    quantity = int(buyable_qty * (self._allocation / 100))
                    if quantity < 1:
                        quantity = 1
                    logger.info(
                        f"[scalping][{self._stock_code}] 수량 계산: "
                        f"{buyable_qty}주 x {self._allocation}% = {quantity}주"
                    )
                else:
                    logger.error(
                        f"[scalping][{self._stock_code}] 매수가능수량 조회 실패"
                    )
                    return False

            # 컨텍스트 설정
            timeout_minutes = max(1, timeout_seconds // 60)
            self._signal_ctx = ScalpingSignalContext(
                signal_price=buy_price,
                signal_time=datetime.now(),
                tp_pct=0.001,  # 참고용 (실제로는 sell_price 사용)
                sl_pct=0.01,  # 1% 손절
                timeout_minutes=timeout_minutes,
            )
            # 메타데이터에 매도가/타임아웃 저장
            self._signal_ctx.metadata["sell_price"] = sell_price
            self._signal_ctx.metadata["timeout_seconds"] = timeout_seconds
            self._signal_ctx.metadata["is_limit_order"] = True

            # 즉시 지정가 매수 주문 (MONITORING 건너뜀)
            logger.info(
                f"[scalping][{self._stock_name}] limit_order 매수 주문: "
                f"buy_price={buy_price:,}, sell_price={sell_price:,}, "
                f"qty={quantity}, timeout={timeout_seconds}초"
            )

            try:
                result = self._broker.place_limit_order(
                    stock_code=self._stock_code,
                    side=OrderSide.BUY,
                    quantity=quantity,
                    price=buy_price,
                )
            except Exception as e:
                logger.error(f"[scalping][{self._stock_code}] 매수 주문 실패: {e}")
                return False

            if result and result.success:
                self._buy_order_id = result.order_id
                self._buy_order_branch = getattr(result, "branch_no", "01")
                self._buy_order_price = buy_price
                self._buy_order_qty = quantity
                self._buy_order_time = datetime.now()
                self._transition(ScalpingState.BUY_PENDING)

                # DB 저장
                self._save_order_to_db(result.order_id, "BUY", quantity, buy_price)

                logger.info(
                    f"[scalping][{self._stock_name}] limit_order 매수 주문 완료: "
                    f"order_id={result.order_id}, "
                    f"{buy_price:,}원 x {quantity}주"
                )

                # Slack 알림
                if self._slack:
                    try:
                        self._slack.notify_signal(
                            stock_code=self._stock_code,
                            stock_name=self._stock_name,
                            signal_type="BUY",
                            price=buy_price,
                            strategy_name=self._strategy_name,
                            reason=(
                                f"지정가 매수 (목표 매도={sell_price:,}원, "
                                f"타임아웃={timeout_seconds}초)"
                            ),
                            strategy_win_rate=None,
                        )
                    except Exception as e:
                        logger.warning(f"[scalping] Slack 알림 실패: {e}")

                return True
            else:
                logger.error(
                    f"[scalping][{self._stock_code}] limit_order 매수 주문 실패: "
                    f"result={result}"
                )
                return False

    def handle_short_signal(self, short_price: int, reason: str) -> None:
        """
        SHORT 시그널 감지 시 즉시 청산

        Args:
            short_price: SHORT 시그널 가격
            reason: 시그널 사유

        동작:
            - MONITORING: 즉시 시그널 만료
            - BUY_PENDING: 매수 주문 취소 + 시그널 만료
            - POSITION_HELD: 매수 주문 취소 + 포지션 시장가 매도
            - SELL_PENDING: 매도 주문 취소 + 시장가 매도
        """
        with self._lock:
            # 사유에 따른 메시지 분류
            if "익절" in reason:
                signal_type = "전략 익절"
                emoji = "📈"
            elif "손절" in reason:
                signal_type = "전략 손절"
                emoji = "📉"
            elif "반전" in reason or "하락" in reason or "SHORT" in reason.upper():
                signal_type = "SHORT 반전"
                emoji = "🔻"
            else:
                signal_type = "청산 시그널"
                emoji = "⚠️"

            logger.warning(
                f"[scalping][{self._stock_name}] {signal_type} 감지: "
                f"가격={short_price:,}원, 사유={reason}, 상태={self._state.value}"
            )

            # Slack 알림
            if self._slack:
                try:
                    self._slack.send_message(
                        f"{emoji} [{self._stock_name}] {signal_type}\n"
                        f"• 가격: {short_price:,}원\n"
                        f"• 사유: {reason}\n"
                        f"• 조치: 즉시 청산 ({self._state.value})"
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

            if self._state == ScalpingState.MONITORING:
                # DIP 바운더리 찾는 중 → 즉시 종료
                self._handle_signal_expired(f"{signal_type}: {reason}", short_price)

            elif self._state == ScalpingState.BUY_PENDING:
                # 매수 주문 대기 중 → 주문 취소 후 종료
                if self._buy_order_id:
                    self._cancel_buy_order()
                    self._clear_buy_order()
                self._handle_signal_expired(f"{signal_type}: {reason}", short_price)

            elif self._state == ScalpingState.POSITION_HELD:
                # 부분 체결 상태 → 주문 취소 + 포지션 매도
                if self._buy_order_id:
                    self._cancel_buy_order()
                    # 취소 중 추가 체결 확인
                    if self._buy_order_branch:
                        filled_qty, _ = self._broker.get_order_status(
                            self._buy_order_id,
                            stock_code=self._stock_code,
                            order_qty=self._buy_order_qty,
                            side=OrderSide.BUY,
                        )
                        if filled_qty > self._held_qty:
                            self._update_position(filled_qty, self._buy_order_price)
                    self._clear_buy_order()

                # 포지션 시장가 매도
                if self._held_qty > 0:
                    self._market_sell_all(f"{signal_type}: {reason}")
                else:
                    self._handle_signal_expired(f"{signal_type}: {reason}", short_price)

            elif self._state == ScalpingState.SELL_PENDING:
                # 매도 주문 대기 중 → 기존 주문 취소 + 시장가 매도
                if self._sell_order_id:
                    self._cancel_sell_order()
                    self._clear_sell_order()

                # 포지션이 남아있으면 시장가 매도
                if self._held_qty > 0:
                    self._market_sell_all(f"{signal_type}: {reason}")
                else:
                    self._log_signal_summary()
                    self._reset_to_idle()

            # COOLDOWN은 무시 (이미 종료 과정 중)

    def on_tick(self, price: int, timestamp: datetime) -> None:
        """
        WebSocket tick 수신 시 호출

        모든 상태에서 tick을 price_tracker에 누적하고,
        현재 상태에 따라 적절한 핸들러 호출.
        """
        with self._lock:
            if self._state == ScalpingState.IDLE:
                return

            # boundary tracker: MONITORING/BUY_PENDING에서만 동작 (limit_order 제외)
            event = None
            if self._state in (ScalpingState.MONITORING, ScalpingState.BUY_PENDING):
                # limit_order는 boundary tracking 스킵
                if not (self._signal_ctx and self._signal_ctx.metadata.get("is_limit_order")):
                    event = self._boundary_tracker.add_tick(price)
                if event == "BREACH":
                    logger.info(
                        f"[scalping][{self._stock_name}] 바운더리 이탈 "
                        f"({self._boundary_tracker.get_breach_count()}회)"
                    )

            # DEPRECATED: old tracker tick (backward compatibility)
            self._price_tracker.add_tick(timestamp, price)

            # 시그널 수명 만료 체크 (SELL_PENDING 제외 - 이미 매도 진행 중)
            if self._signal_ctx and self._state != ScalpingState.SELL_PENDING:
                expired, reason = self._signal_ctx.is_expired(timestamp, price)
                if expired:
                    # [limit_order] buy_timeout 중에는 시그널 TP 무시
                    is_limit_order = self._signal_ctx.metadata.get("is_limit_order")
                    if is_limit_order and self._buy_order_time and "TP" in reason:
                        elapsed = (timestamp - self._buy_order_time).total_seconds()
                        if elapsed < self._config.buy_timeout_seconds:
                            pass  # 시그널 TP 스킵, SL/타임아웃은 처리
                        else:
                            self._handle_signal_expired(reason, price)
                            return
                    else:
                        self._handle_signal_expired(reason, price)
                        return

            # 상태별 핸들러 디스패치
            # MONITORING handler에 event 전달 (DIP 감지용)
            if self._state == ScalpingState.MONITORING:
                self._handle_monitoring(price, timestamp, event)
            elif self._state == ScalpingState.BUY_PENDING:
                self._handle_buy_pending(price, timestamp)
            elif self._state == ScalpingState.POSITION_HELD:
                self._handle_position_held(price, timestamp)
            elif self._state == ScalpingState.SELL_PENDING:
                self._handle_sell_pending(price, timestamp)
            elif self._state == ScalpingState.COOLDOWN:
                self._handle_cooldown(price, timestamp)

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

        # DB 업데이트
        self._update_order_fill_in_db(
            self._buy_order_id, self._held_qty, filled_price
        )

        # Check if fully filled
        if self._held_qty >= self._buy_order_qty:
            # Full fill → immediate sell
            self._clear_buy_order()
            self._place_sell_order()
            logger.info(
                f"[WS] 전량 매수 체결 (+{actual_fill}주) → 즉시 매도: "
                f"{self._held_qty}주 @ {self._held_avg_price:,.0f}원"
            )

            # Slack notification - full fill
            if self._slack:
                try:
                    self._slack.notify_fill(
                        fill_type="BUY",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=self._held_qty,
                        price=filled_price,
                        strategy_name=self._strategy_name,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

            return True

        # Partial fill → POSITION_HELD
        if self._state == ScalpingState.BUY_PENDING:
            self._transition(ScalpingState.POSITION_HELD)
            logger.info(
                f"[WS] 부분 매수 체결 (+{actual_fill}주): "
                f"{self._held_qty}/{self._buy_order_qty}주 @ {self._held_avg_price:,.0f}원"
            )

            # Slack notification - partial fill
            if self._slack:
                try:
                    self._slack.notify_fill(
                        fill_type="BUY",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=actual_fill,
                        price=filled_price,
                        strategy_name=self._strategy_name,
                        total_filled=self._held_qty,
                        order_quantity=self._buy_order_qty,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")
        else:
            # POSITION_HELD에서 추가 체결
            logger.info(
                f"[WS] 추가 매수 체결 (+{actual_fill}주): "
                f"{self._held_qty}/{self._buy_order_qty}주 @ {self._held_avg_price:,.0f}원"
            )

            # Slack notification - additional fill
            if self._slack:
                try:
                    self._slack.notify_fill(
                        fill_type="BUY",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=actual_fill,
                        price=filled_price,
                        strategy_name=self._strategy_name,
                        total_filled=self._held_qty,
                        order_quantity=self._buy_order_qty,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

        return True

    def _handle_ws_sell_fill(self, filled_qty: int, filled_price: int) -> bool:
        """매도 체결 WS 처리 (증분)"""
        # EC-2: 이미 COOLDOWN이면 무시
        if self._state == ScalpingState.COOLDOWN:
            logger.debug("[WS] 중복 매도 체결 알림 무시 (already in COOLDOWN)")
            return True

        # 중복 방지: 이미 전량 체결이면 무시
        if self._sold_qty >= self._sell_order_qty:
            logger.debug("[WS] 이미 전량 매도 체결됨, 추가 알림 무시")
            return True

        # 실제 반영 수량 (초과 방지)
        remaining_sell = self._sell_order_qty - self._sold_qty
        actual_fill = min(filled_qty, remaining_sell)
        if actual_fill <= 0:
            return True

        # 누적 업데이트
        self._sold_qty += actual_fill
        self._last_sell_fill_time = datetime.now()  # 마지막 체결 시간 갱신

        # 이번 체결분 PnL
        fill_pnl = int((filled_price - self._held_avg_price) * actual_fill)
        self._sold_pnl += fill_pnl

        # held_qty 차감 (E-3에서 정확한 잔여 수량 보장)
        self._held_qty = max(self._held_qty - actual_fill, 0)

        # DB 업데이트 (누적 체결 정보)
        self._update_order_fill_in_db(
            self._sell_order_id, self._sold_qty, filled_price,
            pnl=self._sold_pnl, avg_cost=self._held_avg_price,
            pnl_rate=((filled_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0,
        )

        if self._sold_qty >= self._sell_order_qty:
            # === Full sell fill (단건이든 분할이든) ===
            total_pnl = self._sold_pnl
            profit_pct = ((filled_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0
            self._record_cycle_complete(total_pnl)

            sell_qty_for_log = self._sell_order_qty  # clear 전에 저장
            self._clear_sell_order()
            self._clear_position()
            self._cooldown_start = datetime.now()
            self._transition(ScalpingState.COOLDOWN)
            logger.info(
                f"[WS] 전량 매도 체결: {sell_qty_for_log}주 @ {filled_price:,}원, "
                f"손익: {total_pnl:,}원"
            )

            # Slack notification - sell fill with PnL
            if self._slack and self._signal_ctx:
                try:
                    self._slack.notify_fill(
                        fill_type="SELL",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=sell_qty_for_log,
                        price=filled_price,
                        strategy_name=self._strategy_name,
                        profit_loss=total_pnl,
                        profit_rate=profit_pct,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

            # Daily summary DB 업데이트
            if self._report_generator:
                try:
                    report = self._report_generator.generate()
                    self._report_generator.save_to_db(report)
                    logger.info(f"[scalping] Daily summary 업데이트: {report.realized_pnl:,}원")
                except Exception as e:
                    logger.warning(f"[scalping] Daily summary 업데이트 실패: {e}")

            return True

        # === Partial sell ===
        logger.info(
            f"[WS] 부분 매도: {actual_fill}주 @ {filled_price:,}원 "
            f"(누적 {self._sold_qty}/{self._sell_order_qty}주)"
        )

        # Slack 알림 - 부분 매도
        if self._slack:
            try:
                fill_profit_rate = ((filled_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0
                self._slack.notify_fill(
                    fill_type="SELL",
                    stock_code=self._stock_code,
                    stock_name=self._stock_name,
                    quantity=actual_fill,
                    price=filled_price,
                    strategy_name=self._strategy_name,
                    profit_loss=fill_pnl,
                    profit_rate=fill_profit_rate,
                    total_filled=self._sold_qty,
                    order_quantity=self._sell_order_qty,
                )
            except Exception:
                pass
        return True

    def deactivate(self) -> None:
        """강제 종료 (일간 청산, 긴급 정지 등)"""
        with self._lock:
            logger.info(f"[scalping][{self._stock_name}] 강제 종료 시작")
            self._cleanup_all_orders()
            if self._held_qty > 0:
                self._market_sell_all("강제 종료", force_immediate=True)
            else:
                self._reset_to_idle()

    # ──────────────────────────────────────────
    # 상태 핸들러
    # ──────────────────────────────────────────

    def _handle_monitoring(
        self,
        price: int,
        timestamp: datetime,
        event: Optional[str],
    ) -> None:
        """
        MONITORING: DIP 패턴 감지 시 매수

        Args:
            price: 현재가
            timestamp: 현재 시간
            event: "BREACH", "DIP", or None from boundary tracker
        """
        # 1. Breach 횟수 확인
        if not self._boundary_tracker.is_trading_allowed():
            breach_count = self._boundary_tracker.get_breach_count()
            logger.warning(
                f"[scalping][{self._stock_name}] 최대 breach 도달 "
                f"({breach_count}회) → 거래 중단"
            )

            # Slack notification - safety stop
            if self._slack:
                try:
                    self._slack.send_message(
                        f"⚠️ [{self._stock_name}] 스캘핑 안전 중단\n"
                        f"• 사유: 최대 breach 도달 ({breach_count}회)\n"
                        f"• 조치: 거래 중단"
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

            self._reset_to_idle()
            return

        # 2. DIP 이벤트 대기
        if event != "DIP":
            return  # 바운더리 구성 중이거나 DIP 조건 미달

        # 3. 매수 가격 확정 (하단 바운더리)
        buy_price = self._boundary_tracker.get_buy_price()
        if buy_price is None:
            logger.error(
                f"[scalping][{self._stock_name}] DIP 감지했으나 buy_price None"
            )
            return

        # 4. 호가 단위 맞춤
        buy_price = round_to_tick_size(buy_price, direction="down")

        # 5. 시그널가 대비 하락률 체크
        signal_price = self._signal_ctx.signal_price
        p_price = self._boundary_tracker.get_percentile_price(
            self._config.percentile_threshold
        )
        if p_price is None:
            return

        dip_rate = (signal_price - p_price) / signal_price
        min_dip = self._config.dip_from_signal_pct
        if dip_rate < min_dip:
            logger.debug(
                f"[scalping][{self._stock_name}] P{self._config.percentile_threshold:.0f}({p_price:,}) "
                f"하락률 {dip_rate*100:.3f}% < {min_dip*100:.2f}% → 매수 대기"
            )
            return

        # 6. 매수 수량 결정
        try:
            buyable_qty, _ = self._broker.get_buyable_quantity(
                self._stock_code, buy_price
            )
            if buyable_qty > 0:
                quantity = int(buyable_qty * (self._allocation / 100))
                if quantity < 1:
                    quantity = 1
            else:
                quantity = self._config.position_size
        except Exception as e:
            quantity = self._config.position_size
            logger.warning(
                f"[scalping][{self._stock_name}] 수량 계산 실패 → "
                f"fallback={quantity}"
            )

        # 7. 매수 주문
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
            self._last_order_check_time = None
            self._transition(ScalpingState.BUY_PENDING)

            # DB 저장
            self._save_order_to_db(result.order_id, "BUY", quantity, buy_price)

            # 디버깅 정보 풍부하게
            lower, upper, tick_count = self._boundary_tracker.get_boundary_info()
            logger.info(
                f"[scalping][{self._stock_name}] DIP 매수 주문: "
                f"{buy_price:,}원 x {quantity}주 "
                f"(breach={self._boundary_tracker.get_breach_count()}, "
                f"boundary={lower:,}~{upper:,}, ticks={tick_count})"
            )

            # Slack notification - buy order
            if self._slack:
                try:
                    self._slack.notify_buy(
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=quantity,
                        price=buy_price,
                        strategy_name=self._strategy_name,
                        reason=(
                            f"DIP 매수 (breach={self._boundary_tracker.get_breach_count()}, "
                            f"boundary={lower:,}~{upper:,}원, {tick_count}틱)"
                        ),
                        strategy_win_rate=None,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 매수 주문 실패: {result.message}"
            )

    def _handle_buy_pending(self, price: int, timestamp: datetime) -> None:
        """BUY_PENDING: REST 폴백 + 타임아웃 처리 (WS가 우선)"""
        if not self._buy_order_id:
            # limit_order 모드: IDLE로 복귀
            if self._signal_ctx and self._signal_ctx.metadata.get("is_limit_order"):
                self._reset_to_idle()
            else:
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

        # WS 비활성 → REST 폴백 사용
        if self._ws_client:
            logger.debug(
                f"[scalping] WS 비활성 - REST 폴백 "
                f"(subscribed={getattr(self._ws_client, '_order_notice_subscribed', 'N/A')}, "
                f"running={getattr(self._ws_client, '_running', 'N/A')})"
            )

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

            # DB 업데이트
            self._update_order_fill_in_db(
                self._buy_order_id, filled_qty, self._buy_order_price
            )

            # Slack notification - REST buy fill
            if self._slack:
                try:
                    self._slack.notify_fill(
                        fill_type="BUY",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=new_fills,
                        price=self._buy_order_price,
                        strategy_name=self._strategy_name,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

        if unfilled_qty == 0 and filled_qty > 0:
            # Full fill
            self._clear_buy_order()
            self._place_sell_order()
        elif filled_qty > 0:
            # Partial fill
            self._transition(ScalpingState.POSITION_HELD)
        elif self._buy_order_time:
            # 체결 없음 → 타임아웃 체크 (부분체결 시에는 POSITION_HELD에서 TP/SL 처리)
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

                            # DB 업데이트 - 추가 매수 체결
                            self._update_order_fill_in_db(
                                self._buy_order_id, filled_qty, self._buy_order_price
                            )

                            # Slack notification - REST additional buy fill
                            if self._slack:
                                try:
                                    self._slack.notify_fill(
                                        fill_type="BUY",
                                        stock_code=self._stock_code,
                                        stock_name=self._stock_name,
                                        quantity=new_fills,
                                        price=self._buy_order_price,
                                        strategy_name=self._strategy_name,
                                    )
                                except Exception as e:
                                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

                        if unfilled_qty == 0:
                            self._clear_buy_order()
                            logger.info(
                                f"[REST 폴백] 전량 매수 체결 → 즉시 매도 주문: "
                                f"{self._held_qty}주 @ {self._held_avg_price:,.0f}원"
                            )
                            self._place_sell_order()
                            return
                else:
                    self._last_order_check_time = timestamp

        if self._held_qty <= 0:
            # limit_order 모드: IDLE로 복귀
            if self._signal_ctx and self._signal_ctx.metadata.get("is_limit_order"):
                self._reset_to_idle()
            else:
                self._transition(ScalpingState.MONITORING)
            return

        # limit_order 전략 여부 확인
        is_limit_order = self._signal_ctx and self._signal_ctx.metadata.get(
            "is_limit_order"
        )

        # [limit_order] buy_timeout 경과 시 미체결 즉시 취소 (POSITION_HELD 유지)
        if is_limit_order and self._buy_order_id and self._buy_order_time:
            elapsed = (timestamp - self._buy_order_time).total_seconds()
            if elapsed >= self._config.buy_timeout_seconds:
                cancel_qty = self._buy_order_qty - self._held_qty
                logger.info(
                    f"[scalping][{self._stock_name}] buy_timeout 경과 "
                    f"({elapsed:.0f}초) → 미체결 {cancel_qty}주 취소"
                )
                self._cancel_buy_order()
                self._clear_buy_order()
                # return 없음 - 아래 TP/SL 체크 계속

        # +0.1% 매도 조건 확인
        sell_target = self._calculate_sell_price(self._held_avg_price)
        if price >= sell_target:
            # [limit_order] 미체결 있으면 TP 무시 (buy_timeout 중)
            if is_limit_order and self._buy_order_id:
                pass  # TP 스킵, SL 체크로 넘어감
            else:
                logger.info(
                    f"[scalping][{self._stock_name}] TP 도달: "
                    f"현재가 {price:,} >= 목표가 {sell_target:,}"
                )
                # 매수 주문 남아있으면 먼저 취소 (일반 전략용)
                if self._buy_order_id:
                    cancel_qty = self._buy_order_qty - self._held_qty
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

                    # Slack 알림 - TP 매수 취소
                    if self._slack:
                        try:
                            self._slack.send_message(
                                f"📊 [{self._stock_name}] TP 도달 → 미체결 매수 취소\n"
                                f"• 취소: {cancel_qty}주 / 체결: {self._held_qty}주\n"
                                f"• 시장가 매도 진행"
                            )
                        except Exception:
                            pass
                    self._clear_buy_order()

                # 부분체결 상황 → 시장가 매도
                self._market_sell_all("부분체결 TP 도달")
                return  # SELL_PENDING 전환 후 즉시 반환 (SL 중복 실행 방지)

        # SL 체크 (매 틱)
        if self._signal_ctx and price <= self._signal_ctx.sl_price:
            logger.warning(f"[scalping][{self._stock_name}] SL 도달 → 시장가 매도")
            if self._buy_order_id:
                cancel_qty = self._buy_order_qty - self._held_qty
                self._cancel_buy_order()

                # Slack 알림 - SL 매수 취소
                if self._slack:
                    try:
                        self._slack.send_message(
                            f"🛑 [{self._stock_name}] SL 도달 → 미체결 매수 취소\n"
                            f"• 취소: {cancel_qty}주 / 체결: {self._held_qty}주\n"
                            f"• 시장가 매도 진행"
                        )
                    except Exception:
                        pass
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

        # 매도 타임아웃 체크 (지정가 주문만, 시장가는 스킵)
        if self._sell_order_time and self._sell_order_price > 0:
            timeout_sec = self._config.sell_timeout_seconds
            # 기준 시간: 마지막 체결 시간 또는 주문 시간
            reference_time = self._last_sell_fill_time or self._sell_order_time
            elapsed = (timestamp - reference_time).total_seconds()

            # 잔여 수량이 있고 타임아웃 경과
            remaining = self._sell_order_qty - self._sold_qty
            if remaining > 0 and elapsed >= timeout_sec:
                if self._sold_qty == 0:
                    reason = f"매도 타임아웃: {elapsed:.0f}초 체결 없음"
                else:
                    reason = f"부분체결 후 타임아웃: {elapsed:.0f}초 추가 체결 없음 (잔여 {remaining}주)"

                logger.warning(f"[scalping][{self._stock_name}] {reason} → 시장가 전환")

                # 부분 매도 PnL 보존 후 주문 정리
                saved_sold_pnl = self._sold_pnl
                self._cancel_sell_order()
                self._clear_sell_order()
                self._sold_pnl = saved_sold_pnl  # 부분 매도 PnL 복원

                self._market_sell_all(reason)
                return

        # SL 체크 (지정가 주문만, 시장가는 스킵)
        if self._sell_order_price > 0 and self._signal_ctx and price <= self._signal_ctx.sl_price:
            logger.warning(f"[scalping] 매도 대기 중 SL 도달 → 시장가 전환")

            # 부분 매도 PnL 보존 후 주문 정리
            saved_sold_pnl = self._sold_pnl
            self._cancel_sell_order()
            self._clear_sell_order()
            self._sold_pnl = saved_sold_pnl  # 부분 매도 PnL 복원

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
            # Full sell fill via REST

            # 체결가 결정: 지정가=주문가, 시장가=현재가(근사)
            if self._sell_order_price > 0:
                sell_price = self._sell_order_price
            else:
                sell_price = price  # 시장가: 현재 틱 가격을 근사치로 사용

            # REST에서 새로 확인된 체결 (WS에서 미처리분)
            new_fills = max(0, filled_qty - self._sold_qty)
            if new_fills > 0:
                new_pnl = int((sell_price - self._held_avg_price) * new_fills)
                self._sold_pnl += new_pnl
                self._held_qty = max(self._held_qty - new_fills, 0)
                self._sold_qty = filled_qty
                self._last_sell_fill_time = timestamp  # 마지막 체결 시간 갱신

            total_pnl = self._sold_pnl
            profit_pct = ((sell_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0

            logger.info(
                f"[scalping][{self._stock_name}] 매도 체결 (REST): "
                f"{sell_price:,}원 x {filled_qty}주, 손익: {total_pnl:,}원"
            )
            self._record_cycle_complete(total_pnl)

            # DB 업데이트
            self._update_order_fill_in_db(
                self._sell_order_id, filled_qty, sell_price,
                pnl=total_pnl, avg_cost=self._held_avg_price, pnl_rate=profit_pct,
            )

            self._clear_sell_order()
            self._clear_position()
            self._cooldown_start = timestamp
            self._transition(ScalpingState.COOLDOWN)

            # Slack notification
            if self._slack and self._signal_ctx:
                try:
                    self._slack.notify_fill(
                        fill_type="SELL",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=filled_qty,
                        price=sell_price,
                        strategy_name=self._strategy_name,
                        profit_loss=total_pnl,
                        profit_rate=profit_pct,
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")

            # Daily summary DB 업데이트
            if self._report_generator:
                try:
                    report = self._report_generator.generate()
                    self._report_generator.save_to_db(report)
                    logger.info(f"[scalping] Daily summary 업데이트: {report.realized_pnl:,}원")
                except Exception as e:
                    logger.warning(f"[scalping] Daily summary 업데이트 실패: {e}")

        elif filled_qty > 0 and filled_qty > self._sold_qty:
            # Partial sell fill via REST (부분 체결)

            # 체결가 결정
            if self._sell_order_price > 0:
                sell_price = self._sell_order_price
            else:
                sell_price = price

            # 이번에 새로 확인된 체결량 (REST는 누적값이므로 차이 계산)
            new_fills = filled_qty - self._sold_qty

            # PnL 계산 및 상태 업데이트 (WS 로직과 동일)
            fill_pnl = int((sell_price - self._held_avg_price) * new_fills)
            self._sold_pnl += fill_pnl
            self._held_qty = max(self._held_qty - new_fills, 0)
            self._sold_qty = filled_qty  # REST는 누적값으로 설정
            self._last_sell_fill_time = timestamp  # 마지막 체결 시간 갱신

            logger.info(
                f"[REST 폴백] 부분 매도: {new_fills}주 @ {sell_price:,}원 "
                f"(누적 {self._sold_qty}/{self._sell_order_qty}주)"
            )

            # DB 업데이트
            self._update_order_fill_in_db(
                self._sell_order_id, self._sold_qty, sell_price,
                pnl=self._sold_pnl, avg_cost=self._held_avg_price,
                pnl_rate=((sell_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0,
            )

            # Slack 알림 - 부분 매도
            if self._slack:
                try:
                    fill_profit_rate = ((sell_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0
                    self._slack.notify_fill(
                        fill_type="SELL",
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=new_fills,
                        price=sell_price,
                        strategy_name=self._strategy_name,
                        profit_loss=fill_pnl,
                        profit_rate=fill_profit_rate,
                        total_filled=self._sold_qty,
                        order_quantity=self._sell_order_qty,
                    )
                except Exception:
                    pass

    def _handle_cooldown(self, price: int, timestamp: datetime) -> None:
        """
        COOLDOWN: limit_order는 IDLE, 일반은 MONITORING 복귀

        - limit_order: one-shot 전략, 다음 신호 대기를 위해 IDLE로 복귀
        - 일반 scalping: 바운더리가 새로 구성될 때까지 MONITORING에서 대기
        """
        # Slack dedup 초기화: 다음 사이클 알림 허용
        if self._slack:
            self._slack.reset_signal_for_key(self._stock_code, self._strategy_name)

        # limit_order 모드: IDLE로 복귀 (boundary_tracker 미사용, 다음 신호 대기)
        if self._signal_ctx and self._signal_ctx.metadata.get("is_limit_order"):
            logger.info(
                f"[scalping][{self._stock_name}] limit_order 완료 → IDLE (다음 신호 대기)"
            )
            self._log_signal_summary()
            self._reset_to_idle()
            return

        # === 기존 scalping 로직 ===
        # 최대 사이클 수 확인
        if self._signal_ctx and self._signal_ctx.cycle_count >= self._config.max_cycles:
            logger.info(
                f"[scalping][{self._stock_name}] 최대 사이클 도달 "
                f"({self._signal_ctx.cycle_count}/{self._config.max_cycles})"
            )
            # Slack 종료 알림
            if self._slack and self._signal_ctx:
                try:
                    ctx = self._signal_ctx
                    # 오늘 전체 손익 조회
                    daily_pnl = ctx.total_pnl
                    if self._report_generator:
                        try:
                            report = self._report_generator.generate()
                            daily_pnl = report.realized_pnl
                        except Exception:
                            pass
                    self._slack.send_message(
                        f"[{self._stock_name}] 스캘핑 거래 완료\n"
                        f"• 당일 누적 손익: *{daily_pnl:,}원*\n"
                        f"• 완료 사이클: {ctx.cycle_count}/{self._config.max_cycles}회\n"
                        f"• 다음 시그널 대기 중"
                    )
                except Exception:
                    pass
            self._reset_to_idle()
            return

        # 바운더리/DIP 상태 초기화 (틱/breach는 유지)
        self._boundary_tracker.reset_for_new_cycle()

        # 즉시 MONITORING 복귀
        self._cooldown_start = None
        self._transition(ScalpingState.MONITORING)
        logger.info(
            f"[scalping][{self._stock_name}] COOLDOWN → MONITORING "
            f"(바운더리 재구성 대기)"
        )

    # ──────────────────────────────────────────
    # 시그널 만료 처리
    # ──────────────────────────────────────────

    def _handle_signal_expired(self, reason: str, current_price: int) -> None:
        """시그널 수명 만료 시 정리"""
        logger.info(f"[scalping][{self._stock_name}] 시그널 만료: {reason}")

        # Slack notification - signal termination
        if self._slack and self._signal_ctx:
            try:
                total_cycles = self._signal_ctx.cycle_count
                total_pnl = self._signal_ctx.total_pnl

                now = datetime.now()
                self._slack.send_message(
                    f"[{self._stock_name}] 스캘핑 시그널 종료\n"
                    f"• 사유: {reason}\n"
                    f"• 완료 사이클: {total_cycles}회\n"
                    f"• 누적 손익: {total_pnl:,}원\n"
                    f"• 평균 손익: {total_pnl//total_cycles if total_cycles > 0 else 0:,}원/사이클\n"
                    f"{now.strftime('%Y-%m-%d %H:%M:%S')}"
                )
            except Exception as e:
                logger.warning(f"[scalping] Slack 알림 실패: {e}")

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

        # main_beam_1 등 limit_order 전략: metadata에서 매도가 사용
        if (
            self._signal_ctx
            and self._signal_ctx.metadata.get("is_limit_order")
            and self._signal_ctx.metadata.get("sell_price")
        ):
            sell_price = self._signal_ctx.metadata["sell_price"]
        else:
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
            self._sell_order_time = datetime.now()  # 매도 주문 시간 기록
            self._last_sell_fill_time = None  # 체결 시간 초기화
            self._last_order_check_time = None  # 첫 체결 확인 즉시 실행
            self._transition(ScalpingState.SELL_PENDING)

            # DB 저장
            self._save_order_to_db(result.order_id, "SELL", self._held_qty, sell_price)

            logger.info(
                f"[scalping][{self._stock_name}] 매도 주문: "
                f"{sell_price:,}원 x {self._held_qty}주 "
                f"(매수가 {self._held_avg_price:,.0f} + {self._config.sell_profit_pct*100:.1f}%)"
            )

            # Slack notification - sell order
            if self._slack:
                try:
                    profit_pct = self._config.sell_profit_pct * 100
                    expected_profit = int((sell_price - self._held_avg_price) * self._held_qty)
                    expected_rate = ((sell_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0
                    self._slack.notify_sell(
                        stock_code=self._stock_code,
                        stock_name=self._stock_name,
                        quantity=self._held_qty,
                        price=sell_price,
                        profit_loss=expected_profit,
                        profit_rate=expected_rate,
                        strategy_name=self._strategy_name,
                        reason=f"목표 +{profit_pct:.1f}% 매도 ({sell_price:,}원)",
                    )
                except Exception as e:
                    logger.warning(f"[scalping] Slack 알림 실패: {e}")
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 매도 주문 실패: {result.message} → 시장가 매도"
            )
            self._market_sell_all("지정가 매도 실패 → 시장가")

    def _market_sell_all(self, reason: str, force_immediate: bool = False) -> None:
        """시장가 전량 매도"""
        if self._held_qty <= 0:
            self._log_signal_summary()
            self._reset_to_idle()
            return

        sell_qty = self._held_qty

        logger.info(
            f"[scalping][{self._stock_name}] 시장가 매도: "
            f"{sell_qty}주 ({reason})"
        )

        # Slack notification - market sell order
        if self._slack:
            try:
                self._slack.notify_sell(
                    stock_code=self._stock_code,
                    stock_name=self._stock_name,
                    quantity=sell_qty,
                    price=0,
                    profit_loss=0,
                    profit_rate=0.0,
                    strategy_name=self._strategy_name,
                    reason=f"시장가 매도 ({reason})",
                )
            except Exception as e:
                logger.warning(f"[scalping] Slack 알림 실패: {e}")

        result = self._broker.place_market_order(
            stock_code=self._stock_code,
            side=OrderSide.SELL,
            quantity=sell_qty,
        )

        if result.success:
            # DB 저장 (시장가)
            self._save_order_to_db(
                result.order_id, "SELL", sell_qty, 0, order_type="market"
            )

            if force_immediate:
                # 비상 경로: 즉시 처리 (deactivate 등)
                self._update_order_fill_in_db(
                    result.order_id, sell_qty, 0,
                    pnl=self._sold_pnl, avg_cost=self._held_avg_price, pnl_rate=0.0,
                )
                self._record_cycle_complete(self._sold_pnl)

                # DailyReport 업데이트 (강제 매도 손익 반영)
                if self._report_generator:
                    try:
                        report = self._report_generator.generate()
                        self._report_generator.save_to_db(report)
                    except Exception as e:
                        logger.warning(f"[scalping] Daily summary 업데이트 실패: {e}")

                self._clear_position()
                self._clear_sell_order()
                self._log_signal_summary()
                self._reset_to_idle()
            else:
                # 정상 경로: SELL_PENDING으로 전환, WS 체결 대기
                saved_sold_pnl = self._sold_pnl  # 기존 부분 매도 PnL 보존
                self._sell_order_id = result.order_id
                self._sell_order_branch = getattr(result, 'order_branch', None)
                self._sell_order_price = 0  # 시장가
                self._sell_order_qty = sell_qty
                self._sell_order_time = datetime.now()  # 매도 주문 시간 기록
                self._last_sell_fill_time = None  # 체결 시간 초기화
                self._sold_qty = 0          # 새 주문의 누적 초기화
                self._sold_pnl = saved_sold_pnl  # 이전 부분 매도 PnL 유지
                self._last_order_check_time = None  # REST 즉시 확인 가능
                self._transition(ScalpingState.SELL_PENDING)
        else:
            logger.error(
                f"[scalping][{self._stock_name}] 시장가 매도 실패: {result.message}"
            )
            # 주문 실패: 기존 부분 매도 PnL만 기록
            if self._sold_pnl != 0:
                self._record_cycle_complete(self._sold_pnl)
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
        """매수 취소 후 복귀 (limit_order는 IDLE, 일반은 MONITORING)"""
        cancel_qty = self._buy_order_qty
        cancel_price = self._buy_order_price
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

        # limit_order 모드: IDLE로 복귀 (boundary_tracker 미사용, 다음 신호 대기)
        if self._signal_ctx and self._signal_ctx.metadata.get("is_limit_order"):
            logger.info(
                f"[scalping][{self._stock_name}] limit_order 매수 미체결 → IDLE"
            )
            if self._slack:
                try:
                    self._slack.send_message(
                        f"⏱️ [{self._stock_name}] 매수 미체결 취소\n"
                        f"• {cancel_price:,}원 x {cancel_qty}주\n"
                        f"• 다음 신호 대기 중"
                    )
                except Exception:
                    pass
            self._log_signal_summary()
            self._reset_to_idle()
            return

        # 기존 scalping: MONITORING으로 복귀 (바운더리 재탐색)
        self._transition(ScalpingState.MONITORING)

        # Slack 알림 - 매수 타임아웃 취소
        if self._slack:
            try:
                self._slack.send_message(
                    f"⏱️ [{self._stock_name}] 매수 주문 타임아웃 취소\n"
                    f"• {cancel_price:,}원 x {cancel_qty}주 → 미체결 취소\n"
                    f"• 바운더리 재탐색 중"
                )
            except Exception:
                pass

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
            filled_qty, _ = self._broker.get_order_status(
                self._sell_order_id,
                stock_code=self._stock_code,
                order_qty=self._sell_order_qty,
                side=OrderSide.SELL,
            )
            # REST에서 확인한 추가 체결 (WS에서 미처리분만)
            new_fills = max(0, filled_qty - self._sold_qty)
            if new_fills > 0:
                sell_price = self._sell_order_price if self._sell_order_price > 0 else self._held_avg_price
                additional_pnl = int((sell_price - self._held_avg_price) * new_fills)
                self._sold_pnl += additional_pnl
                self._held_qty = max(self._held_qty - new_fills, 0)
                self._sold_qty = filled_qty  # sold_qty 동기화

            # orders 테이블 업데이트 (_clear_sell_order 전에!)
            if filled_qty > 0 and self._sell_order_id:
                sell_price = self._sell_order_price if self._sell_order_price > 0 else self._held_avg_price
                pnl_rate = ((sell_price / self._held_avg_price) - 1) * 100 if self._held_avg_price > 0 else 0.0
                self._update_order_fill_in_db(
                    self._sell_order_id,
                    filled_qty,
                    sell_price,
                    pnl=self._sold_pnl,
                    avg_cost=self._held_avg_price,
                    pnl_rate=pnl_rate,
                )

            # 전체 매도 PnL을 signal_ctx에 기록
            if self._signal_ctx and self._sold_pnl != 0:
                self._signal_ctx.total_pnl += self._sold_pnl

            # DailyReport 업데이트 (orders 업데이트 후)
            if self._report_generator and self._sold_pnl != 0:
                try:
                    report = self._report_generator.generate()
                    self._report_generator.save_to_db(report)
                except Exception as e:
                    logger.warning(f"[scalping] Daily summary 업데이트 실패: {e}")

            # 포지션 0이면 PositionManager 동기화
            if self._held_qty == 0:
                self._clear_position()

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

        # PositionManager 동기화
        if self._position_manager:
            try:
                self._position_manager.add_position(
                    stock_code=self._stock_code,
                    stock_name=self._stock_name,
                    quantity=new_qty,
                    avg_price=fill_price,
                    current_price=fill_price,
                    strategy_name=self._strategy_name,
                    order_id=self._buy_order_id or "",
                )
            except Exception as e:
                logger.error(f"[scalping] PositionManager 동기화 실패: {e}")

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


    def _compute_sell_pnl(self, sell_price: int, sell_qty: int) -> tuple[int, float]:
        """매도 손익 계산 (직접 계산)"""
        buy_price = self._held_avg_price
        pnl = int((sell_price - buy_price) * sell_qty)
        pct = ((sell_price / buy_price) - 1) * 100 if buy_price > 0 else 0.0
        return pnl, pct

    def _clear_sell_order(self) -> None:
        self._sell_order_id = None
        self._sell_order_branch = None
        self._sell_order_price = 0
        self._sell_order_qty = 0
        self._sell_order_time = None
        self._last_sell_fill_time = None
        self._sold_qty = 0
        self._sold_pnl = 0

    def _clear_position(self) -> None:
        self._held_qty = 0
        self._held_avg_price = 0.0

        # PositionManager에서 포지션 제거
        if self._position_manager:
            try:
                self._position_manager.remove_position(self._stock_code)
            except Exception as e:
                logger.error(f"[scalping] PositionManager 포지션 제거 실패: {e}")

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
        # Slack dedup 초기화: 다음 시그널 알림 허용
        if self._slack:
            self._slack.reset_signal_for_key(self._stock_code, self._strategy_name)

        # EC-6: 보유 포지션이 있으면 시장가 매도 후 종료
        if self._held_qty > 0:
            logger.warning(
                f"[scalping][{self._stock_name}] 거래 중단 전 포지션 청산: {self._held_qty}주"
            )
            self._market_sell_all("최대 breach 도달")
            # market_sell_all이 SELL_PENDING으로 전환하므로
            # 체결 완료 후 COOLDOWN → IDLE로 자연스럽게 전환됨
            return

        # 포지션 없으면 바로 IDLE
        self._signal_ctx = None
        self._clear_buy_order()
        self._clear_sell_order()
        self._clear_position()
        self._cooldown_start = None
        self._price_tracker.reset()
        self._boundary_tracker.reset()
        self._transition(ScalpingState.IDLE)

    # ── DB 저장 헬퍼 ──────────────────────────────────────────

    def _save_order_to_db(
        self,
        order_id: str,
        side: str,
        quantity: int,
        price: int,
        order_type: str = "limit",
    ) -> None:
        """스캘핑 주문 DB 저장"""
        if not self._db:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with self._db.get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO orders
                    (order_id, stock_code, stock_name, side, order_type,
                     quantity, price, filled_quantity, filled_price,
                     status, strategy_name, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 'submitted', ?, ?, ?)
                    """,
                    (
                        order_id,
                        self._stock_code,
                        self._stock_name,
                        side,
                        order_type,
                        quantity,
                        price,
                        self._strategy_name,
                        now,
                        now,
                    ),
                )
        except Exception as e:
            logger.warning(f"[scalping] 주문 DB 저장 실패: {e}")

    def _update_order_fill_in_db(
        self,
        order_id: str,
        filled_qty: int,
        filled_price: int,
        pnl: Optional[int] = None,
        avg_cost: Optional[float] = None,
        pnl_rate: Optional[float] = None,
    ) -> None:
        """주문 체결 정보 DB 업데이트"""
        if not self._db or not order_id:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with self._db.get_cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE orders SET
                        filled_quantity = ?, filled_price = ?,
                        status = 'filled',
                        pnl = ?, avg_cost = ?, pnl_rate = ?,
                        updated_at = ?
                    WHERE order_id = ?
                    """,
                    (
                        filled_qty,
                        filled_price,
                        pnl,
                        avg_cost,
                        pnl_rate,
                        now,
                        order_id,
                    ),
                )
        except Exception as e:
            logger.warning(f"[scalping] 주문 체결 DB 업데이트 실패: {e}")

    def _transition(self, new_state: ScalpingState) -> None:
        """상태 전환 (로그 포함)"""
        old_state = self._state
        self._state = new_state
        if old_state != new_state:
            logger.debug(
                f"[scalping][{self._stock_name}] "
                f"상태 전환: {old_state.value} → {new_state.value}"
            )

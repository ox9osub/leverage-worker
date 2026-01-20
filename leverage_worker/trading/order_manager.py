"""
주문 관리 모듈

주문 실행, 체결 확인, 미체결 취소
- 중복 주문 방지
- 주문 상태 추적
- 체결 확인 및 포지션 업데이트
- 감사 추적 (SQLite)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set, Callable

from leverage_worker.data.database import Database
from leverage_worker.trading.broker import (
    KISBroker,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderInfo,
)
from leverage_worker.trading.position_manager import PositionManager
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.audit_logger import get_audit_logger

logger = get_logger(__name__)


class OrderState(Enum):
    """내부 주문 상태"""
    PENDING = "pending"         # 주문 대기
    SUBMITTED = "submitted"     # 접수됨
    PARTIAL = "partial"         # 부분 체결
    FILLED = "filled"           # 전량 체결
    CANCELLED = "cancelled"     # 취소됨
    FAILED = "failed"           # 실패


@dataclass
class ManagedOrder:
    """관리 주문"""
    order_id: str
    stock_code: str
    stock_name: str
    side: OrderSide
    quantity: int
    price: int
    strategy_name: Optional[str]
    state: OrderState
    filled_qty: int = 0
    filled_price: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    branch_no: str = ""

    @property
    def is_pending(self) -> bool:
        return self.state in (OrderState.PENDING, OrderState.SUBMITTED, OrderState.PARTIAL)

    @property
    def is_complete(self) -> bool:
        return self.state in (OrderState.FILLED, OrderState.CANCELLED, OrderState.FAILED)

    @property
    def remaining_qty(self) -> int:
        return self.quantity - self.filled_qty


class OrderManager:
    """
    주문 관리자

    - 주문 실행
    - 체결 확인
    - 중복 주문 방지
    - 미체결 일괄 취소
    """

    def __init__(
        self,
        broker: KISBroker,
        position_manager: PositionManager,
        database: Database,
    ):
        self._broker = broker
        self._position_manager = position_manager
        self._db = database

        # 활성 주문: order_id -> ManagedOrder
        self._active_orders: Dict[str, ManagedOrder] = {}

        # 종목별 진행 중 주문 (중복 방지)
        self._pending_stocks: Set[str] = set()

        # 콜백
        self._on_fill_callback: Optional[Callable] = None

        # 감사 추적 로거
        self._audit = get_audit_logger()

        logger.info("OrderManager initialized")

    def set_on_fill_callback(self, callback: Callable) -> None:
        """체결 콜백 설정"""
        self._on_fill_callback = callback

    def place_buy_order(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        strategy_name: str,
        check_deposit: bool = True,
    ) -> Optional[str]:
        """
        매수 주문 실행

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            strategy_name: 전략 이름
            check_deposit: 예수금 확인 여부 (기본: True)

        Returns:
            주문 ID 또는 None (실패 시)
        """
        # 중복 주문 체크
        if stock_code in self._pending_stocks:
            logger.warning(f"[{stock_code}] 매수 주문 차단: 중복 주문 (pending 상태의 주문 존재)")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="BUY",
                quantity=quantity,
                price=0,
                strategy_name=strategy_name,
                status="rejected",
                reason="duplicate_order_blocked",
            )
            return None

        # 예수금 확인 (시장가 주문 시 현재가 기준으로 계산)
        if check_deposit:
            price_info = self._broker.get_current_price(stock_code)
            if not price_info:
                logger.error(f"[{stock_code}] 매수 주문 차단: 현재가 조회 실패")
                self._audit.log_order(
                    event_type="ORDER_REJECTED",
                    module="OrderManager",
                    stock_code=stock_code,
                    stock_name=stock_name,
                    order_id=None,
                    side="BUY",
                    quantity=quantity,
                    price=0,
                    strategy_name=strategy_name,
                    status="rejected",
                    reason="price_fetch_failed",
                )
                return None

            current_price = price_info.current_price
            # 1% 마진 적용 (시장가 슬리피지 대비)
            required_amount = int(current_price * quantity * 1.01)

            deposit = self._broker.get_deposit()
            if deposit is None:
                logger.warning(f"Cannot get deposit, proceeding without check: {stock_code}")
            elif deposit < required_amount:
                logger.warning(
                    f"[{stock_code}] 매수 주문 차단: 예수금 부족 "
                    f"(필요: {required_amount:,}원, 보유: {deposit:,}원)"
                )
                self._audit.log_order(
                    event_type="ORDER_REJECTED",
                    module="OrderManager",
                    stock_code=stock_code,
                    stock_name=stock_name,
                    order_id=None,
                    side="BUY",
                    quantity=quantity,
                    price=current_price,
                    strategy_name=strategy_name,
                    status="rejected",
                    reason="insufficient_deposit",
                    metadata={"required": required_amount, "available": deposit},
                )
                return None

            logger.debug(
                f"Deposit check passed: {stock_code} "
                f"required={required_amount:,}, available={deposit:,}"
            )

        # 주문 실행
        result = self._broker.place_market_order(stock_code, OrderSide.BUY, quantity)

        if not result.success:
            logger.error(f"[{stock_code}] 매수 주문 실패: 브로커 거절 - {result.message}")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="BUY",
                quantity=quantity,
                price=0,
                strategy_name=strategy_name,
                status="rejected",
                reason=f"broker_rejected: {result.message}",
            )
            return None

        # 주문 등록
        order = ManagedOrder(
            order_id=result.order_id,
            stock_code=stock_code,
            stock_name=stock_name,
            side=OrderSide.BUY,
            quantity=quantity,
            price=result.price,
            strategy_name=strategy_name,
            state=OrderState.SUBMITTED,
        )

        self._active_orders[result.order_id] = order
        self._pending_stocks.add(stock_code)

        # DB 저장
        self._save_order_to_db(order)

        # 감사 로그 기록 (주문 접수)
        self._audit.log_order(
            event_type="ORDER_SUBMIT",
            module="OrderManager",
            stock_code=stock_code,
            stock_name=stock_name,
            order_id=result.order_id,
            side="BUY",
            quantity=quantity,
            price=result.price,
            strategy_name=strategy_name,
            status="submitted",
        )

        logger.info(f"Buy order placed: {stock_code} x {quantity} (ID: {result.order_id})")

        return result.order_id

    def place_buy_order_with_chase(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        deposit: int,
        strategy_name: str,
        interval: float = 0.5,
        max_retry: int = 10,
    ) -> Optional[str]:
        """
        매도호가1 추격 매수 (지정가 주문 + 반복 정정)

        - 0.5초마다 미체결 확인
        - 매도호가1로 정정 (최대 10회)
        - 가격 상승 시 수량 자동 조정

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 목표 수량
            deposit: 사용 가능 예수금
            strategy_name: 전략 이름
            interval: 정정 간격 (초)
            max_retry: 최대 정정 횟수

        Returns:
            주문 ID 또는 None (실패 시)
        """
        import time

        # 중복 주문 체크
        if stock_code in self._pending_stocks:
            logger.warning(f"[{stock_code}] 매수 주문 차단: 중복 주문 (pending 상태의 주문 존재)")
            return None

        # 1. 매도호가1 조회
        ask_price = self._broker.get_asking_price(stock_code)
        if not ask_price or ask_price <= 0:
            logger.error(f"[{stock_code}] 매수 주문 차단: 매도호가1 조회 실패")
            return None

        # 2. 초기 수량 계산 (예수금 기준)
        max_qty_by_deposit = deposit // ask_price
        order_qty = min(quantity, max_qty_by_deposit)

        if order_qty < 1:
            logger.warning(f"[{stock_code}] 매수 주문 차단: 예수금 부족 (예수금: {deposit:,}, 호가: {ask_price:,})")
            return None

        logger.info(
            f"[{stock_code}] 지정가 매수 시작: {order_qty}주 @ {ask_price:,}원 "
            f"(예수금: {deposit:,}원)"
        )

        # 3. 지정가 주문 실행
        result = self._broker.place_limit_order(stock_code, OrderSide.BUY, order_qty, ask_price)

        if not result.success:
            logger.error(f"[{stock_code}] 지정가 매수 실패: {result.message}")
            return None

        order_id = result.order_id
        order_branch = result.order_branch

        # 주문 등록
        order = ManagedOrder(
            order_id=order_id,
            stock_code=stock_code,
            stock_name=stock_name,
            side=OrderSide.BUY,
            quantity=order_qty,
            price=ask_price,
            strategy_name=strategy_name,
            state=OrderState.SUBMITTED,
        )
        self._active_orders[order_id] = order
        self._pending_stocks.add(stock_code)

        # DB 저장
        self._save_order_to_db(order)

        # 감사 로그 기록
        self._audit.log_order(
            event_type="ORDER_SUBMIT",
            module="OrderManager",
            stock_code=stock_code,
            stock_name=stock_name,
            order_id=order_id,
            side="BUY",
            quantity=order_qty,
            price=ask_price,
            strategy_name=strategy_name,
            status="submitted",
        )

        # 4. 반복 정정 루프
        total_filled = 0
        current_price = ask_price

        for retry in range(max_retry):
            time.sleep(interval)  # 대기

            # 체결 상태 확인
            filled_qty, unfilled_qty = self._broker.get_order_status(order_id)
            total_filled = filled_qty

            if unfilled_qty == 0:
                # 전량 체결 완료
                logger.info(f"[{stock_code}] 전량 체결 완료: {total_filled}주")
                break

            # 미체결 있음 → 매도호가1 재조회
            new_ask_price = self._broker.get_asking_price(stock_code)
            if not new_ask_price or new_ask_price <= 0:
                logger.warning(f"[{stock_code}] 매도호가1 재조회 실패, 대기")
                continue

            # 가격 변동 시에만 정정
            if new_ask_price == current_price:
                logger.debug(f"[{stock_code}] 가격 변동 없음, 대기 (미체결: {unfilled_qty}주)")
                continue

            # 가격 상승 시 수량 재계산
            if new_ask_price > current_price:
                # 남은 예수금 = 초기 예수금 - (체결수량 * 체결가격들의 합)
                # 간소화: 체결수량 * 현재 기준가격으로 계산
                used_amount = total_filled * current_price
                remaining_deposit = deposit - used_amount

                # 새 가격으로 주문 가능한 수량
                affordable_qty = remaining_deposit // new_ask_price
                new_order_qty = min(unfilled_qty, affordable_qty)

                if new_order_qty < unfilled_qty:
                    logger.info(
                        f"[{stock_code}] 가격 상승으로 수량 조정: "
                        f"{unfilled_qty}주 → {new_order_qty}주 "
                        f"(가격: {current_price:,} → {new_ask_price:,})"
                    )
            else:
                new_order_qty = unfilled_qty

            if new_order_qty <= 0:
                logger.warning(f"[{stock_code}] 예수금 부족으로 추가 매수 불가")
                break

            # 정정 직전 체결 상태 재확인 (race condition 방지)
            latest_filled, latest_unfilled = self._broker.get_order_status(order_id)
            if latest_unfilled == 0:
                # 정정 전에 전량 체결됨
                logger.info(f"[{stock_code}] 정정 전 전량 체결 완료: {latest_filled}주")
                total_filled = latest_filled
                break

            # 실제 미체결 수량으로 정정 수량 조정
            if latest_unfilled < new_order_qty:
                logger.info(
                    f"[{stock_code}] 체결 진행으로 수량 재조정: "
                    f"{new_order_qty}주 → {latest_unfilled}주"
                )
                new_order_qty = latest_unfilled
                total_filled = latest_filled

            # 정정 주문
            if self._broker.modify_order(order_id, order_branch, new_order_qty, new_ask_price):
                current_price = new_ask_price
                logger.info(
                    f"[{stock_code}] 정정 주문 #{retry+1}: "
                    f"{new_order_qty}주 @ {new_ask_price:,}원"
                )
            else:
                logger.warning(f"[{stock_code}] 정정 주문 실패")

        # 최종 상태 로그
        final_filled, final_unfilled = self._broker.get_order_status(order_id)
        if final_unfilled > 0:
            logger.warning(
                f"[{stock_code}] 매수 부분 체결: {final_filled}주 체결, {final_unfilled}주 미체결"
            )

        return order_id

    def place_sell_order(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        strategy_name: Optional[str],
    ) -> Optional[str]:
        """
        매도 주문 실행

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            strategy_name: 전략 이름 (None이면 전략 없이 매도)

        Returns:
            주문 ID 또는 None (실패 시)
        """
        # 중복 주문 체크
        if stock_code in self._pending_stocks:
            logger.warning(f"[{stock_code}] 매도 주문 차단: 중복 주문 (pending 상태의 주문 존재)")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="SELL",
                quantity=quantity,
                price=0,
                strategy_name=strategy_name or "",
                status="rejected",
                reason="duplicate_order_blocked",
            )
            return None

        # 주문 실행
        result = self._broker.place_market_order(stock_code, OrderSide.SELL, quantity)

        if not result.success:
            logger.error(f"[{stock_code}] 매도 주문 실패: 브로커 거절 - {result.message}")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="SELL",
                quantity=quantity,
                price=0,
                strategy_name=strategy_name or "",
                status="rejected",
                reason=f"broker_rejected: {result.message}",
            )
            return None

        # 주문 등록
        order = ManagedOrder(
            order_id=result.order_id,
            stock_code=stock_code,
            stock_name=stock_name,
            side=OrderSide.SELL,
            quantity=quantity,
            price=result.price,
            strategy_name=strategy_name,
            state=OrderState.SUBMITTED,
        )

        self._active_orders[result.order_id] = order
        self._pending_stocks.add(stock_code)

        # DB 저장
        self._save_order_to_db(order)

        # 감사 로그 기록 (주문 접수)
        self._audit.log_order(
            event_type="ORDER_SUBMIT",
            module="OrderManager",
            stock_code=stock_code,
            stock_name=stock_name,
            order_id=result.order_id,
            side="SELL",
            quantity=quantity,
            price=result.price,
            strategy_name=strategy_name or "",
            status="submitted",
        )

        logger.info(f"Sell order placed: {stock_code} x {quantity} (ID: {result.order_id})")

        return result.order_id

    def place_sell_order_with_fallback(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        strategy_name: Optional[str],
        limit_price: int,
        fallback_seconds: float = 1.0,
    ) -> Optional[str]:
        """
        지정가 매도 주문 후 미체결 시 시장가로 전환

        TP 달성 시 사용: 지정가로 먼저 시도 후 빠르게 시장가 전환

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            strategy_name: 전략 이름
            limit_price: 지정가 (TP 목표가)
            fallback_seconds: 미체결 대기 시간 (초)

        Returns:
            주문 ID 또는 None (실패 시)
        """
        import time

        # 중복 주문 체크
        if stock_code in self._pending_stocks:
            logger.warning(f"[{stock_code}] 매도 주문 차단: 중복 주문 (pending 상태의 주문 존재)")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="SELL",
                quantity=quantity,
                price=limit_price,
                strategy_name=strategy_name or "",
                status="rejected",
                reason="duplicate_order_blocked",
            )
            return None

        # 1. 지정가 매도 주문 실행
        logger.info(
            f"[{stock_code}] 지정가 매도 시작: {quantity}주 @ {limit_price:,}원 "
            f"(fallback: {fallback_seconds}초)"
        )

        result = self._broker.place_limit_order(stock_code, OrderSide.SELL, quantity, limit_price)

        if not result.success:
            logger.error(f"[{stock_code}] 지정가 매도 실패: {result.message}")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="SELL",
                quantity=quantity,
                price=limit_price,
                strategy_name=strategy_name or "",
                status="rejected",
                reason=f"broker_rejected: {result.message}",
            )
            return None

        order_id = result.order_id

        # 주문 등록
        order = ManagedOrder(
            order_id=order_id,
            stock_code=stock_code,
            stock_name=stock_name,
            side=OrderSide.SELL,
            quantity=quantity,
            price=limit_price,
            strategy_name=strategy_name,
            state=OrderState.SUBMITTED,
        )
        self._active_orders[order_id] = order
        self._pending_stocks.add(stock_code)

        # DB 저장
        self._save_order_to_db(order)

        # 감사 로그 기록
        self._audit.log_order(
            event_type="ORDER_SUBMIT",
            module="OrderManager",
            stock_code=stock_code,
            stock_name=stock_name,
            order_id=order_id,
            side="SELL",
            quantity=quantity,
            price=limit_price,
            strategy_name=strategy_name or "",
            status="submitted",
            reason="limit_order_with_fallback",
        )

        # 2. 대기
        time.sleep(fallback_seconds)

        # 3. 체결 상태 확인
        filled_qty, unfilled_qty = self._broker.get_order_status(order_id)

        if unfilled_qty == 0:
            # 전량 체결 완료
            logger.info(f"[{stock_code}] 지정가 매도 전량 체결: {filled_qty}주 @ {limit_price:,}원")
            return order_id

        # 4. 미체결 있음 → 취소 후 시장가 전환
        logger.info(
            f"[{stock_code}] 지정가 매도 미체결: {unfilled_qty}주 → 시장가 전환"
        )

        # 기존 주문 취소
        if not self._broker.cancel_order(order_id):
            logger.warning(f"[{stock_code}] 지정가 주문 취소 실패, 체결 재확인")
            # 취소 실패 시 체결 상태 재확인
            filled_qty, unfilled_qty = self._broker.get_order_status(order_id)
            if unfilled_qty == 0:
                logger.info(f"[{stock_code}] 취소 실패했으나 전량 체결됨")
                return order_id

        # pending 상태 해제 (시장가 재주문을 위해)
        self._pending_stocks.discard(stock_code)
        if order_id in self._active_orders:
            del self._active_orders[order_id]

        # 미체결 수량만큼 시장가 매도
        logger.info(f"[{stock_code}] 시장가 매도 전환: {unfilled_qty}주")

        market_result = self._broker.place_market_order(stock_code, OrderSide.SELL, unfilled_qty)

        if not market_result.success:
            logger.error(f"[{stock_code}] 시장가 매도 전환 실패: {market_result.message}")
            self._audit.log_order(
                event_type="ORDER_REJECTED",
                module="OrderManager",
                stock_code=stock_code,
                stock_name=stock_name,
                order_id=None,
                side="SELL",
                quantity=unfilled_qty,
                price=0,
                strategy_name=strategy_name or "",
                status="rejected",
                reason=f"market_fallback_failed: {market_result.message}",
            )
            return order_id  # 부분 체결된 지정가 주문 ID 반환

        # 시장가 주문 등록
        market_order = ManagedOrder(
            order_id=market_result.order_id,
            stock_code=stock_code,
            stock_name=stock_name,
            side=OrderSide.SELL,
            quantity=unfilled_qty,
            price=market_result.price,
            strategy_name=strategy_name,
            state=OrderState.SUBMITTED,
        )
        self._active_orders[market_result.order_id] = market_order
        self._pending_stocks.add(stock_code)

        # DB 저장
        self._save_order_to_db(market_order)

        # 감사 로그 기록
        self._audit.log_order(
            event_type="ORDER_SUBMIT",
            module="OrderManager",
            stock_code=stock_code,
            stock_name=stock_name,
            order_id=market_result.order_id,
            side="SELL",
            quantity=unfilled_qty,
            price=market_result.price,
            strategy_name=strategy_name or "",
            status="submitted",
            reason="market_fallback_from_limit",
        )

        logger.info(
            f"[{stock_code}] 시장가 매도 전환 완료: {unfilled_qty}주 (ID: {market_result.order_id})"
        )

        return market_result.order_id

    def check_fills(self) -> List[ManagedOrder]:
        """
        체결 확인 및 포지션 업데이트

        Returns:
            체결된 주문 리스트
        """
        if not self._active_orders:
            return []

        # 브로커에서 당일 주문 조회
        broker_orders = self._broker.get_today_orders()
        broker_orders_dict = {o.order_id: o for o in broker_orders}

        filled_orders = []

        for order_id, order in list(self._active_orders.items()):
            if order_id not in broker_orders_dict:
                continue

            broker_order = broker_orders_dict[order_id]

            # 상태 업데이트
            prev_filled = order.filled_qty
            order.filled_qty = broker_order.filled_qty
            order.filled_price = broker_order.filled_price
            order.branch_no = broker_order.branch_no
            order.updated_at = datetime.now()

            # 상태 판단
            if broker_order.status == OrderStatus.FILLED:
                order.state = OrderState.FILLED
            elif broker_order.status == OrderStatus.PARTIAL:
                order.state = OrderState.PARTIAL
            elif broker_order.status == OrderStatus.CANCELLED:
                order.state = OrderState.CANCELLED

            # 신규 체결분 처리
            new_filled = order.filled_qty - prev_filled
            if new_filled > 0:
                self._handle_fill(order, new_filled)

            # 완료된 주문 정리
            if order.is_complete:
                self._active_orders.pop(order_id)
                self._pending_stocks.discard(order.stock_code)
                filled_orders.append(order)

                # DB 업데이트
                self._update_order_in_db(order)

                logger.info(
                    f"Order completed: {order.stock_code} {order.side.value} "
                    f"x {order.filled_qty} @ {order.filled_price}"
                )

        return filled_orders

    def _handle_fill(self, order: ManagedOrder, filled_qty: int) -> None:
        """체결 처리"""
        # 감사 로그 기록 (체결)
        self._audit.log_order(
            event_type="ORDER_FILLED",
            module="OrderManager",
            stock_code=order.stock_code,
            stock_name=order.stock_name,
            order_id=order.order_id,
            side=order.side.value,
            quantity=filled_qty,
            price=order.filled_price,
            strategy_name=order.strategy_name or "",
            status="filled",
            metadata={
                "total_filled": order.filled_qty,
                "remaining": order.remaining_qty,
            },
        )

        if order.side == OrderSide.BUY:
            # 매수 체결 → 포지션 추가
            self._position_manager.add_position(
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                quantity=filled_qty,
                avg_price=order.filled_price,
                current_price=order.filled_price,
                strategy_name=order.strategy_name,
                order_id=order.order_id,
            )
        else:
            # 매도 체결 → 포지션 업데이트/제거
            position = self._position_manager.get_position(order.stock_code)
            if position:
                remaining = position.quantity - filled_qty
                if remaining <= 0:
                    self._position_manager.remove_position(order.stock_code)
                else:
                    self._position_manager.update_quantity(order.stock_code, remaining)

        # 콜백 호출
        if self._on_fill_callback:
            self._on_fill_callback(order, filled_qty)

    def cancel_order(self, order_id: str) -> bool:
        """단일 주문 취소"""
        if order_id not in self._active_orders:
            return False

        order = self._active_orders[order_id]

        success = self._broker.cancel_order(
            order_id=order_id,
            order_branch=order.branch_no,
            quantity=order.remaining_qty,
        )

        if success:
            order.state = OrderState.CANCELLED
            order.updated_at = datetime.now()
            self._active_orders.pop(order_id)
            self._pending_stocks.discard(order.stock_code)
            self._update_order_in_db(order)

            # 감사 로그 기록 (취소)
            self._audit.log_order(
                event_type="ORDER_CANCELLED",
                module="OrderManager",
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                order_id=order_id,
                side=order.side.value,
                quantity=order.remaining_qty,
                price=order.price,
                strategy_name=order.strategy_name or "",
                status="cancelled",
                reason="user_requested",
            )

        return success

    def cancel_all_pending(self) -> int:
        """
        모든 미체결 주문 취소

        Returns:
            취소된 주문 수
        """
        cancelled = self._broker.cancel_all_pending_orders()

        # 내부 상태 정리 + 감사 로깅
        for order in list(self._active_orders.values()):
            order.state = OrderState.CANCELLED
            order.updated_at = datetime.now()
            self._update_order_in_db(order)

            # 감사 로그 기록 (일괄 취소)
            self._audit.log_order(
                event_type="ORDER_CANCELLED",
                module="OrderManager",
                stock_code=order.stock_code,
                stock_name=order.stock_name,
                order_id=order.order_id,
                side=order.side.value,
                quantity=order.remaining_qty,
                price=order.price,
                strategy_name=order.strategy_name or "",
                status="cancelled",
                reason="cancel_all_pending",
            )

        self._active_orders.clear()
        self._pending_stocks.clear()

        return cancelled

    def get_active_orders(self) -> List[ManagedOrder]:
        """활성 주문 조회"""
        return list(self._active_orders.values())

    def has_pending_order(self, stock_code: str) -> bool:
        """종목에 진행 중인 주문이 있는지 확인"""
        return stock_code in self._pending_stocks

    def get_today_trade_count(self, stock_code: str) -> int:
        """당일 해당 종목의 거래 횟수 조회"""
        today = datetime.now().strftime("%Y-%m-%d")

        query = """
            SELECT COUNT(*) as cnt FROM orders
            WHERE stock_code = ? AND DATE(created_at) = ? AND status = 'filled'
        """

        row = self._db.fetch_one(query, (stock_code, today))
        return row["cnt"] if row else 0

    def _save_order_to_db(self, order: ManagedOrder) -> None:
        """주문 DB 저장"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with self._db.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO orders
                (order_id, stock_code, stock_name, side, order_type, quantity,
                 price, filled_quantity, filled_price, status, strategy_name,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order.order_id,
                order.stock_code,
                order.stock_name,
                order.side.value,
                "market",
                order.quantity,
                order.price,
                order.filled_qty,
                order.filled_price,
                order.state.value,
                order.strategy_name,
                order.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                now,
            ))

    def _update_order_in_db(self, order: ManagedOrder) -> None:
        """주문 DB 업데이트"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with self._db.get_cursor() as cursor:
            cursor.execute("""
                UPDATE orders SET
                    filled_quantity = ?,
                    filled_price = ?,
                    status = ?,
                    updated_at = ?
                WHERE order_id = ?
            """, (
                order.filled_qty,
                order.filled_price,
                order.state.value,
                now,
                order.order_id,
            ))

"""
주문 관리 모듈

주문 실행, 체결 확인, 미체결 취소
- 중복 주문 방지
- 주문 상태 추적
- 체결 확인 및 포지션 업데이트
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
    ) -> Optional[str]:
        """
        매수 주문 실행

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            strategy_name: 전략 이름

        Returns:
            주문 ID 또는 None (실패 시)
        """
        # 중복 주문 체크
        if stock_code in self._pending_stocks:
            logger.warning(f"Duplicate order blocked: {stock_code}")
            return None

        # 주문 실행
        result = self._broker.place_market_order(stock_code, OrderSide.BUY, quantity)

        if not result.success:
            logger.error(f"Buy order failed: {stock_code} - {result.message}")
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

        logger.info(f"Buy order placed: {stock_code} x {quantity} (ID: {result.order_id})")

        return result.order_id

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
            logger.warning(f"Duplicate order blocked: {stock_code}")
            return None

        # 주문 실행
        result = self._broker.place_market_order(stock_code, OrderSide.SELL, quantity)

        if not result.success:
            logger.error(f"Sell order failed: {stock_code} - {result.message}")
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

        logger.info(f"Sell order placed: {stock_code} x {quantity} (ID: {result.order_id})")

        return result.order_id

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

        return success

    def cancel_all_pending(self) -> int:
        """
        모든 미체결 주문 취소

        Returns:
            취소된 주문 수
        """
        cancelled = self._broker.cancel_all_pending_orders()

        # 내부 상태 정리
        for order in list(self._active_orders.values()):
            order.state = OrderState.CANCELLED
            order.updated_at = datetime.now()
            self._update_order_in_db(order)

        self._active_orders.clear()
        self._pending_stocks.clear()

        logger.info(f"Cancelled all pending orders: {cancelled}")

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

"""
포지션 관리 모듈

보유 종목 및 전략 매핑 관리
- 포지션 추적
- 종목-전략 매핑
- 기존 보유 주식 관리 (전략 없이)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set

from leverage_worker.data.database import Database
from leverage_worker.trading.broker import KISBroker, Position, OrderSide
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ManagedPosition:
    """
    관리 포지션

    전략이 관리하는 포지션 정보
    """
    stock_code: str
    stock_name: str
    quantity: int
    avg_price: float
    current_price: int
    strategy_name: Optional[str]  # None이면 전략 없이 관리
    entry_order_id: Optional[str]
    entry_time: datetime
    updated_at: datetime = field(default_factory=datetime.now)

    @property
    def eval_amount(self) -> int:
        """평가금액"""
        return self.quantity * self.current_price

    @property
    def profit_loss(self) -> int:
        """평가손익"""
        return int((self.current_price - self.avg_price) * self.quantity)

    @property
    def profit_rate(self) -> float:
        """수익률 (%)"""
        if self.avg_price > 0:
            return ((self.current_price - self.avg_price) / self.avg_price) * 100
        return 0.0

    @property
    def is_managed_by_strategy(self) -> bool:
        """전략 관리 여부"""
        return self.strategy_name is not None


class PositionManager:
    """
    포지션 관리자

    - 전략별 포지션 추적
    - 브로커 잔고와 동기화
    - DB 영속화
    """

    def __init__(self, broker: KISBroker, database: Database):
        self._broker = broker
        self._db = database

        # 메모리 캐시: stock_code -> ManagedPosition
        self._positions: Dict[str, ManagedPosition] = {}

        # 전략별 종목 매핑: strategy_name -> Set[stock_code]
        self._strategy_stocks: Dict[str, Set[str]] = {}

        logger.info("PositionManager initialized")

    def sync_with_broker(self) -> None:
        """
        브로커 잔고와 동기화

        - 브로커의 실제 잔고를 조회
        - 기존 관리 포지션과 비교
        - 신규/삭제/업데이트 반영
        """
        broker_positions, _ = self._broker.get_balance()
        broker_codes = {p.stock_code for p in broker_positions}

        # 1. 브로커에 있는 포지션 업데이트/추가
        for bp in broker_positions:
            if bp.stock_code in self._positions:
                # 기존 포지션 업데이트
                mp = self._positions[bp.stock_code]
                mp.quantity = bp.quantity
                mp.avg_price = bp.avg_price
                mp.current_price = bp.current_price
                mp.updated_at = datetime.now()
            else:
                # 신규 포지션 (전략 없이 관리)
                mp = ManagedPosition(
                    stock_code=bp.stock_code,
                    stock_name=bp.stock_name,
                    quantity=bp.quantity,
                    avg_price=bp.avg_price,
                    current_price=bp.current_price,
                    strategy_name=None,  # 전략 없이 관리
                    entry_order_id=None,
                    entry_time=datetime.now(),
                )
                self._positions[bp.stock_code] = mp
                logger.info(
                    f"New position detected (unmanaged): {bp.stock_code} "
                    f"x {bp.quantity} @ {bp.avg_price}"
                )

        # 2. 브로커에 없는 포지션 제거
        for stock_code in list(self._positions.keys()):
            if stock_code not in broker_codes:
                mp = self._positions.pop(stock_code)
                # 전략 매핑에서도 제거
                if mp.strategy_name and mp.strategy_name in self._strategy_stocks:
                    self._strategy_stocks[mp.strategy_name].discard(stock_code)
                logger.info(f"Position removed: {stock_code}")

        # 3. DB 저장
        self._save_to_db()

        logger.debug(f"Synced {len(self._positions)} positions with broker")

    def get_position(self, stock_code: str) -> Optional[ManagedPosition]:
        """특정 종목 포지션 조회"""
        return self._positions.get(stock_code)

    def get_all_positions(self) -> List[ManagedPosition]:
        """전체 포지션 조회"""
        return list(self._positions.values())

    def get_strategy_positions(self, strategy_name: str) -> List[ManagedPosition]:
        """특정 전략의 포지션 조회"""
        return [
            p for p in self._positions.values()
            if p.strategy_name == strategy_name
        ]

    def get_unmanaged_positions(self) -> List[ManagedPosition]:
        """전략 없이 관리되는 포지션 조회"""
        return [
            p for p in self._positions.values()
            if p.strategy_name is None
        ]

    def add_position(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        avg_price: float,
        current_price: int,
        strategy_name: str,
        order_id: str,
    ) -> None:
        """
        포지션 추가 (매수 체결 후)

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            quantity: 수량
            avg_price: 평균가
            current_price: 현재가
            strategy_name: 전략 이름
            order_id: 주문 ID
        """
        now = datetime.now()

        if stock_code in self._positions:
            # 기존 포지션에 추가 (물타기)
            mp = self._positions[stock_code]
            total_qty = mp.quantity + quantity
            total_cost = (mp.quantity * mp.avg_price) + (quantity * avg_price)
            mp.avg_price = total_cost / total_qty
            mp.quantity = total_qty
            mp.current_price = current_price
            mp.updated_at = now
            logger.info(f"Position added to: {stock_code} +{quantity} (total: {total_qty})")
        else:
            # 신규 포지션
            mp = ManagedPosition(
                stock_code=stock_code,
                stock_name=stock_name,
                quantity=quantity,
                avg_price=avg_price,
                current_price=current_price,
                strategy_name=strategy_name,
                entry_order_id=order_id,
                entry_time=now,
            )
            self._positions[stock_code] = mp
            logger.info(f"New position: {stock_code} x {quantity} @ {avg_price}")

        # 전략 매핑
        if strategy_name:
            if strategy_name not in self._strategy_stocks:
                self._strategy_stocks[strategy_name] = set()
            self._strategy_stocks[strategy_name].add(stock_code)

        self._save_to_db()

    def remove_position(self, stock_code: str) -> Optional[ManagedPosition]:
        """
        포지션 제거 (매도 체결 후)

        Returns:
            제거된 포지션 또는 None
        """
        if stock_code not in self._positions:
            return None

        mp = self._positions.pop(stock_code)

        # 전략 매핑에서 제거
        if mp.strategy_name and mp.strategy_name in self._strategy_stocks:
            self._strategy_stocks[mp.strategy_name].discard(stock_code)

        self._save_to_db()
        logger.info(f"Position removed: {stock_code}")

        return mp

    def update_price(self, stock_code: str, current_price: int) -> None:
        """현재가 업데이트"""
        if stock_code in self._positions:
            self._positions[stock_code].current_price = current_price
            self._positions[stock_code].updated_at = datetime.now()

    def update_quantity(self, stock_code: str, quantity: int) -> None:
        """수량 업데이트 (부분 체결 등)"""
        if stock_code in self._positions:
            mp = self._positions[stock_code]
            mp.quantity = quantity
            mp.updated_at = datetime.now()

            if quantity <= 0:
                self.remove_position(stock_code)

    def assign_strategy(self, stock_code: str, strategy_name: str) -> bool:
        """
        기존 포지션에 전략 할당

        Args:
            stock_code: 종목코드
            strategy_name: 전략 이름

        Returns:
            성공 여부
        """
        if stock_code not in self._positions:
            return False

        mp = self._positions[stock_code]

        # 기존 전략 매핑 제거
        if mp.strategy_name and mp.strategy_name in self._strategy_stocks:
            self._strategy_stocks[mp.strategy_name].discard(stock_code)

        # 새 전략 할당
        mp.strategy_name = strategy_name
        mp.updated_at = datetime.now()

        if strategy_name not in self._strategy_stocks:
            self._strategy_stocks[strategy_name] = set()
        self._strategy_stocks[strategy_name].add(stock_code)

        self._save_to_db()
        logger.info(f"Strategy assigned: {stock_code} -> {strategy_name}")

        return True

    def get_total_eval_amount(self) -> int:
        """전체 평가금액"""
        return sum(p.eval_amount for p in self._positions.values())

    def get_total_profit_loss(self) -> int:
        """전체 평가손익"""
        return sum(p.profit_loss for p in self._positions.values())

    def _save_to_db(self) -> None:
        """DB에 포지션 저장"""
        # 기존 데이터 삭제
        with self._db.get_cursor() as cursor:
            cursor.execute("DELETE FROM positions")

        # 현재 포지션 저장
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for mp in self._positions.values():
            with self._db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO positions
                    (stock_code, stock_name, quantity, avg_price, current_price,
                     strategy_name, entry_order_id, entry_time, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    mp.stock_code,
                    mp.stock_name,
                    mp.quantity,
                    mp.avg_price,
                    mp.current_price,
                    mp.strategy_name,
                    mp.entry_order_id,
                    mp.entry_time.strftime("%Y-%m-%d %H:%M:%S"),
                    now,
                ))

    def load_from_db(self) -> None:
        """DB에서 포지션 로드"""
        rows = self._db.fetch_all("SELECT * FROM positions")

        for row in rows:
            mp = ManagedPosition(
                stock_code=row["stock_code"],
                stock_name=row["stock_name"],
                quantity=row["quantity"],
                avg_price=row["avg_price"],
                current_price=row["current_price"] or 0,
                strategy_name=row["strategy_name"],
                entry_order_id=row["entry_order_id"],
                entry_time=datetime.strptime(row["entry_time"], "%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
            )
            self._positions[mp.stock_code] = mp

            # 전략 매핑 복원
            if mp.strategy_name:
                if mp.strategy_name not in self._strategy_stocks:
                    self._strategy_stocks[mp.strategy_name] = set()
                self._strategy_stocks[mp.strategy_name].add(mp.stock_code)

        logger.info(f"Loaded {len(self._positions)} positions from DB")

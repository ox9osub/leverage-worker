"""
일일 리포트 모듈

장 마감 후 거래 요약 생성
- 당일 거래 내역
- 손익 집계
- DB 저장
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from leverage_worker.data.database import Database
from leverage_worker.notification.slack_notifier import SlackNotifier
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TradeRecord:
    """거래 기록"""
    stock_code: str
    stock_name: str
    side: str  # "buy" or "sell"
    quantity: int
    price: int
    strategy_name: Optional[str]
    profit_loss: int = 0  # 매도 시만
    profit_rate: float = 0.0  # 매도 시만
    order_time: str = ""


@dataclass
class PositionRecord:
    """보유 포지션 기록"""
    stock_code: str
    stock_name: str
    quantity: int
    avg_price: float
    current_price: float
    profit_loss: int = 0  # 평가손익
    profit_loss_rate: float = 0.0  # 수익률
    strategy_name: Optional[str] = None


@dataclass
class DailyReport:
    """일일 리포트"""
    date: str
    total_trades: int = 0
    buy_trades: int = 0
    sell_trades: int = 0
    realized_pnl: int = 0
    win_trades: int = 0
    lose_trades: int = 0
    trades: List[TradeRecord] = field(default_factory=list)
    positions: List[PositionRecord] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        if self.sell_trades > 0:
            return (self.win_trades / self.sell_trades) * 100
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "total_trades": self.total_trades,
            "buy_trades": self.buy_trades,
            "sell_trades": self.sell_trades,
            "realized_pnl": self.realized_pnl,
            "win_trades": self.win_trades,
            "lose_trades": self.lose_trades,
            "win_rate": self.win_rate,
            "trades": [
                {
                    "stock_code": t.stock_code,
                    "stock_name": t.stock_name,
                    "side": t.side,
                    "quantity": t.quantity,
                    "price": t.price,
                    "strategy_name": t.strategy_name,
                    "profit_loss": t.profit_loss,
                    "profit_rate": t.profit_rate,
                    "order_time": t.order_time,
                }
                for t in self.trades
            ],
            "positions": [
                {
                    "stock_code": p.stock_code,
                    "stock_name": p.stock_name,
                    "quantity": p.quantity,
                    "avg_price": p.avg_price,
                    "current_price": p.current_price,
                    "profit_loss": p.profit_loss,
                    "profit_loss_rate": p.profit_loss_rate,
                    "strategy_name": p.strategy_name,
                }
                for p in self.positions
            ],
        }


class DailyReportGenerator:
    """
    일일 리포트 생성기

    - DB에서 당일 거래 내역 조회
    - 손익 집계
    - Slack 전송
    """

    def __init__(
        self,
        database: Database,
        slack_notifier: Optional[SlackNotifier] = None,
    ):
        self._db = database
        self._slack = slack_notifier

        logger.info("DailyReportGenerator initialized")

    def generate(self, date: Optional[str] = None) -> DailyReport:
        """
        일일 리포트 생성

        Args:
            date: 날짜 (YYYY-MM-DD). None이면 오늘.

        Returns:
            DailyReport 객체
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        report = DailyReport(date=date)

        # DB에서 당일 체결된 주문 조회 (부분 체결 후 취소된 주문도 포함)
        query = """
            SELECT * FROM orders
            WHERE DATE(created_at) = ? AND filled_quantity > 0
            ORDER BY created_at ASC
        """

        rows = self._db.fetch_all(query, (date,))

        for row in rows:
            side = row["side"]
            report.total_trades += 1

            trade = TradeRecord(
                stock_code=row["stock_code"],
                stock_name=row["stock_name"] or row["stock_code"],
                side=side,
                quantity=row["filled_quantity"],
                price=row["filled_price"],
                strategy_name=row["strategy_name"],
                order_time=row["created_at"],
            )

            if side == "buy":
                report.buy_trades += 1
            else:
                report.sell_trades += 1

                # 매도 손익 계산: DB에 저장된 값 우선 사용 (없으면 FIFO 계산)
                # sqlite3.Row는 .get() 메서드가 없으므로 keys() 체크
                row_keys = row.keys()
                if "pnl" in row_keys and row["pnl"] is not None:
                    pnl = row["pnl"]
                    avg_cost = row["avg_cost"] if "avg_cost" in row_keys and row["avg_cost"] else 0
                else:
                    # fallback: 기존 FIFO 계산 (마이그레이션 이전 데이터용)
                    pnl, avg_cost = self._calculate_trade_pnl(dict(row))

                trade.profit_loss = pnl
                if avg_cost > 0:
                    trade.profit_rate = ((row["filled_price"] - avg_cost) / avg_cost) * 100

                if pnl > 0:
                    report.win_trades += 1
                elif pnl < 0:
                    report.lose_trades += 1

                report.realized_pnl += pnl

            report.trades.append(trade)

        # 현재 보유 포지션 조회
        position_query = """
            SELECT * FROM positions
            WHERE quantity > 0
            ORDER BY stock_code ASC
        """
        position_rows = self._db.fetch_all(position_query)

        for row in position_rows:
            avg_price = row["avg_price"]
            current_price = row["current_price"] or avg_price
            quantity = row["quantity"]

            # 평가손익 계산
            profit_loss = int((current_price - avg_price) * quantity)
            profit_loss_rate = ((current_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0

            position = PositionRecord(
                stock_code=row["stock_code"],
                stock_name=row["stock_name"] or row["stock_code"],
                quantity=quantity,
                avg_price=avg_price,
                current_price=current_price,
                profit_loss=profit_loss,
                profit_loss_rate=profit_loss_rate,
                strategy_name=row["strategy_name"],
            )
            report.positions.append(position)

        logger.info(
            f"Report generated for {date}: "
            f"{report.total_trades} trades, "
            f"PnL: {report.realized_pnl:,}원, "
            f"Positions: {len(report.positions)}건"
        )

        return report

    def _calculate_trade_pnl(self, sell_order: dict) -> tuple[int, float]:
        """
        매도 거래의 손익 계산 (FIFO 방식)

        FIFO 기준으로 계산:
        - 매도 시점까지의 모든 매수 주문을 조회
        - 이전 매도로 청산된 물량을 FIFO로 차감
        - 남은 포지션의 평균 매입가 계산
        - (매도가 - 평균매입가) × 수량

        Returns:
            (손익(원), 평균매입가) 튜플
        """
        stock_code = sell_order["stock_code"]
        sell_price = sell_order["filled_price"]
        sell_qty = sell_order["filled_quantity"]
        sell_time = sell_order["created_at"]

        # 매도 시점 이전의 모든 매수 주문 조회 (시간순)
        query = """
            SELECT filled_price, filled_quantity, created_at FROM orders
            WHERE stock_code = ? AND side = 'buy' AND status = 'filled'
            AND created_at < ?
            ORDER BY created_at ASC
        """

        buy_orders = self._db.fetch_all(query, (stock_code, sell_time))

        if not buy_orders:
            return (0, 0.0)

        # 매도 시점 이전의 모든 매도 주문도 조회 (이미 청산된 물량 계산)
        sell_query = """
            SELECT filled_quantity FROM orders
            WHERE stock_code = ? AND side = 'sell' AND status = 'filled'
            AND created_at < ?
        """

        prev_sells = self._db.fetch_all(sell_query, (stock_code, sell_time))
        prev_sell_qty = sum(row["filled_quantity"] for row in prev_sells) if prev_sells else 0

        # FIFO 방식으로 이전 매도분 차감 후 남은 포지션의 평균가 계산
        remaining_to_skip = prev_sell_qty  # 이전 매도로 청산된 수량
        remaining_position = 0
        total_cost = 0

        for buy in buy_orders:
            buy_qty = buy["filled_quantity"]
            buy_price = buy["filled_price"]

            if remaining_to_skip > 0:
                # FIFO: 이전 매도로 청산된 물량 차감
                if buy_qty <= remaining_to_skip:
                    remaining_to_skip -= buy_qty
                    continue  # 이 매수는 전부 청산됨
                else:
                    # 일부만 청산됨
                    buy_qty -= remaining_to_skip
                    remaining_to_skip = 0

            # 남은 포지션에 추가
            remaining_position += buy_qty
            total_cost += buy_qty * buy_price

        # 평균 매입가 계산
        if remaining_position > 0:
            avg_cost = total_cost / remaining_position
            pnl = (sell_price - avg_cost) * sell_qty
            return (int(pnl), avg_cost)

        return (0, 0.0)

    def _calculate_trade_pnl_with_avg_cost(self, sell_order: dict, avg_cost: float) -> int:
        """
        평균 매입가가 제공된 경우의 손익 계산

        Args:
            sell_order: 매도 주문 정보
            avg_cost: 평균 매입가

        Returns:
            손익 (원)
        """
        sell_price = sell_order["filled_price"]
        sell_qty = sell_order["filled_quantity"]

        if avg_cost <= 0:
            return 0

        pnl = (sell_price - avg_cost) * sell_qty
        return int(pnl)

    def save_to_db(self, report: DailyReport) -> None:
        """리포트 DB 저장"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with self._db.get_cursor() as cursor:
            # UPSERT
            cursor.execute("""
                INSERT INTO daily_summary
                (trade_date, total_trades, buy_trades, sell_trades,
                 realized_pnl, win_trades, lose_trades, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date) DO UPDATE SET
                    total_trades = excluded.total_trades,
                    buy_trades = excluded.buy_trades,
                    sell_trades = excluded.sell_trades,
                    realized_pnl = excluded.realized_pnl,
                    win_trades = excluded.win_trades,
                    lose_trades = excluded.lose_trades
            """, (
                report.date,
                report.total_trades,
                report.buy_trades,
                report.sell_trades,
                report.realized_pnl,
                report.win_trades,
                report.lose_trades,
                now,
            ))

        logger.debug(f"Report saved to DB: {report.date}")

    def send_to_slack(self, report: DailyReport) -> bool:
        """리포트 Slack 전송"""
        if not self._slack or not self._slack.is_enabled:
            logger.debug("Slack not enabled, skipping report send")
            return False

        return self._slack.send_daily_report(report.to_dict())

    def generate_and_send(self, date: Optional[str] = None) -> DailyReport:
        """
        리포트 생성, 저장, 전송

        Args:
            date: 날짜 (YYYY-MM-DD). None이면 오늘.

        Returns:
            DailyReport 객체
        """
        report = self.generate(date)
        self.save_to_db(report)
        self.send_to_slack(report)
        return report

    def get_today_realized_pnl(self) -> int:
        """
        당일 실현손익 조회 (프로그램 재시작 시 복구용)

        Returns:
            당일 실현손익 합계 (원)
        """
        today = datetime.now().strftime("%Y-%m-%d")

        query = """
            SELECT * FROM orders
            WHERE DATE(created_at) = ? AND status = 'filled' AND side = 'sell'
            ORDER BY created_at ASC
        """

        rows = self._db.fetch_all(query, (today,))

        total_pnl = 0
        for row in rows:
            pnl, _ = self._calculate_trade_pnl(dict(row))
            total_pnl += pnl

        logger.debug(f"Today's realized PnL loaded from DB: {total_pnl:,}원")
        return total_pnl

    def get_monthly_summary(self, year: int, month: int) -> Dict[str, Any]:
        """
        월별 요약 조회

        Args:
            year: 연도
            month: 월

        Returns:
            월별 요약 딕셔너리
        """
        month_prefix = f"{year:04d}-{month:02d}"

        query = """
            SELECT
                COUNT(*) as days,
                SUM(total_trades) as total_trades,
                SUM(buy_trades) as buy_trades,
                SUM(sell_trades) as sell_trades,
                SUM(realized_pnl) as realized_pnl,
                SUM(win_trades) as win_trades,
                SUM(lose_trades) as lose_trades
            FROM daily_summary
            WHERE trade_date LIKE ?
        """

        row = self._db.fetch_one(query, (f"{month_prefix}%",))

        if row:
            return {
                "year": year,
                "month": month,
                "trading_days": row["days"] or 0,
                "total_trades": row["total_trades"] or 0,
                "buy_trades": row["buy_trades"] or 0,
                "sell_trades": row["sell_trades"] or 0,
                "realized_pnl": row["realized_pnl"] or 0,
                "win_trades": row["win_trades"] or 0,
                "lose_trades": row["lose_trades"] or 0,
            }

        return {
            "year": year,
            "month": month,
            "trading_days": 0,
            "total_trades": 0,
            "buy_trades": 0,
            "sell_trades": 0,
            "realized_pnl": 0,
            "win_trades": 0,
            "lose_trades": 0,
        }

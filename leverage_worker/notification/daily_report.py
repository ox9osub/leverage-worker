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

        # DB에서 당일 체결된 주문 조회
        query = """
            SELECT * FROM orders
            WHERE DATE(created_at) = ? AND status = 'filled'
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

                # 매도 손익 계산 (간단 버전: 직전 매수가와 비교)
                # 실제로는 포지션 히스토리를 봐야 정확
                pnl = self._calculate_trade_pnl(row)
                trade.profit_loss = pnl

                if pnl > 0:
                    report.win_trades += 1
                elif pnl < 0:
                    report.lose_trades += 1

                report.realized_pnl += pnl

            report.trades.append(trade)

        logger.info(
            f"Report generated for {date}: "
            f"{report.total_trades} trades, "
            f"PnL: {report.realized_pnl:,}원"
        )

        return report

    def _calculate_trade_pnl(self, sell_order: dict) -> int:
        """
        매도 거래의 손익 계산

        간단 버전: 같은 종목의 직전 매수 체결가와 비교
        """
        stock_code = sell_order["stock_code"]
        sell_price = sell_order["filled_price"]
        sell_qty = sell_order["filled_quantity"]
        sell_time = sell_order["created_at"]

        # 직전 매수 주문 찾기
        query = """
            SELECT filled_price FROM orders
            WHERE stock_code = ? AND side = 'buy' AND status = 'filled'
            AND created_at < ?
            ORDER BY created_at DESC
            LIMIT 1
        """

        row = self._db.fetch_one(query, (stock_code, sell_time))

        if row:
            buy_price = row["filled_price"]
            pnl = (sell_price - buy_price) * sell_qty
            return int(pnl)

        return 0

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

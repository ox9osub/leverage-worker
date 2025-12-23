"""
일봉 데이터 저장소 모듈

일봉 OHLCV 데이터 저장 및 조회
- 일별 시가, 고가, 저가, 종가, 거래량
- 수정종가, 등락률 지원
- 백테스팅용 기간 조회
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from leverage_worker.data.database import Database
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DailyCandle:
    """일봉 OHLCV 데이터"""

    stock_code: str  # 종목코드
    trade_date: str  # 거래일 (YYYYMMDD)
    open_price: float  # 시가
    high_price: float  # 고가
    low_price: float  # 저가
    close_price: float  # 종가
    volume: int  # 거래량
    trade_amount: Optional[int] = None  # 거래대금
    adj_close_price: Optional[float] = None  # 수정종가
    change_rate: Optional[float] = None  # 등락률 (%)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def date_formatted(self) -> str:
        """날짜 포맷 (YYYY-MM-DD)"""
        return f"{self.trade_date[:4]}-{self.trade_date[4:6]}-{self.trade_date[6:]}"

    @property
    def change_amount(self) -> Optional[float]:
        """전일 대비 변동액 (수정종가 기준)"""
        if self.change_rate is not None and self.close_price:
            prev_close = self.close_price / (1 + self.change_rate / 100)
            return self.close_price - prev_close
        return None


class DailyCandleRepository:
    """
    일봉 데이터 저장소

    - 일봉 OHLCV upsert
    - 기간별 조회 (백테스팅)
    - 최근 N일 조회 (전략용)
    """

    def __init__(self, database: Database):
        self._db = database
        logger.debug("DailyCandleRepository initialized")

    def upsert(
        self,
        stock_code: str,
        trade_date: str,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: int,
        trade_amount: Optional[int] = None,
        adj_close_price: Optional[float] = None,
        change_rate: Optional[float] = None,
    ) -> None:
        """
        일봉 데이터 upsert

        Args:
            stock_code: 종목코드
            trade_date: 거래일 (YYYYMMDD)
            open_price: 시가
            high_price: 고가
            low_price: 저가
            close_price: 종가
            volume: 거래량
            trade_amount: 거래대금
            adj_close_price: 수정종가
            change_rate: 등락률 (%)
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO daily_candles
                (stock_code, trade_date, open_price, high_price, low_price,
                 close_price, volume, trade_amount, adj_close_price, change_rate,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, trade_date) DO UPDATE SET
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volume = excluded.volume,
                trade_amount = excluded.trade_amount,
                adj_close_price = excluded.adj_close_price,
                change_rate = excluded.change_rate,
                updated_at = excluded.updated_at
        """

        params = (
            stock_code,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            trade_amount,
            adj_close_price,
            change_rate,
            now,
            now,
        )

        with self._db.get_cursor() as cursor:
            cursor.execute(query, params)

        logger.debug(f"Daily candle upsert: {stock_code} @ {trade_date}")

    def upsert_batch(self, candles: List[DailyCandle]) -> int:
        """
        일봉 데이터 일괄 upsert

        Args:
            candles: DailyCandle 리스트

        Returns:
            처리된 건수
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO daily_candles
                (stock_code, trade_date, open_price, high_price, low_price,
                 close_price, volume, trade_amount, adj_close_price, change_rate,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, trade_date) DO UPDATE SET
                open_price = excluded.open_price,
                high_price = excluded.high_price,
                low_price = excluded.low_price,
                close_price = excluded.close_price,
                volume = excluded.volume,
                trade_amount = excluded.trade_amount,
                adj_close_price = excluded.adj_close_price,
                change_rate = excluded.change_rate,
                updated_at = excluded.updated_at
        """

        params_list = [
            (
                c.stock_code,
                c.trade_date,
                c.open_price,
                c.high_price,
                c.low_price,
                c.close_price,
                c.volume,
                c.trade_amount,
                c.adj_close_price,
                c.change_rate,
                now,
                now,
            )
            for c in candles
        ]

        with self._db.get_cursor() as cursor:
            cursor.executemany(query, params_list)

        logger.info(f"Daily candle batch upsert: {len(candles)} records")
        return len(candles)

    def get(self, stock_code: str, trade_date: str) -> Optional[DailyCandle]:
        """
        특정 일봉 조회

        Args:
            stock_code: 종목코드
            trade_date: 거래일 (YYYYMMDD)

        Returns:
            DailyCandle 또는 None
        """
        query = """
            SELECT * FROM daily_candles
            WHERE stock_code = ? AND trade_date = ?
        """
        row = self._db.fetch_one(query, (stock_code, trade_date))

        if row:
            return self._row_to_candle(row)
        return None

    def get_range(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> List[DailyCandle]:
        """
        기간별 일봉 조회 (백테스팅용)

        Args:
            stock_code: 종목코드
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)

        Returns:
            DailyCandle 리스트 (날짜순)
        """
        query = """
            SELECT * FROM daily_candles
            WHERE stock_code = ? AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date ASC
        """
        rows = self._db.fetch_all(query, (stock_code, start_date, end_date))
        return [self._row_to_candle(row) for row in rows]

    def get_recent(
        self,
        stock_code: str,
        count: int = 20,
        before_date: Optional[str] = None,
    ) -> List[DailyCandle]:
        """
        최근 N일 일봉 조회 (전략용)

        Args:
            stock_code: 종목코드
            count: 조회 개수
            before_date: 이 날짜 이전 데이터만 (미포함)

        Returns:
            DailyCandle 리스트 (과거 → 최근 순)
        """
        if before_date:
            query = """
                SELECT * FROM daily_candles
                WHERE stock_code = ? AND trade_date < ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            params = (stock_code, before_date, count)
        else:
            query = """
                SELECT * FROM daily_candles
                WHERE stock_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            params = (stock_code, count)

        rows = self._db.fetch_all(query, params)

        # 시간순 정렬 (과거 → 최근)
        return [self._row_to_candle(row) for row in reversed(rows)]

    def get_latest(self, stock_code: str) -> Optional[DailyCandle]:
        """
        가장 최근 일봉 조회

        Args:
            stock_code: 종목코드

        Returns:
            DailyCandle 또는 None
        """
        query = """
            SELECT * FROM daily_candles
            WHERE stock_code = ?
            ORDER BY trade_date DESC
            LIMIT 1
        """
        row = self._db.fetch_one(query, (stock_code,))

        if row:
            return self._row_to_candle(row)
        return None

    def get_oldest(self, stock_code: str) -> Optional[DailyCandle]:
        """
        가장 오래된 일봉 조회

        Args:
            stock_code: 종목코드

        Returns:
            DailyCandle 또는 None
        """
        query = """
            SELECT * FROM daily_candles
            WHERE stock_code = ?
            ORDER BY trade_date ASC
            LIMIT 1
        """
        row = self._db.fetch_one(query, (stock_code,))

        if row:
            return self._row_to_candle(row)
        return None

    def get_date_range(self, stock_code: str) -> Optional[Tuple[str, str]]:
        """
        종목의 데이터 기간 조회

        Args:
            stock_code: 종목코드

        Returns:
            (시작일, 종료일) 튜플 또는 None
        """
        query = """
            SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date
            FROM daily_candles
            WHERE stock_code = ?
        """
        row = self._db.fetch_one(query, (stock_code,))

        if row and row["min_date"]:
            return (row["min_date"], row["max_date"])
        return None

    def get_count(self, stock_code: Optional[str] = None) -> int:
        """
        일봉 데이터 개수

        Args:
            stock_code: 종목코드 (None이면 전체)

        Returns:
            데이터 개수
        """
        if stock_code:
            query = "SELECT COUNT(*) as cnt FROM daily_candles WHERE stock_code = ?"
            row = self._db.fetch_one(query, (stock_code,))
        else:
            query = "SELECT COUNT(*) as cnt FROM daily_candles"
            row = self._db.fetch_one(query)

        return row["cnt"] if row else 0

    def get_stock_codes(self) -> List[str]:
        """
        저장된 종목코드 목록

        Returns:
            종목코드 리스트
        """
        query = "SELECT DISTINCT stock_code FROM daily_candles ORDER BY stock_code"
        rows = self._db.fetch_all(query)
        return [row["stock_code"] for row in rows]

    def get_available_dates(self, stock_code: str) -> List[str]:
        """
        종목의 거래일 목록

        Args:
            stock_code: 종목코드

        Returns:
            거래일 리스트 (YYYYMMDD)
        """
        query = """
            SELECT trade_date FROM daily_candles
            WHERE stock_code = ?
            ORDER BY trade_date ASC
        """
        rows = self._db.fetch_all(query, (stock_code,))
        return [row["trade_date"] for row in rows]

    def delete_range(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> int:
        """
        기간별 일봉 삭제

        Args:
            stock_code: 종목코드
            start_date: 시작일
            end_date: 종료일

        Returns:
            삭제된 행 수
        """
        query = """
            DELETE FROM daily_candles
            WHERE stock_code = ? AND trade_date BETWEEN ? AND ?
        """

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (stock_code, start_date, end_date))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted {deleted} daily candles: {stock_code} ({start_date}~{end_date})")

        return deleted

    def delete_stock(self, stock_code: str) -> int:
        """
        종목의 모든 일봉 삭제

        Args:
            stock_code: 종목코드

        Returns:
            삭제된 행 수
        """
        query = "DELETE FROM daily_candles WHERE stock_code = ?"

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (stock_code,))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted all {deleted} daily candles for {stock_code}")

        return deleted

    def _row_to_candle(self, row) -> DailyCandle:
        """DB 행을 DailyCandle 객체로 변환"""
        return DailyCandle(
            stock_code=row["stock_code"],
            trade_date=row["trade_date"],
            open_price=row["open_price"],
            high_price=row["high_price"],
            low_price=row["low_price"],
            close_price=row["close_price"],
            volume=row["volume"],
            trade_amount=row["trade_amount"],
            adj_close_price=row["adj_close_price"],
            change_rate=row["change_rate"],
            created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
            updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
        )

"""
분봉 데이터 저장소 모듈

분봉 OHLCV 데이터 저장 및 조회
- 1분봉 OHLCV 저장
- 실시간 시세 업데이트
- 전략용 과거 데이터 조회

기존 PriceRepository와 호환되는 인터페이스 제공
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from leverage_worker.data.database import Database
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


def _get_current_candle_datetime() -> str:
    """현재 시간의 분봉 datetime 문자열 반환 (YYYY-MM-DD HH:MM)"""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M")


def _get_current_trade_date() -> str:
    """현재 거래일 문자열 반환 (YYYYMMDD)"""
    return datetime.now().strftime("%Y%m%d")


def _candle_datetime_to_minute_key(candle_datetime: str) -> str:
    """candle_datetime을 minute_key로 변환 (호환성용)"""
    # "2025-01-23 09:30" -> "20250123_0930"
    dt = datetime.strptime(candle_datetime, "%Y-%m-%d %H:%M")
    return dt.strftime("%Y%m%d_%H%M")


def _minute_key_to_candle_datetime(minute_key: str) -> str:
    """minute_key를 candle_datetime으로 변환 (마이그레이션용)"""
    # "20250123_0930" -> "2025-01-23 09:30"
    date_part, time_part = minute_key.split("_")
    return f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]} {time_part[:2]}:{time_part[2:]}"


@dataclass
class MinuteCandle:
    """분봉 OHLCV 데이터"""

    stock_code: str  # 종목코드
    candle_datetime: str  # 봉 시작 시간 (YYYY-MM-DD HH:MM)
    trade_date: str  # 거래일 (YYYYMMDD)
    open_price: float  # 시가
    high_price: float  # 고가
    low_price: float  # 저가
    close_price: float  # 종가
    volume: int  # 거래량
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def minute_key(self) -> str:
        """기존 PriceRepository 호환 - minute_key (YYYYMMDD_HHMM)"""
        return _candle_datetime_to_minute_key(self.candle_datetime)

    @property
    def date_str(self) -> str:
        """날짜 문자열 (YYYYMMDD)"""
        return self.trade_date

    @property
    def time_str(self) -> str:
        """시간 문자열 (HH:MM)"""
        return self.candle_datetime.split(" ")[1]


# 기존 PriceRepository의 OHLCV와 호환되는 별칭
OHLCV = MinuteCandle


class MinuteCandleRepository:
    """
    분봉 데이터 저장소

    - 분봉 OHLCV upsert
    - 실시간 시세로부터 분봉 생성/업데이트
    - 과거 N개 분봉 조회 (전략용)
    - 당일/기간별 데이터 조회

    기존 PriceRepository와 호환되는 메서드 제공
    """

    def __init__(self, database: Database):
        self._db = database
        logger.debug("MinuteCandleRepository initialized")

    # ==========================================
    # 기존 PriceRepository 호환 메서드
    # ==========================================

    def upsert_price(
        self,
        stock_code: str,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: int,
        minute_key: Optional[str] = None,
    ) -> None:
        """
        [호환성] 가격 데이터 upsert (있으면 UPDATE, 없으면 INSERT)

        기존 PriceRepository.upsert_price()와 동일한 인터페이스

        Args:
            stock_code: 종목코드
            open_price: 시가
            high_price: 고가
            low_price: 저가
            close_price: 종가 (현재가)
            volume: 거래량
            minute_key: 분봉 키 (YYYYMMDD_HHMM, None이면 현재 분)
        """
        if minute_key:
            candle_datetime = _minute_key_to_candle_datetime(minute_key)
            trade_date = minute_key.split("_")[0]
        else:
            candle_datetime = _get_current_candle_datetime()
            trade_date = _get_current_trade_date()

        self.upsert(
            stock_code=stock_code,
            candle_datetime=candle_datetime,
            trade_date=trade_date,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
        )

    def upsert_from_api_response(
        self,
        stock_code: str,
        current_price: float,
        volume: int,
        minute_key: Optional[str] = None,
    ) -> None:
        """
        [호환성] API 응답으로부터 가격 upsert (실시간 현재가)

        기존 PriceRepository.upsert_from_api_response()와 동일한 인터페이스

        Args:
            stock_code: 종목코드
            current_price: 현재가
            volume: 누적 거래량
            minute_key: 분봉 키 (YYYYMMDD_HHMM)
        """
        if minute_key:
            candle_datetime = _minute_key_to_candle_datetime(minute_key)
            trade_date = minute_key.split("_")[0]
        else:
            candle_datetime = _get_current_candle_datetime()
            trade_date = _get_current_trade_date()

        self.upsert_from_realtime(
            stock_code=stock_code,
            current_price=current_price,
            volume=volume,
            candle_datetime=candle_datetime,
            trade_date=trade_date,
        )

    def get_price(self, stock_code: str, minute_key: str) -> Optional[MinuteCandle]:
        """
        [호환성] 특정 분봉 데이터 조회

        기존 PriceRepository.get_price()와 동일한 인터페이스

        Args:
            stock_code: 종목코드
            minute_key: 분봉 키 (YYYYMMDD_HHMM)

        Returns:
            MinuteCandle 객체 또는 None
        """
        candle_datetime = _minute_key_to_candle_datetime(minute_key)
        return self.get(stock_code, candle_datetime)

    def get_recent_prices(
        self,
        stock_code: str,
        count: int = 500,
        before_minute_key: Optional[str] = None,
    ) -> List[MinuteCandle]:
        """
        [호환성] 최근 N개 분봉 데이터 조회 (전략용)

        기존 PriceRepository.get_recent_prices()와 동일한 인터페이스

        Args:
            stock_code: 종목코드
            count: 조회 개수
            before_minute_key: 이 분봉 이전 데이터만 조회 (미포함)

        Returns:
            MinuteCandle 리스트 (시간순 정렬, 과거 → 최근)
        """
        if before_minute_key:
            before_datetime = _minute_key_to_candle_datetime(before_minute_key)
            return self.get_recent(stock_code, count, before_datetime=before_datetime)
        return self.get_recent(stock_code, count)

    def get_today_prices(self, stock_code: str) -> List[MinuteCandle]:
        """
        [호환성] 당일 전체 분봉 데이터 조회

        기존 PriceRepository.get_today_prices()와 동일한 인터페이스

        Args:
            stock_code: 종목코드

        Returns:
            MinuteCandle 리스트 (시간순 정렬)
        """
        return self.get_by_date(stock_code, _get_current_trade_date())

    def get_latest_price(self, stock_code: str) -> Optional[MinuteCandle]:
        """
        [호환성] 가장 최근 분봉 데이터 조회

        기존 PriceRepository.get_latest_price()와 동일한 인터페이스

        Args:
            stock_code: 종목코드

        Returns:
            MinuteCandle 객체 또는 None
        """
        return self.get_latest(stock_code)

    def get_price_count(self, stock_code: Optional[str] = None) -> int:
        """
        [호환성] 가격 데이터 개수 조회

        기존 PriceRepository.get_price_count()와 동일한 인터페이스
        """
        return self.get_count(stock_code)

    def get_stock_codes(self) -> List[str]:
        """
        [호환성] 저장된 종목코드 목록 조회

        기존 PriceRepository.get_stock_codes()와 동일한 인터페이스
        """
        return self.get_stored_stock_codes()

    # ==========================================
    # 신규 메서드 (새로운 스키마 기반)
    # ==========================================

    def upsert(
        self,
        stock_code: str,
        candle_datetime: str,
        trade_date: str,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: int,
    ) -> None:
        """
        분봉 데이터 upsert

        Args:
            stock_code: 종목코드
            candle_datetime: 봉 시작 시간 (YYYY-MM-DD HH:MM)
            trade_date: 거래일 (YYYYMMDD)
            open_price: 시가
            high_price: 고가
            low_price: 저가
            close_price: 종가
            volume: 거래량
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO minute_candles
                (stock_code, candle_datetime, trade_date, open_price, high_price,
                 low_price, close_price, volume, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, candle_datetime) DO UPDATE SET
                high_price = MAX(high_price, excluded.high_price),
                low_price = MIN(low_price, excluded.low_price),
                close_price = excluded.close_price,
                volume = excluded.volume,
                updated_at = excluded.updated_at
        """

        params = (
            stock_code,
            candle_datetime,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            now,
            now,
        )

        with self._db.get_cursor() as cursor:
            cursor.execute(query, params)

        logger.debug(
            f"Minute candle upsert: {stock_code} @ {candle_datetime} - "
            f"O:{open_price} H:{high_price} L:{low_price} C:{close_price} V:{volume}"
        )

    def upsert_from_realtime(
        self,
        stock_code: str,
        current_price: float,
        volume: int,
        candle_datetime: Optional[str] = None,
        trade_date: Optional[str] = None,
    ) -> None:
        """
        실시간 시세로부터 분봉 upsert

        현재 분봉의 경우:
        - 첫 조회: O=H=L=C=현재가
        - 이후 조회: H=max(H,현재가), L=min(L,현재가), C=현재가

        Args:
            stock_code: 종목코드
            current_price: 현재가
            volume: 누적 거래량
            candle_datetime: 봉 시작 시간 (None이면 현재)
            trade_date: 거래일 (None이면 오늘)
        """
        if candle_datetime is None:
            candle_datetime = _get_current_candle_datetime()
        if trade_date is None:
            trade_date = _get_current_trade_date()

        existing = self.get(stock_code, candle_datetime)

        if existing:
            self.upsert(
                stock_code=stock_code,
                candle_datetime=candle_datetime,
                trade_date=trade_date,
                open_price=existing.open_price,
                high_price=max(existing.high_price, current_price),
                low_price=min(existing.low_price, current_price),
                close_price=current_price,
                volume=volume,
            )
        else:
            self.upsert(
                stock_code=stock_code,
                candle_datetime=candle_datetime,
                trade_date=trade_date,
                open_price=current_price,
                high_price=current_price,
                low_price=current_price,
                close_price=current_price,
                volume=volume,
            )

    def upsert_batch(self, candles: List[MinuteCandle]) -> int:
        """
        분봉 데이터 일괄 upsert

        Args:
            candles: MinuteCandle 리스트

        Returns:
            처리된 건수
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO minute_candles
                (stock_code, candle_datetime, trade_date, open_price, high_price,
                 low_price, close_price, volume, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, candle_datetime) DO UPDATE SET
                high_price = MAX(high_price, excluded.high_price),
                low_price = MIN(low_price, excluded.low_price),
                close_price = excluded.close_price,
                volume = excluded.volume,
                updated_at = excluded.updated_at
        """

        params_list = [
            (
                c.stock_code,
                c.candle_datetime,
                c.trade_date,
                c.open_price,
                c.high_price,
                c.low_price,
                c.close_price,
                c.volume,
                now,
                now,
            )
            for c in candles
        ]

        with self._db.get_cursor() as cursor:
            cursor.executemany(query, params_list)

        logger.info(f"Minute candle batch upsert: {len(candles)} records")
        return len(candles)

    def get(self, stock_code: str, candle_datetime: str) -> Optional[MinuteCandle]:
        """
        특정 분봉 조회

        Args:
            stock_code: 종목코드
            candle_datetime: 봉 시작 시간 (YYYY-MM-DD HH:MM)

        Returns:
            MinuteCandle 또는 None
        """
        query = """
            SELECT * FROM minute_candles
            WHERE stock_code = ? AND candle_datetime = ?
        """
        row = self._db.fetch_one(query, (stock_code, candle_datetime))

        if row:
            return self._row_to_candle(row)
        return None

    def get_recent(
        self,
        stock_code: str,
        count: int = 60,
        before_datetime: Optional[str] = None,
    ) -> List[MinuteCandle]:
        """
        최근 N개 분봉 조회 (전략용)

        Args:
            stock_code: 종목코드
            count: 조회 개수
            before_datetime: 이 시간 이전 데이터만 (미포함)

        Returns:
            MinuteCandle 리스트 (과거 → 최근 순)
        """
        if before_datetime:
            query = """
                SELECT * FROM minute_candles
                WHERE stock_code = ? AND candle_datetime < ?
                ORDER BY candle_datetime DESC
                LIMIT ?
            """
            params = (stock_code, before_datetime, count)
        else:
            query = """
                SELECT * FROM minute_candles
                WHERE stock_code = ?
                ORDER BY candle_datetime DESC
                LIMIT ?
            """
            params = (stock_code, count)

        rows = self._db.fetch_all(query, params)
        return [self._row_to_candle(row) for row in reversed(rows)]

    def get_latest(self, stock_code: str) -> Optional[MinuteCandle]:
        """
        가장 최근 분봉 조회

        Args:
            stock_code: 종목코드

        Returns:
            MinuteCandle 또는 None
        """
        query = """
            SELECT * FROM minute_candles
            WHERE stock_code = ?
            ORDER BY candle_datetime DESC
            LIMIT 1
        """
        row = self._db.fetch_one(query, (stock_code,))

        if row:
            return self._row_to_candle(row)
        return None

    def get_by_date(self, stock_code: str, trade_date: str) -> List[MinuteCandle]:
        """
        특정 거래일의 전체 분봉 조회

        Args:
            stock_code: 종목코드
            trade_date: 거래일 (YYYYMMDD)

        Returns:
            MinuteCandle 리스트 (시간순)
        """
        query = """
            SELECT * FROM minute_candles
            WHERE stock_code = ? AND trade_date = ?
            ORDER BY candle_datetime ASC
        """
        rows = self._db.fetch_all(query, (stock_code, trade_date))
        return [self._row_to_candle(row) for row in rows]

    def get_range(
        self,
        stock_code: str,
        start_datetime: str,
        end_datetime: str,
    ) -> List[MinuteCandle]:
        """
        기간별 분봉 조회

        Args:
            stock_code: 종목코드
            start_datetime: 시작 시간 (YYYY-MM-DD HH:MM)
            end_datetime: 종료 시간 (YYYY-MM-DD HH:MM)

        Returns:
            MinuteCandle 리스트 (시간순)
        """
        query = """
            SELECT * FROM minute_candles
            WHERE stock_code = ? AND candle_datetime BETWEEN ? AND ?
            ORDER BY candle_datetime ASC
        """
        rows = self._db.fetch_all(query, (stock_code, start_datetime, end_datetime))
        return [self._row_to_candle(row) for row in rows]

    def get_count(self, stock_code: Optional[str] = None) -> int:
        """
        분봉 데이터 개수

        Args:
            stock_code: 종목코드 (None이면 전체)

        Returns:
            데이터 개수
        """
        if stock_code:
            query = "SELECT COUNT(*) as cnt FROM minute_candles WHERE stock_code = ?"
            row = self._db.fetch_one(query, (stock_code,))
        else:
            query = "SELECT COUNT(*) as cnt FROM minute_candles"
            row = self._db.fetch_one(query)

        return row["cnt"] if row else 0

    def get_stored_stock_codes(self) -> List[str]:
        """
        저장된 종목코드 목록

        Returns:
            종목코드 리스트
        """
        query = "SELECT DISTINCT stock_code FROM minute_candles ORDER BY stock_code"
        rows = self._db.fetch_all(query)
        return [row["stock_code"] for row in rows]

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
            FROM minute_candles
            WHERE stock_code = ?
        """
        row = self._db.fetch_one(query, (stock_code,))

        if row and row["min_date"]:
            return (row["min_date"], row["max_date"])
        return None

    def delete_by_date(self, stock_code: str, trade_date: str) -> int:
        """
        특정 거래일의 분봉 삭제

        Args:
            stock_code: 종목코드
            trade_date: 거래일

        Returns:
            삭제된 행 수
        """
        query = """
            DELETE FROM minute_candles
            WHERE stock_code = ? AND trade_date = ?
        """

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (stock_code, trade_date))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted {deleted} minute candles: {stock_code} @ {trade_date}")

        return deleted

    def delete_old_data(self, days_to_keep: int = 30) -> int:
        """
        오래된 데이터 삭제

        Args:
            days_to_keep: 보관할 일수

        Returns:
            삭제된 행 수
        """
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y%m%d")

        query = """
            DELETE FROM minute_candles
            WHERE trade_date < ?
        """

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (cutoff_date,))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted {deleted} old minute candle records (before {cutoff_date})")

        return deleted

    def delete_stock(self, stock_code: str) -> int:
        """
        종목의 모든 분봉 삭제

        Args:
            stock_code: 종목코드

        Returns:
            삭제된 행 수
        """
        query = "DELETE FROM minute_candles WHERE stock_code = ?"

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (stock_code,))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted all {deleted} minute candles for {stock_code}")

        return deleted

    def _row_to_candle(self, row) -> MinuteCandle:
        """DB 행을 MinuteCandle 객체로 변환"""
        return MinuteCandle(
            stock_code=row["stock_code"],
            candle_datetime=row["candle_datetime"],
            trade_date=row["trade_date"],
            open_price=row["open_price"],
            high_price=row["high_price"],
            low_price=row["low_price"],
            close_price=row["close_price"],
            volume=row["volume"],
            created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
            updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
        )


# 기존 PriceRepository 호환 별칭
PriceRepository = MinuteCandleRepository

"""
가격 저장소 모듈 (DEPRECATED)

이 모듈은 하위 호환성을 위해 유지됩니다.
새로운 코드에서는 minute_candle_repository를 사용하세요.

>>> from leverage_worker.data.minute_candle_repository import MinuteCandleRepository
>>> # 또는 호환성 별칭 사용
>>> from leverage_worker.data.minute_candle_repository import PriceRepository
"""

import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from leverage_worker.data.database import Database
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.time_utils import get_current_minute_key

logger = get_logger(__name__)

# Deprecation 경고
warnings.warn(
    "price_repository 모듈은 deprecated입니다. "
    "minute_candle_repository.MinuteCandleRepository를 사용하세요.",
    DeprecationWarning,
    stacklevel=2,
)


@dataclass
class OHLCV:
    """분봉 OHLCV 데이터"""
    stock_code: str
    minute_key: str  # YYYYMMDD_HHMM
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def date_str(self) -> str:
        """날짜 문자열 (YYYYMMDD)"""
        return self.minute_key.split("_")[0]

    @property
    def time_str(self) -> str:
        """시간 문자열 (HHMM)"""
        return self.minute_key.split("_")[1]


class PriceRepository:
    """
    가격 데이터 저장소

    - 분봉 OHLCV upsert
    - 과거 N개 분봉 조회
    - 당일 데이터 조회
    """

    def __init__(self, database: Database):
        self._db = database
        logger.debug("PriceRepository initialized")

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
        가격 데이터 upsert (있으면 UPDATE, 없으면 INSERT)

        Args:
            stock_code: 종목코드
            open_price: 시가
            high_price: 고가
            low_price: 저가
            close_price: 종가 (현재가)
            volume: 거래량
            minute_key: 분봉 키 (None이면 현재 분)
        """
        if minute_key is None:
            minute_key = get_current_minute_key(datetime.now())

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # SQLite UPSERT 구문
        query = """
            INSERT INTO price_history
                (stock_code, minute_key, open_price, high_price, low_price,
                 close_price, volume, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, minute_key) DO UPDATE SET
                high_price = MAX(high_price, excluded.high_price),
                low_price = MIN(low_price, excluded.low_price),
                close_price = excluded.close_price,
                volume = excluded.volume,
                updated_at = excluded.updated_at
        """

        params = (
            stock_code,
            minute_key,
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
            f"Price upsert: {stock_code} @ {minute_key} - "
            f"O:{open_price} H:{high_price} L:{low_price} C:{close_price} V:{volume}"
        )

    def upsert_from_api_response(
        self,
        stock_code: str,
        current_price: float,
        volume: int,
        minute_key: Optional[str] = None,
    ) -> None:
        """
        API 응답으로부터 가격 upsert (실시간 현재가)

        현재 분봉의 경우:
        - 첫 조회: O=H=L=C=현재가
        - 이후 조회: H=max(H,현재가), L=min(L,현재가), C=현재가

        Args:
            stock_code: 종목코드
            current_price: 현재가
            volume: 누적 거래량
            minute_key: 분봉 키
        """
        if minute_key is None:
            minute_key = get_current_minute_key(datetime.now())

        # 기존 데이터 조회
        existing = self.get_price(stock_code, minute_key)

        if existing:
            # UPDATE: 고가/저가 갱신, 종가 업데이트
            self.upsert_price(
                stock_code=stock_code,
                open_price=existing.open_price,
                high_price=max(existing.high_price, current_price),
                low_price=min(existing.low_price, current_price),
                close_price=current_price,
                volume=volume,
                minute_key=minute_key,
            )
        else:
            # INSERT: 모든 가격을 현재가로
            self.upsert_price(
                stock_code=stock_code,
                open_price=current_price,
                high_price=current_price,
                low_price=current_price,
                close_price=current_price,
                volume=volume,
                minute_key=minute_key,
            )

    def get_price(self, stock_code: str, minute_key: str) -> Optional[OHLCV]:
        """
        특정 분봉 데이터 조회

        Args:
            stock_code: 종목코드
            minute_key: 분봉 키

        Returns:
            OHLCV 객체 또는 None
        """
        query = """
            SELECT * FROM price_history
            WHERE stock_code = ? AND minute_key = ?
        """

        row = self._db.fetch_one(query, (stock_code, minute_key))

        if row:
            return OHLCV(
                stock_code=row["stock_code"],
                minute_key=row["minute_key"],
                open_price=row["open_price"],
                high_price=row["high_price"],
                low_price=row["low_price"],
                close_price=row["close_price"],
                volume=row["volume"],
                created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
            )
        return None

    def get_recent_prices(
        self,
        stock_code: str,
        count: int = 60,
        before_minute_key: Optional[str] = None,
    ) -> List[OHLCV]:
        """
        최근 N개 분봉 데이터 조회 (전략용)

        Args:
            stock_code: 종목코드
            count: 조회 개수
            before_minute_key: 이 분봉 이전 데이터만 조회 (미포함)

        Returns:
            OHLCV 리스트 (시간순 정렬, 과거 → 최근)
        """
        if before_minute_key:
            query = """
                SELECT * FROM price_history
                WHERE stock_code = ? AND minute_key < ?
                ORDER BY minute_key DESC
                LIMIT ?
            """
            params = (stock_code, before_minute_key, count)
        else:
            query = """
                SELECT * FROM price_history
                WHERE stock_code = ?
                ORDER BY minute_key DESC
                LIMIT ?
            """
            params = (stock_code, count)

        rows = self._db.fetch_all(query, params)

        # 시간순 정렬 (과거 → 최근)
        results = []
        for row in reversed(rows):
            results.append(
                OHLCV(
                    stock_code=row["stock_code"],
                    minute_key=row["minute_key"],
                    open_price=row["open_price"],
                    high_price=row["high_price"],
                    low_price=row["low_price"],
                    close_price=row["close_price"],
                    volume=row["volume"],
                    created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
                    updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
                )
            )

        return results

    def get_today_prices(self, stock_code: str) -> List[OHLCV]:
        """
        당일 전체 분봉 데이터 조회

        Args:
            stock_code: 종목코드

        Returns:
            OHLCV 리스트 (시간순 정렬)
        """
        today = datetime.now().strftime("%Y%m%d")
        minute_key_prefix = f"{today}_"

        query = """
            SELECT * FROM price_history
            WHERE stock_code = ? AND minute_key LIKE ?
            ORDER BY minute_key ASC
        """

        rows = self._db.fetch_all(query, (stock_code, f"{minute_key_prefix}%"))

        results = []
        for row in rows:
            results.append(
                OHLCV(
                    stock_code=row["stock_code"],
                    minute_key=row["minute_key"],
                    open_price=row["open_price"],
                    high_price=row["high_price"],
                    low_price=row["low_price"],
                    close_price=row["close_price"],
                    volume=row["volume"],
                    created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
                    updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
                )
            )

        return results

    def get_latest_price(self, stock_code: str) -> Optional[OHLCV]:
        """
        가장 최근 분봉 데이터 조회

        Args:
            stock_code: 종목코드

        Returns:
            OHLCV 객체 또는 None
        """
        query = """
            SELECT * FROM price_history
            WHERE stock_code = ?
            ORDER BY minute_key DESC
            LIMIT 1
        """

        row = self._db.fetch_one(query, (stock_code,))

        if row:
            return OHLCV(
                stock_code=row["stock_code"],
                minute_key=row["minute_key"],
                open_price=row["open_price"],
                high_price=row["high_price"],
                low_price=row["low_price"],
                close_price=row["close_price"],
                volume=row["volume"],
                created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
                updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
            )
        return None

    def delete_old_data(self, days_to_keep: int = 30) -> int:
        """
        오래된 데이터 삭제

        Args:
            days_to_keep: 보관할 일수

        Returns:
            삭제된 행 수
        """
        from datetime import timedelta

        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y%m%d")
        cutoff_key = f"{cutoff_date}_0000"

        query = """
            DELETE FROM price_history
            WHERE minute_key < ?
        """

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (cutoff_key,))
            deleted = cursor.rowcount

        if deleted > 0:
            logger.info(f"Deleted {deleted} old price records (before {cutoff_date})")

        return deleted

    def get_price_count(self, stock_code: Optional[str] = None) -> int:
        """
        가격 데이터 개수 조회

        Args:
            stock_code: 종목코드 (None이면 전체)

        Returns:
            데이터 개수
        """
        if stock_code:
            query = "SELECT COUNT(*) as cnt FROM price_history WHERE stock_code = ?"
            row = self._db.fetch_one(query, (stock_code,))
        else:
            query = "SELECT COUNT(*) as cnt FROM price_history"
            row = self._db.fetch_one(query)

        return row["cnt"] if row else 0

    def get_stock_codes(self) -> List[str]:
        """
        저장된 종목코드 목록 조회

        Returns:
            종목코드 리스트
        """
        query = "SELECT DISTINCT stock_code FROM price_history ORDER BY stock_code"
        rows = self._db.fetch_all(query)
        return [row["stock_code"] for row in rows]

"""
종목 마스터 저장소 모듈

종목 기본 정보 저장 및 조회
- 종목코드, 종목명, 시장구분, 업종
- 활성 상태 관리
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from leverage_worker.data.database import Database
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Stock:
    """종목 정보"""

    stock_code: str  # 종목코드 (6자리)
    stock_name: str  # 종목명
    market: str  # 시장 (KOSPI/KOSDAQ)
    sector: Optional[str] = None  # 업종
    is_active: bool = True  # 활성 여부
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StockRepository:
    """
    종목 마스터 저장소

    - 종목 정보 upsert
    - 종목 조회 (코드, 시장, 업종별)
    - 활성 상태 관리
    """

    def __init__(self, database: Database):
        self._db = database
        logger.debug("StockRepository initialized")

    def upsert(
        self,
        stock_code: str,
        stock_name: str,
        market: str,
        sector: Optional[str] = None,
        is_active: bool = True,
    ) -> None:
        """
        종목 정보 upsert (있으면 UPDATE, 없으면 INSERT)

        Args:
            stock_code: 종목코드
            stock_name: 종목명
            market: 시장 (KOSPI/KOSDAQ)
            sector: 업종
            is_active: 활성 여부
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO stocks
                (stock_code, stock_name, market, sector, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                market = excluded.market,
                sector = excluded.sector,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
        """

        params = (
            stock_code,
            stock_name,
            market,
            sector,
            1 if is_active else 0,
            now,
            now,
        )

        with self._db.get_cursor() as cursor:
            cursor.execute(query, params)

        logger.debug(f"Stock upsert: {stock_code} ({stock_name}) - {market}")

    def upsert_batch(self, stocks: List[Stock]) -> int:
        """
        종목 정보 일괄 upsert

        Args:
            stocks: Stock 객체 리스트

        Returns:
            처리된 종목 수
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            INSERT INTO stocks
                (stock_code, stock_name, market, sector, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                market = excluded.market,
                sector = excluded.sector,
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
        """

        params_list = [
            (
                s.stock_code,
                s.stock_name,
                s.market,
                s.sector,
                1 if s.is_active else 0,
                now,
                now,
            )
            for s in stocks
        ]

        with self._db.get_cursor() as cursor:
            cursor.executemany(query, params_list)

        logger.info(f"Stock batch upsert: {len(stocks)} stocks")
        return len(stocks)

    def get(self, stock_code: str) -> Optional[Stock]:
        """
        종목 정보 조회

        Args:
            stock_code: 종목코드

        Returns:
            Stock 객체 또는 None
        """
        query = "SELECT * FROM stocks WHERE stock_code = ?"
        row = self._db.fetch_one(query, (stock_code,))

        if row:
            return self._row_to_stock(row)
        return None

    def get_by_name(self, stock_name: str) -> Optional[Stock]:
        """
        종목명으로 조회

        Args:
            stock_name: 종목명

        Returns:
            Stock 객체 또는 None
        """
        query = "SELECT * FROM stocks WHERE stock_name = ?"
        row = self._db.fetch_one(query, (stock_name,))

        if row:
            return self._row_to_stock(row)
        return None

    def search_by_name(self, keyword: str, limit: int = 20) -> List[Stock]:
        """
        종목명 검색 (부분 일치)

        Args:
            keyword: 검색 키워드
            limit: 최대 결과 수

        Returns:
            Stock 리스트
        """
        query = """
            SELECT * FROM stocks
            WHERE stock_name LIKE ?
            ORDER BY stock_name
            LIMIT ?
        """
        rows = self._db.fetch_all(query, (f"%{keyword}%", limit))
        return [self._row_to_stock(row) for row in rows]

    def get_all(self, active_only: bool = True) -> List[Stock]:
        """
        전체 종목 조회

        Args:
            active_only: 활성 종목만 조회

        Returns:
            Stock 리스트
        """
        if active_only:
            query = "SELECT * FROM stocks WHERE is_active = 1 ORDER BY stock_code"
        else:
            query = "SELECT * FROM stocks ORDER BY stock_code"

        rows = self._db.fetch_all(query)
        return [self._row_to_stock(row) for row in rows]

    def get_by_market(self, market: str, active_only: bool = True) -> List[Stock]:
        """
        시장별 종목 조회

        Args:
            market: 시장 (KOSPI/KOSDAQ)
            active_only: 활성 종목만 조회

        Returns:
            Stock 리스트
        """
        if active_only:
            query = """
                SELECT * FROM stocks
                WHERE market = ? AND is_active = 1
                ORDER BY stock_code
            """
        else:
            query = """
                SELECT * FROM stocks
                WHERE market = ?
                ORDER BY stock_code
            """

        rows = self._db.fetch_all(query, (market,))
        return [self._row_to_stock(row) for row in rows]

    def get_by_sector(self, sector: str, active_only: bool = True) -> List[Stock]:
        """
        업종별 종목 조회

        Args:
            sector: 업종
            active_only: 활성 종목만 조회

        Returns:
            Stock 리스트
        """
        if active_only:
            query = """
                SELECT * FROM stocks
                WHERE sector = ? AND is_active = 1
                ORDER BY stock_code
            """
        else:
            query = """
                SELECT * FROM stocks
                WHERE sector = ?
                ORDER BY stock_code
            """

        rows = self._db.fetch_all(query, (sector,))
        return [self._row_to_stock(row) for row in rows]

    def set_active(self, stock_code: str, is_active: bool) -> bool:
        """
        종목 활성 상태 변경

        Args:
            stock_code: 종목코드
            is_active: 활성 여부

        Returns:
            성공 여부
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
            UPDATE stocks
            SET is_active = ?, updated_at = ?
            WHERE stock_code = ?
        """

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (1 if is_active else 0, now, stock_code))
            updated = cursor.rowcount > 0

        if updated:
            status = "active" if is_active else "inactive"
            logger.info(f"Stock {stock_code} set to {status}")

        return updated

    def delete(self, stock_code: str) -> bool:
        """
        종목 삭제

        Args:
            stock_code: 종목코드

        Returns:
            삭제 성공 여부
        """
        query = "DELETE FROM stocks WHERE stock_code = ?"

        with self._db.get_cursor() as cursor:
            cursor.execute(query, (stock_code,))
            deleted = cursor.rowcount > 0

        if deleted:
            logger.info(f"Stock deleted: {stock_code}")

        return deleted

    def get_count(self, active_only: bool = True) -> int:
        """
        종목 수 조회

        Args:
            active_only: 활성 종목만

        Returns:
            종목 수
        """
        if active_only:
            query = "SELECT COUNT(*) as cnt FROM stocks WHERE is_active = 1"
        else:
            query = "SELECT COUNT(*) as cnt FROM stocks"

        row = self._db.fetch_one(query)
        return row["cnt"] if row else 0

    def get_markets(self) -> List[str]:
        """
        시장 목록 조회

        Returns:
            시장 리스트
        """
        query = "SELECT DISTINCT market FROM stocks ORDER BY market"
        rows = self._db.fetch_all(query)
        return [row["market"] for row in rows]

    def get_sectors(self) -> List[str]:
        """
        업종 목록 조회

        Returns:
            업종 리스트
        """
        query = "SELECT DISTINCT sector FROM stocks WHERE sector IS NOT NULL ORDER BY sector"
        rows = self._db.fetch_all(query)
        return [row["sector"] for row in rows]

    def _row_to_stock(self, row) -> Stock:
        """DB 행을 Stock 객체로 변환"""
        return Stock(
            stock_code=row["stock_code"],
            stock_name=row["stock_name"],
            market=row["market"],
            sector=row["sector"],
            is_active=bool(row["is_active"]),
            created_at=datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S"),
            updated_at=datetime.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S"),
        )

"""
데이터베이스 모듈

SQLite 데이터베이스 연결 및 테이블 관리
- 종목 마스터 (stocks)
- 일봉 데이터 (daily_candles)
- 분봉 데이터 (minute_candles)
- 주문 기록 (orders)
- 포지션 (positions)
- 일일 거래 요약 (daily_summary)
"""

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Generator

from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


class Database:
    """
    SQLite 데이터베이스 관리 클래스

    - 연결 풀링 (스레드별 연결)
    - 테이블 자동 생성
    - 트랜잭션 관리
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        Args:
            db_path: DB 파일 경로. None이면 기본 경로 사용
        """
        if db_path is None:
            # 기본 경로: leverage_worker/data/trading.db
            self._db_path = Path(__file__).parent / "trading.db"
        else:
            self._db_path = db_path

        # 스레드별 연결 저장
        self._local = threading.local()
        self._lock = threading.Lock()

        # DB 디렉토리 생성
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # 테이블 초기화
        self._init_tables()

        logger.info(f"Database initialized: {self._db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """현재 스레드의 DB 연결 반환 (없으면 생성)"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                str(self._db_path),
                check_same_thread=False,
                timeout=30.0,
            )
            # Row를 dict처럼 접근 가능하게
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection

    @contextmanager
    def get_cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        """커서 컨텍스트 매니저 (자동 커밋/롤백)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Cursor, None, None]:
        """명시적 트랜잭션 (여러 쿼리를 하나의 트랜잭션으로)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN")
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction error: {e}")
            raise
        finally:
            cursor.close()

    def _init_tables(self) -> None:
        """테이블 생성"""
        with self.get_cursor() as cursor:
            # ========================================
            # 종목 마스터 테이블
            # ========================================
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL UNIQUE,
                    stock_name TEXT NOT NULL,
                    market TEXT NOT NULL,
                    sector TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stocks_market
                ON stocks(market)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stocks_active
                ON stocks(is_active)
            """)

            # ========================================
            # 일봉 데이터 테이블
            # ========================================
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    trade_amount INTEGER,
                    adj_close_price REAL,
                    change_rate REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(stock_code, trade_date)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_stock
                ON daily_candles(stock_code)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_date
                ON daily_candles(trade_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_stock_date
                ON daily_candles(stock_code, trade_date)
            """)

            # ========================================
            # 분봉 데이터 테이블
            # ========================================
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_candles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    candle_datetime TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(stock_code, candle_datetime)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_minute_stock
                ON minute_candles(stock_code)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_minute_date
                ON minute_candles(trade_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_minute_stock_date
                ON minute_candles(stock_code, trade_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_minute_stock_datetime
                ON minute_candles(stock_code, candle_datetime)
            """)

            # ========================================
            # [DEPRECATED] 기존 분봉 가격 테이블 (마이그레이션 후 제거 예정)
            # ========================================
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL,
                    minute_key TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(stock_code, minute_key)
                )
            """)

            # 가격 조회 인덱스
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_stock_minute
                ON price_history(stock_code, minute_key)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_stock_time
                ON price_history(stock_code, created_at)
            """)

            # 주문 기록 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL UNIQUE,
                    stock_code TEXT NOT NULL,
                    stock_name TEXT,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL,
                    filled_quantity INTEGER DEFAULT 0,
                    filled_price REAL,
                    status TEXT NOT NULL,
                    strategy_name TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # 주문 조회 인덱스
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_stock
                ON orders(stock_code)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_status
                ON orders(status)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_orders_date
                ON orders(created_at)
            """)

            # 포지션 테이블 (현재 보유)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT NOT NULL UNIQUE,
                    stock_name TEXT,
                    quantity INTEGER NOT NULL,
                    avg_price REAL NOT NULL,
                    current_price REAL,
                    strategy_name TEXT,
                    entry_order_id TEXT,
                    entry_time TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # 일일 거래 요약 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date TEXT NOT NULL UNIQUE,
                    total_trades INTEGER DEFAULT 0,
                    buy_trades INTEGER DEFAULT 0,
                    sell_trades INTEGER DEFAULT 0,
                    realized_pnl REAL DEFAULT 0,
                    win_trades INTEGER DEFAULT 0,
                    lose_trades INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                )
            """)

        logger.debug("Database tables initialized")

    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """단일 쿼리 실행"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor

    def execute_many(self, query: str, params_list: list) -> None:
        """다중 쿼리 실행"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)

    def fetch_one(self, query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """단일 행 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()

    def fetch_all(self, query: str, params: tuple = ()) -> list:
        """전체 행 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def close(self) -> None:
        """현재 스레드의 연결 종료"""
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
            logger.debug("Database connection closed")

    def close_all(self) -> None:
        """모든 연결 종료 (메인 스레드에서 호출)"""
        self.close()
        logger.info("All database connections closed")

    def vacuum(self) -> None:
        """DB 최적화 (압축)"""
        with self.get_cursor() as cursor:
            cursor.execute("VACUUM")
        logger.info("Database vacuumed")

    def get_table_stats(self) -> dict:
        """테이블별 통계 조회"""
        stats = {}
        tables = [
            "stocks",
            "daily_candles",
            "minute_candles",
            "price_history",  # deprecated
            "orders",
            "positions",
            "daily_summary",
        ]

        for table in tables:
            try:
                row = self.fetch_one(f"SELECT COUNT(*) as cnt FROM {table}")
                stats[table] = row["cnt"] if row else 0
            except Exception:
                stats[table] = 0

        return stats

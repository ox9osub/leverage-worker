"""
Data 모듈 - 데이터베이스, 가격/시세 저장소

주요 클래스:
- Database: SQLite 데이터베이스 베이스 클래스
- MarketDataDB: 시세 DB (모의/실전 공유)
- TradingDB: 매매 DB (모의/실전 분리)
- StockRepository: 종목 마스터 관리
- DailyCandleRepository: 일봉 데이터 관리
- MinuteCandleRepository: 분봉 데이터 관리 (기존 PriceRepository 대체)
"""

from leverage_worker.data.database import Database, MarketDataDB, TradingDB
from leverage_worker.data.stock_repository import Stock, StockRepository
from leverage_worker.data.daily_candle_repository import DailyCandle, DailyCandleRepository
from leverage_worker.data.minute_candle_repository import (
    MinuteCandle,
    MinuteCandleRepository,
    OHLCV,  # 호환성 별칭
    PriceRepository,  # 호환성 별칭
)

__all__ = [
    # Database
    "Database",
    "MarketDataDB",
    "TradingDB",
    # Stock
    "Stock",
    "StockRepository",
    # Daily Candle
    "DailyCandle",
    "DailyCandleRepository",
    # Minute Candle
    "MinuteCandle",
    "MinuteCandleRepository",
    # 호환성 별칭
    "OHLCV",
    "PriceRepository",
]

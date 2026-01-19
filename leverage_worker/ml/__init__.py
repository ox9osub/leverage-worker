# -*- coding: utf-8 -*-
"""
ML 모듈

변동성 예측 + 방향 판단 기반 트레이딩 신호 생성
kis-trader에서 이식

주요 컴포넌트:
- TradingFeatureEngineer: 150+ 피처 엔지니어링
- VolatilityDirectionSignalGenerator: 실시간 신호 생성
- SignalConfig: 신호 생성 설정
"""

from leverage_worker.ml.config import SignalConfig
from leverage_worker.ml.features import TradingFeatureEngineer, FeatureConfig
from leverage_worker.ml.signal_generator import (
    VolatilityDirectionSignalGenerator,
    create_signal_generator,
)
from leverage_worker.ml.data_utils import (
    candles_to_dataframe,
    get_daily_high_low,
    filter_today_candles,
)

__all__ = [
    # Config
    "SignalConfig",
    "FeatureConfig",
    # Features
    "TradingFeatureEngineer",
    # Signal Generator
    "VolatilityDirectionSignalGenerator",
    "create_signal_generator",
    # Data Utils
    "candles_to_dataframe",
    "get_daily_high_low",
    "filter_today_candles",
]

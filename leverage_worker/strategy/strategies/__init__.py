"""
전략 구현체들

이 모듈을 import하면 모든 전략이 자동으로 StrategyRegistry에 등록됩니다.

KODEX 레버리지 (122630) 전략:
    - hybrid_momentum: 하이브리드 모멘텀 전략 (S_Hybrid_Mom10_MR15)
    - breakout_high5: 5일 신고가 돌파 전략 (A_Breakout_High5)
    - bollinger_band: 볼린저 밴드 전략 (C_BB_P15)
    - fibonacci_lucky: 피보나치 5일 전략 (E_Fib5_Lucky7)
    - fee_optimized: 수수료 최적화 전략 (B_FeeOpt)

ML 기반 전략 (122630):
    - ml_price_position: ML 가격 위치 전략 (변동성 예측 + 가격 위치)
    - ml_momentum: ML 모멘텀 전략 (변동성 예측 + 모멘텀)
    - main_beam_1: ML 지정가 매수 전략 (90_1, 앙상블 모델)

KODEX 코스닥150레버리지 (233740) 전략:
    - kosdaq_bb_conservative: 볼린저 밴드 보수적 전략 (BB_Conservative)
    - kosdaq_donchian: 돈치안 채널 전략 (Donchian_Scientific)
    - kosdaq_mdd_target: MDD 타겟 전략 (MDD_Target)

기본 예시 전략:
    - example_strategy: 이동평균 크로스 전략
    - simple_momentum: 단순 모멘텀 전략
"""

# 기존 예시 전략
from leverage_worker.strategy.strategies.example_strategy import (
    ExampleStrategy,
    SimpleMomentumStrategy,
)

# KODEX 레버리지 (122630) 전략
from leverage_worker.strategy.strategies.hybrid_momentum import HybridMomentumStrategy
from leverage_worker.strategy.strategies.breakout_high import BreakoutHigh5Strategy
from leverage_worker.strategy.strategies.bollinger_band import BollingerBandStrategy
from leverage_worker.strategy.strategies.fibonacci_lucky import FibonacciLuckyStrategy
from leverage_worker.strategy.strategies.fee_optimized import FeeOptimizedStrategy

# ML 기반 전략 (122630)
from leverage_worker.strategy.strategies.ml_price_position import MLPricePositionStrategy
from leverage_worker.strategy.strategies.ml_momentum import MLMomentumStrategy
from leverage_worker.strategy.strategies.main_beam_1 import MainBeam1Strategy

# KODEX 코스닥150레버리지 (233740) 전략
from leverage_worker.strategy.strategies.kosdaq_bb_conservative import (
    KosdaqBBConservativeStrategy,
)
from leverage_worker.strategy.strategies.kosdaq_donchian import KosdaqDonchianStrategy
from leverage_worker.strategy.strategies.kosdaq_mdd_target import KosdaqMDDTargetStrategy

# WebSocket 실시간 전략
from leverage_worker.strategy.strategies.dip_buy import DipBuyStrategy

# 스캘핑 전략
from leverage_worker.strategy.strategies.scalping_range import ScalpingRangeStrategy


__all__ = [
    # 기본 예시 전략
    "ExampleStrategy",
    "SimpleMomentumStrategy",
    # KODEX 레버리지 (122630) 전략
    "HybridMomentumStrategy",
    "BreakoutHigh5Strategy",
    "BollingerBandStrategy",
    "FibonacciLuckyStrategy",
    "FeeOptimizedStrategy",
    # ML 기반 전략 (122630)
    "MLPricePositionStrategy",
    "MLMomentumStrategy",
    "MainBeam1Strategy",
    # KODEX 코스닥150레버리지 (233740) 전략
    "KosdaqBBConservativeStrategy",
    "KosdaqDonchianStrategy",
    "KosdaqMDDTargetStrategy",
    # WebSocket 실시간 전략
    "DipBuyStrategy",
    # 스캘핑 전략
    "ScalpingRangeStrategy",
]

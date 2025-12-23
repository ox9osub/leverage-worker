"""
전략 구현체들

이 모듈을 import하면 모든 전략이 자동으로 StrategyRegistry에 등록됩니다.

KODEX 레버리지 (122630) 전략:
    - hybrid_momentum: 하이브리드 모멘텀 전략 (S_Hybrid_Mom10_MR15)
    - breakout_high5: 5일 신고가 돌파 전략 (A_Breakout_High5)
    - bollinger_band: 볼린저 밴드 전략 (C_BB_P15)
    - fibonacci_lucky: 피보나치 5일 전략 (E_Fib5_Lucky7)
    - fee_optimized: 수수료 최적화 전략 (B_FeeOpt)

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

# KODEX 코스닥150레버리지 (233740) 전략
from leverage_worker.strategy.strategies.kosdaq_bb_conservative import (
    KosdaqBBConservativeStrategy,
)
from leverage_worker.strategy.strategies.kosdaq_donchian import KosdaqDonchianStrategy
from leverage_worker.strategy.strategies.kosdaq_mdd_target import KosdaqMDDTargetStrategy


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
    # KODEX 코스닥150레버리지 (233740) 전략
    "KosdaqBBConservativeStrategy",
    "KosdaqDonchianStrategy",
    "KosdaqMDDTargetStrategy",
]

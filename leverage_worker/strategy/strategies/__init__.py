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
    - main_beam_1: ML 지정가 매수 전략 (90_1, 앙상블 모델, 1분 보유)
    - main_beam_2: ML 지정가 매수 전략 (90_2, 앙상블 모델, 2분 보유)
    - main_beam_4: ML 지정가 매수 전략 (4분 보유, 2단계 필터링)

KODEX 코스닥150레버리지 (233740) 전략:
    - kosdaq_bb_conservative: 볼린저 밴드 보수적 전략 (BB_Conservative)
    - kosdaq_donchian: 돈치안 채널 전략 (Donchian_Scientific)
    - kosdaq_mdd_target: MDD 타겟 전략 (MDD_Target)

기본 예시 전략:
    - example_strategy: 이동평균 크로스 전략
    - simple_momentum: 단순 모멘텀 전략
"""

import importlib
import pkgutil
from pathlib import Path

# 디렉토리 내 모든 전략 모듈 자동 import
# @register_strategy 데코레이터가 있는 전략은 자동으로 StrategyRegistry에 등록됨
_package_dir = Path(__file__).parent
for _, _module_name, _ in pkgutil.iter_modules([str(_package_dir)]):
    if not _module_name.startswith('_'):
        importlib.import_module(f'.{_module_name}', package=__name__)

# 명시적 export는 더 이상 필요하지 않음 (레지스트리 통해 접근)
__all__: list[str] = []

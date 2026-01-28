# -*- coding: utf-8 -*-
"""
실시간 신호 생성 모듈

변동성 예측 + 방향 판단 기반 실시간 트레이딩 신호 생성
kis-trader에서 이식 및 수정
"""

import io
import pickle
from pathlib import Path
from typing import Dict, Optional, List, Tuple
from datetime import datetime
import pandas as pd

from leverage_worker.ml.config import SignalConfig
from leverage_worker.ml.features import TradingFeatureEngineer
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


class _ModuleRemappingUnpickler(pickle.Unpickler):
    """
    pickle 로드 시 모듈 경로 재매핑

    kis-trader의 'src.ml.*' → leverage-worker의 'leverage_worker.ml.*'
    """

    MODULE_REMAP = {
        'src.ml.models.gradient_boosting': 'leverage_worker.ml.models.gradient_boosting',
        'src.ml.models.base_model': 'leverage_worker.ml.models.base_model',
        'src.ml.models': 'leverage_worker.ml.models',
        'src.ml.features': 'leverage_worker.ml.features',
        'src.ml.config': 'leverage_worker.ml.config',
        'src.ml.realtime_signal': 'leverage_worker.ml.signal_generator',
        'src.ml': 'leverage_worker.ml',
    }

    def find_class(self, module: str, name: str):
        # 모듈 경로 재매핑
        for old_module, new_module in self.MODULE_REMAP.items():
            if module.startswith(old_module):
                module = module.replace(old_module, new_module, 1)
                break

        # 범용 src → leverage_worker 매핑 (위에서 처리되지 않은 경우)
        if module.startswith('src.'):
            module = module.replace('src.', 'leverage_worker.', 1)

        return super().find_class(module, name)


class VolatilityDirectionSignalGenerator:
    """
    변동성 + 방향 기반 신호 생성기

    2단계 의사결정 시스템:
    1단계: 변동성 예측 (ML 모델) - 향후 20분 내 0.3% 이상 움직일지 예측
    2단계: 방향 판단 (규칙 기반)
       - price_position: 일일 가격 범위 내 위치
       - momentum: 5분/10분 모멘텀 방향

    Note:
        레버리지 ETF는 LONG only 전략으로 운영.
        SHORT 신호가 발생해도 무시됨. (인버스 ETF 필요시 별도 구현)
    """

    def __init__(self, config: Optional[SignalConfig] = None):
        self.config = config or SignalConfig()
        self.model = None
        self.feature_engineer = TradingFeatureEngineer()
        self.feature_columns: List[str] = []

        # 일별 고저가 추적
        self.daily_high: float = 0
        self.daily_low: float = float('inf')

    def load(self, model_path: str) -> None:
        """
        저장된 모델 로드

        Args:
            model_path: 모델 파일 경로 (.pkl)
        """
        with open(model_path, 'rb') as f:
            save_data = _ModuleRemappingUnpickler(f).load()

        self.model = save_data['model']
        self.feature_columns = save_data['feature_columns']

        # config가 저장되어 있으면 로드 (선택적)
        if 'config' in save_data and save_data['config'] is not None:
            # 기존 direction_method는 유지하고 나머지만 업데이트
            loaded_config = save_data['config']
            if hasattr(loaded_config, 'vol_confidence'):
                # 기존 config 값을 유지하면서 필요한 값만 업데이트
                pass  # 현재는 외부에서 전달받은 config 우선 사용

        logger.info(f"모델 로드 완료: {model_path} (피처 {len(self.feature_columns)}개)")

    def update_daily_state(self, daily_high: float, daily_low: float) -> None:
        """
        일별 고저가 업데이트

        Args:
            daily_high: 당일 고가
            daily_low: 당일 저가
        """
        self.daily_high = daily_high
        self.daily_low = daily_low

    def reset_daily(self) -> None:
        """일별 상태 초기화 (장 시작시 호출)"""
        self.daily_high = 0
        self.daily_low = float('inf')

    def generate_signal(
        self,
        df: pd.DataFrame,
        current_price: float,
        current_time: datetime,
    ) -> Tuple[bool, str, float]:
        """
        신호 생성

        Args:
            df: 피처가 포함된 DataFrame (engineer_features 적용 후)
            current_price: 현재가
            current_time: 현재 시간

        Returns:
            (should_signal, direction, vol_prob) 튜플
            - should_signal: 신호 발생 여부
            - direction: 'LONG', 'SHORT', 'HOLD'
            - vol_prob: 변동성 예측 확률
        """
        if self.model is None:
            raise RuntimeError("모델이 로드되지 않았습니다. load() 호출 필요")

        # 피처 추출
        if not all(col in df.columns for col in self.feature_columns):
            # 피처가 없으면 엔지니어링 수행
            df = self.feature_engineer.engineer_features(df)

        # 마지막 행의 피처
        features = df[self.feature_columns].iloc[-1:].values

        # 변동성 예측
        vol_prob = self.model.predict_proba(features)[:, 1][0]

        # 신뢰도 임계값 미달
        if vol_prob < self.config.vol_confidence:
            return False, 'HOLD', vol_prob

        # 방향 판단
        last_row = df.iloc[-1]
        direction = self._determine_direction(current_price, last_row)

        # LONG 신호만 반환 (SHORT은 인버스 ETF 필요)
        if direction == 'LONG':
            return True, 'LONG', vol_prob

        # SHORT 신호는 무시 (로그만 남김)
        if direction == 'SHORT':
            logger.debug(f"SHORT 신호 무시 (인버스 ETF 필요): vol_prob={vol_prob:.2%}")
            return False, 'SHORT', vol_prob

        return False, 'HOLD', vol_prob

    def _determine_direction(self, current_price: float, row: pd.Series) -> str:
        """
        방향 판단

        Args:
            current_price: 현재가
            row: 피처 데이터 (DataFrame의 마지막 행)

        Returns:
            'LONG', 'SHORT', 'HOLD'
        """
        if self.config.direction_method == 'momentum':
            return self._direction_momentum(row)
        else:
            return self._direction_price_position(current_price)

    def _direction_momentum(self, row: pd.Series) -> str:
        """
        모멘텀 기반 방향 판단

        5분/10분 모멘텀이 같은 방향일 때 추세 추종

        Args:
            row: 피처 데이터

        Returns:
            'LONG', 'SHORT', 'HOLD'
        """
        mom5 = row.get('momentum_5', 0)
        mom10 = row.get('momentum_10', 0)

        if mom5 > 0 and mom10 > 0:
            return 'LONG'
        elif mom5 < 0 and mom10 < 0:
            return 'SHORT'  # 인버스 ETF 필요
        return 'HOLD'

    def _direction_price_position(self, current_price: float) -> str:
        """
        가격 위치 기반 방향 판단

        일일 가격 범위 내에서 현재가의 위치를 계산하여 방향 결정
        - 하위 30%: LONG (반등 기대)
        - 상위 70%: SHORT (조정 기대) - 인버스 ETF 필요

        Args:
            current_price: 현재가

        Returns:
            'LONG', 'SHORT', 'HOLD'
        """
        daily_range = self.daily_high - self.daily_low

        if daily_range <= 0:
            return 'HOLD'

        position = (current_price - self.daily_low) / daily_range

        if position < self.config.position_long_threshold:
            return 'LONG'
        elif position > self.config.position_short_threshold:
            return 'SHORT'  # 인버스 ETF 필요
        return 'HOLD'


def create_signal_generator(
    model_path: Optional[str] = None,
    direction_method: str = 'price_position',
    vol_confidence: float = 0.85,
    take_profit_pct: float = 0.003,
    stop_loss_pct: float = 0.010,
    max_holding_minutes: int = 60,
) -> VolatilityDirectionSignalGenerator:
    """
    신호 생성기 생성 편의 함수

    Args:
        model_path: 모델 파일 경로 (None이면 나중에 load 호출 필요)
        direction_method: 방향 판단 방법 ('price_position' or 'momentum')
        vol_confidence: 변동성 예측 신뢰도 임계값
        take_profit_pct: 익절 비율
        stop_loss_pct: 손절 비율
        max_holding_minutes: 최대 보유 시간 (분)

    Returns:
        VolatilityDirectionSignalGenerator 인스턴스
    """
    config = SignalConfig(
        direction_method=direction_method,
        vol_confidence=vol_confidence,
        take_profit_pct=take_profit_pct,
        stop_loss_pct=stop_loss_pct,
        max_holding_minutes=max_holding_minutes,
    )

    generator = VolatilityDirectionSignalGenerator(config)

    if model_path:
        generator.load(model_path)

    return generator

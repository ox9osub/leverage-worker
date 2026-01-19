# -*- coding: utf-8 -*-
"""
ML 전략 설정 모듈
"""

from dataclasses import dataclass


@dataclass
class SignalConfig:
    """
    ML 신호 생성 설정

    Attributes:
        direction_method: 방향 판단 방법 ('price_position' or 'momentum')
        vol_confidence: 변동성 예측 신뢰도 임계값 (기본 0.85)
        take_profit_pct: 익절 비율 (기본 0.003 = 0.3%)
        stop_loss_pct: 손절 비율 (기본 0.010 = 1.0%)
        max_holding_minutes: 최대 보유 시간 (분, 기본 60)
        position_long_threshold: 가격 위치 LONG 임계값 (하위 30%)
        position_short_threshold: 가격 위치 SHORT 임계값 (상위 70%)
    """
    direction_method: str = 'price_position'
    vol_confidence: float = 0.85
    take_profit_pct: float = 0.003  # 0.3%
    stop_loss_pct: float = 0.010    # 1.0%
    max_holding_minutes: int = 60

    # 가격 위치 전략용 파라미터
    position_long_threshold: float = 0.3   # 하위 30%에서 LONG
    position_short_threshold: float = 0.7  # 상위 70%에서 SHORT (인버스 ETF 필요)

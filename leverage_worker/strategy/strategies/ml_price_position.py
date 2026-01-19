"""
ML 가격 위치 전략 (ml_price_position)

2단계 의사결정 시스템:
1단계: 변동성 예측 (ML) - 향후 20분 내 0.3% 이상 움직일지 예측 (신뢰도 85%+)
2단계: 방향 판단 - 일일 가격 범위 내 위치
       - 하위 30% → LONG (반등 기대)
       - 상위 70% → SHORT (인버스 ETF 필요, 현재 미지원)

대상: KODEX 레버리지 (122630)

파라미터:
    model_path: 모델 파일 경로
    vol_confidence: 변동성 예측 신뢰도 (기본 0.85)
    take_profit_pct: 익절 비율 (기본 0.003 = 0.3%)
    stop_loss_pct: 손절 비율 (기본 0.010 = 1.0%)
    max_holding_minutes: 최대 보유 시간 (기본 60분)
    position_size: 매수 수량 (기본 1)

백테스트 결과 (검증 기간):
    - 거래 수: 600회
    - 승률: 79.67%
    - 누적수익: 99.60%
    - Profit Factor: 2.02
"""

from datetime import datetime
from typing import Any, Dict, Optional

from leverage_worker.ml import (
    SignalConfig,
    VolatilityDirectionSignalGenerator,
    candles_to_dataframe,
    get_daily_high_low,
    filter_today_candles,
)
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@register_strategy("ml_price_position")
class MLPricePositionStrategy(BaseStrategy):
    """
    ML 가격 위치 전략

    변동성 예측 + 일일 가격 범위 내 위치 기반 진입
    """

    # 최소 필요 분봉 데이터 개수 (피처 계산용)
    MIN_DATA_REQUIRED = 60

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 파라미터 로드
        self._model_path = self.get_param(
            "model_path",
            "data/ml_models/volatility_direction_price_position_20260118.pkl"
        )
        self._vol_confidence = self.get_param("vol_confidence", 0.85)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.003)
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.010)
        self._max_holding_minutes = self.get_param("max_holding_minutes", 60)
        self._position_size = self.get_param("position_size", 1)

        # 신호 생성기 초기화
        config = SignalConfig(
            direction_method='price_position',
            vol_confidence=self._vol_confidence,
            take_profit_pct=self._take_profit_pct,
            stop_loss_pct=self._stop_loss_pct,
            max_holding_minutes=self._max_holding_minutes,
        )
        self._signal_generator = VolatilityDirectionSignalGenerator(config)
        self._model_loaded = False

        # 진입 시간 추적 (시간 청산용)
        self._entry_time: Optional[datetime] = None

    def _ensure_model_loaded(self) -> bool:
        """모델 로드 확인"""
        if not self._model_loaded:
            try:
                self._signal_generator.load(self._model_path)
                self._model_loaded = True
                logger.info(f"[{self.name}] 모델 로드 완료: {self._model_path}")
            except Exception as e:
                logger.error(f"[{self.name}] 모델 로드 실패: {e}")
                return False
        return True

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """신호 생성 가능 여부 확인"""
        # 충분한 분봉 데이터 확인
        if not context.has_sufficient_data(self.MIN_DATA_REQUIRED):
            return False

        # 모델 로드 확인
        if not self._ensure_model_loaded():
            return False

        return True

    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        매매 시그널 생성

        - 미보유 시: 진입 조건 확인 (변동성 예측 + 가격 위치)
        - 보유 시: 청산 조건 확인 (TP/SL/시간)
        """
        stock_code = context.stock_code

        # 포지션 보유 시 - 청산 조건 확인
        if context.has_position:
            return self._check_exit_condition(context)

        # 미보유 시 - 진입 조건 확인
        return self._check_entry_condition(context)

    def _check_entry_condition(self, context: StrategyContext) -> TradingSignal:
        """진입 조건 확인"""
        stock_code = context.stock_code

        # 분봉 데이터 → DataFrame 변환
        df = candles_to_dataframe(context.price_history)
        if df.empty or len(df) < self.MIN_DATA_REQUIRED:
            return TradingSignal.hold(stock_code, "데이터 부족")

        # 당일 분봉에서 고저가 계산
        today_candles = filter_today_candles(context.price_history)
        if today_candles:
            daily_high, daily_low = get_daily_high_low(today_candles)
        else:
            # 당일 데이터가 없으면 전체 데이터에서 계산
            daily_high, daily_low = get_daily_high_low(context.price_history)

        # 신호 생성기에 일별 고저가 업데이트
        self._signal_generator.update_daily_state(daily_high, daily_low)

        # 피처 엔지니어링 및 신호 생성
        df = self._signal_generator.feature_engineer.engineer_features(df)

        should_signal, direction, vol_prob = self._signal_generator.generate_signal(
            df=df,
            current_price=float(context.current_price),
            current_time=context.current_time,
        )

        if should_signal and direction == 'LONG':
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"ML 변동성 {vol_prob:.1%}, 가격 위치 하단",
                confidence=vol_prob,
            )

        return TradingSignal.hold(stock_code, f"진입 조건 미충족 (vol_prob={vol_prob:.1%})")

    def _check_exit_condition(self, context: StrategyContext) -> TradingSignal:
        """
        청산 조건 확인 (TP/SL/시간)
        """
        stock_code = context.stock_code
        profit_rate = context.profit_rate / 100  # % → 소수

        # 손절 (우선 확인)
        if profit_rate <= -self._stop_loss_pct:
            self._entry_time = None
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=f"손절: {profit_rate:.2%}",
                confidence=1.0,
            )

        # 익절
        if profit_rate >= self._take_profit_pct:
            self._entry_time = None
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=f"익절: {profit_rate:.2%}",
                confidence=1.0,
            )

        # 시간 청산
        if self._entry_time:
            holding_seconds = (context.current_time - self._entry_time).total_seconds()
            holding_minutes = holding_seconds / 60

            if holding_minutes >= self._max_holding_minutes:
                self._entry_time = None
                return TradingSignal.sell(
                    stock_code=stock_code,
                    quantity=context.position_quantity,
                    reason=f"시간 청산: {holding_minutes:.0f}분 보유",
                    confidence=0.8,
                )

        return TradingSignal.hold(stock_code, f"보유 중 ({profit_rate:.2%})")

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        """진입 시 콜백 - 진입 시간 기록"""
        self._entry_time = context.current_time
        logger.info(
            f"[{self.name}] 진입: {context.stock_code} @ {context.current_price:,} - {signal.reason}"
        )

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        """청산 시 콜백"""
        self._entry_time = None
        logger.info(
            f"[{self.name}] 청산: {context.stock_code} @ {context.current_price:,} - {signal.reason}"
        )

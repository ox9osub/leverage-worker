"""
ML 모멘텀 전략 (ml_momentum)

2단계 의사결정 시스템:
1단계: 변동성 예측 (ML) - 향후 20분 내 0.3% 이상 움직일지 예측 (신뢰도 85%+)
2단계: 방향 판단 - 5분/10분 모멘텀 방향 일치 시 추세 추종
       - mom5 > 0 AND mom10 > 0 → LONG
       - mom5 < 0 AND mom10 < 0 → SHORT (인버스 ETF 필요, 현재 미지원)

대상: KODEX 레버리지 (122630)

파라미터:
    model_path: 모델 파일 경로
    vol_confidence: 변동성 예측 신뢰도 (기본 0.85)
    take_profit_pct: 익절 비율 (기본 0.002 = 0.2%)
    stop_loss_pct: 손절 비율 (기본 0.007 = 0.7%)
    max_holding_minutes: 최대 보유 시간 (기본 30분)
    position_size: 매수 수량 (기본 1)

백테스트 결과 (검증 기간):
    - 거래 수: 1,614회
    - 승률: 68.22%
    - 누적수익: 64.69%
    - Profit Factor: 1.22
"""

from datetime import datetime
from typing import Any, Dict, Optional

from leverage_worker.ml import (
    SignalConfig,
    VolatilityDirectionSignalGenerator,
    candles_to_dataframe,
)
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@register_strategy("ml_momentum")
class MLMomentumStrategy(BaseStrategy):
    """
    ML 모멘텀 전략

    변동성 예측 + 5분/10분 모멘텀 방향 일치 시 진입
    """

    # 최소 필요 분봉 데이터 개수 (피처 계산용)
    MIN_DATA_REQUIRED = 60

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 파라미터 로드 (momentum 전략용 기본값)
        self._model_path = self.get_param(
            "model_path",
            "data/ml_models/volatility_direction_price_position_20260118.pkl"
        )
        self._vol_confidence = self.get_param("vol_confidence", 0.85)
        self._take_profit_pct = self.get_param("take_profit_pct", 0.002)  # 0.2%
        self._stop_loss_pct = self.get_param("stop_loss_pct", 0.007)      # 0.7%
        self._max_holding_minutes = self.get_param("max_holding_minutes", 30)
        self._position_size = self.get_param("position_size", 1)

        # 신호 생성기 초기화 (momentum 방식)
        config = SignalConfig(
            direction_method='momentum',  # 모멘텀 방식
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

    def generate_signal(self, context: StrategyContext, execution_mode: str = "scheduler") -> TradingSignal:
        """
        매매 시그널 생성

        - 미보유 시: 진입 조건 확인 (변동성 예측 + 모멘텀)
        - 보유 시: 청산 조건 확인 (TP/SL/시간)
        
        Args:
            context: 전략 실행 컨텍스트
            execution_mode: 실행 모드 ("scheduler" 또는 "websocket")
        """
        stock_code = context.stock_code

        # 포지션 보유 시
        if context.has_position:
            # 스케줄러 모드에서만 매수 시그널 체크 (로그용)
            if execution_mode == "scheduler":
                entry_signal = self._check_entry_condition(context)
                if entry_signal.is_buy:
                    logger.info(
                        f"[{stock_code}] 매수 시그널 감지 (포지션 보유 중 - 스킵): {entry_signal.reason}"
                    )
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

        # 피처 엔지니어링 및 신호 생성
        df = self._signal_generator.feature_engineer.engineer_features(df)

        should_signal, direction, vol_prob = self._signal_generator.generate_signal(
            df=df,
            current_price=float(context.current_price),
            current_time=context.current_time,
        )

        # 모멘텀 정보 추출
        last_row = df.iloc[-1]
        mom5 = last_row.get('momentum_5', 0)
        mom10 = last_row.get('momentum_10', 0)

        # 전일 종가 대비 등락률 계산
        prev_close = context.daily_candles[-1].close_price if context.daily_candles else context.current_price
        change_rate = (context.current_price - prev_close) / prev_close * 100 if prev_close > 0 else 0

        if should_signal and direction == 'LONG':
            logger.info(
                f"[{stock_code}] LONG 시그널 | "
                f"현재가: {context.current_price:,}({change_rate:+.2f}%) | "
                f"변동성: {vol_prob:.1%} | "
                f"모멘텀: {mom5:+.0f}/{mom10:+.0f}"
            )
            return TradingSignal.buy(
                stock_code=stock_code,
                quantity=self._position_size,
                reason=f"ML 변동성 {vol_prob:.1%}, 모멘텀 +{mom5:.0f}/+{mom10:.0f}",
                confidence=vol_prob,
            )

        if direction == 'SHORT':
            logger.info(
                f"[{stock_code}] SHORT 시그널 (미지원) | "
                f"현재가: {context.current_price:,}({change_rate:+.2f}%) | "
                f"변동성: {vol_prob:.1%} | "
                f"모멘텀: {mom5:+.0f}/{mom10:+.0f}"
            )
        else:
            # HOLD 시 디버그 로그 (시그널 미발생 원인 파악용)
            logger.info(
                f"[{stock_code}] HOLD | "
                f"현재가: {context.current_price:,}({change_rate:+.2f}%) | "
                f"변동성: {vol_prob:.1%} (임계: {self._vol_confidence:.0%}) | "
                f"모멘텀: {mom5:+.0f}/{mom10:+.0f} | "
                f"방향: {direction}"
            )

        return TradingSignal.hold(stock_code, f"진입 조건 미충족 (vol_prob={vol_prob:.1%})")

    def _check_exit_condition(self, context: StrategyContext) -> TradingSignal:
        """
        청산 조건 확인 (TP/SL/시간)

        H-L 범위 체크: 분봉의 고가/저가로 60초 사이 터치 여부 확인
        """
        stock_code = context.stock_code
        avg_price = context.avg_price

        # TP/SL 가격 계산
        tp_price = avg_price * (1 + self._take_profit_pct)
        sl_price = avg_price * (1 - self._stop_loss_pct)

        # 최신 분봉의 H/L 가져오기 (폴링 백업용)
        candle_high = context.current_price
        candle_low = context.current_price
        if context.price_history:
            latest_candle = context.price_history[-1]
            candle_high = latest_candle.high_price
            candle_low = latest_candle.low_price

        # 손절 (우선 확인) - 분봉 저가가 SL 이하면 손절
        if candle_low <= sl_price:
            sl_rate = (candle_low - avg_price) / avg_price
            self._entry_time = None
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=f"손절: {sl_rate:.2%} (저가 {candle_low:,} <= SL {sl_price:,.0f})",
                confidence=1.0,
            )

        # 익절 - 분봉 고가가 TP 이상이면 익절
        if candle_high >= tp_price:
            tp_rate = (candle_high - avg_price) / avg_price
            self._entry_time = None
            return TradingSignal.sell(
                stock_code=stock_code,
                quantity=context.position_quantity,
                reason=f"익절: {tp_rate:.2%} (고가 {candle_high:,} >= TP {tp_price:,.0f})",
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

        profit_rate = context.profit_rate / 100
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

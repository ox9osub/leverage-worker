"""
main_beam_1 전략 (90_1 지정가 매수 전략)

ML 기반 1분 지정가 스캘핑 전략
- 앙상블 모델 (LightGBM+XGBoost+CatBoost+RF)로 성공 확률 예측
- 129개 피처 사용
- 지정가 매수/매도로 슬리피지 최소화
- 1분 타임아웃 손절

매매 로직:
1. 매분 1초에 데이터 조회
2. n-1분 봉의 피처 계산 (129개)
3. ML 모델로 성공 확률 예측 (threshold >= 0.60)
4. 이전 봉 종가 * 0.999 지정가 매수
5. 매수 체결 시 매수가 * 1.001 지정가 매도
6. 1분 경과 시 시장가 손절

대상: KODEX 레버리지 (122630)

파라미터:
    model_path: 모델 파일 경로 (limit_order_1min.joblib)
    threshold: 신호 발생 임계값 (기본 0.60)
    buy_discount_pct: 매수 할인율 (기본 0.001 = -0.1%)
    sell_profit_pct: 매도 수익율 (기본 0.001 = +0.1%)
    timeout_seconds: 타임아웃 (기본 60초)
    position_size: 매수 수량 (기본 1)

백테스트 결과:
    - 보유시간: 1분
    - 승률: 86.7%
    - 총수익: 79.84%
    - 거래수: 2,062회
"""

import logging
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, Optional

from leverage_worker.ml.data_utils import candles_to_dataframe
from leverage_worker.ml.ensemble_classifier import EnsembleClassifier
from leverage_worker.ml.features_limit_order import calculate_features
from leverage_worker.scalping.executor import round_to_tick_size
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy

logger = logging.getLogger(__name__)


@register_strategy("main_beam_1")
class MainBeam1Strategy(BaseStrategy):
    """
    main_beam_1 전략 (90_1 지정가 매수)

    ML 기반 1분 스캘핑 전략
    - 앙상블 모델로 성공 확률 예측
    - 지정가 매수/매도로 슬리피지 최소화
    - 1분 타임아웃 손절
    """

    # 최소 필요 분봉 데이터 개수 (60분 이평선 + 여유분)
    # 전날 데이터 포함 시 09:00부터 거래 가능
    MIN_DATA_REQUIRED = 100

    # 기본 모델 경로
    DEFAULT_MODEL_PATH = "data/ml_models/limit_order_1min.joblib"

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 파라미터 로드
        self._model_path = self.get_param("model_path", self.DEFAULT_MODEL_PATH)
        self._threshold = self.get_param("threshold", 0.60)
        self._buy_discount_pct = self.get_param("buy_discount_pct", 0.001)  # -0.1%
        self._sell_profit_pct = self.get_param("sell_profit_pct", 0.001)    # +0.1%
        self._timeout_seconds = self.get_param("timeout_seconds", 60)       # 1분
        self._position_size = self.get_param("position_size", 1)

        # 거래 시간 설정
        self._trading_start = self.get_param("trading_start", "09:00")
        self._trading_end = self.get_param("trading_end", "15:25")

        # 모델 초기화
        self._model: Optional[EnsembleClassifier] = None
        self._model_loaded = False
        self._feature_cols: Optional[list] = None

        # 진입 추적
        self._entry_time: Optional[datetime] = None
        self._entry_price: Optional[int] = None

    def _ensure_model_loaded(self) -> bool:
        """모델 로드 확인 및 수행 (Pre-loading)"""
        if self._model_loaded:
            return True

        try:
            model_path = Path(self._model_path)
            if not model_path.is_absolute():
                # 프로젝트 루트 기준 상대 경로 처리
                project_root = Path(__file__).resolve().parent.parent.parent.parent
                model_path = project_root / self._model_path

            self._model = EnsembleClassifier.load(str(model_path))
            self._feature_cols = self._model.feature_cols

            # 모델에서 threshold 로드 (있으면 덮어쓰기)
            if self._model.threshold:
                model_threshold = self._model.threshold
                if model_threshold != self._threshold:
                    logger.info(
                        f"[{self.name}] 모델 threshold 적용: "
                        f"{self._threshold} → {model_threshold}"
                    )
                    self._threshold = model_threshold

            self._model_loaded = True
            logger.info(
                f"[{self.name}] 모델 로드 완료: {model_path.name} | "
                f"피처 {len(self._feature_cols or [])}개 | "
                f"threshold={self._threshold}"
            )
            return True

        except FileNotFoundError as e:
            logger.error(f"[{self.name}] 모델 파일 없음: {self._model_path}")
            return False
        except Exception as e:
            logger.error(f"[{self.name}] 모델 로드 실패: {e}")
            return False

    def _is_trading_time(self, current_time: datetime) -> bool:
        """거래 시간 확인"""
        time_str = current_time.strftime("%H:%M")
        return self._trading_start <= time_str <= self._trading_end

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """신호 생성 가능 여부 확인"""
        # 모델 로드 확인
        if not self._ensure_model_loaded():
            return False

        # 거래 시간 확인
        if not self._is_trading_time(context.current_time):
            return False

        # 충분한 분봉 데이터 확인
        if not context.has_sufficient_data(self.MIN_DATA_REQUIRED):
            logger.debug(
                f"[{self.name}] 데이터 부족: "
                f"{len(context.price_history)}/{self.MIN_DATA_REQUIRED}"
            )
            return False

        return True

    def generate_signal(
        self, context: StrategyContext, execution_mode: str = "scheduler"
    ) -> TradingSignal:
        """
        매매 시그널 생성

        1. 미보유 시: 진입 조건 확인
        2. 보유 시: 스캘핑 executor가 청산 처리
        """
        stock_code = context.stock_code

        # 포지션 보유 시 - 스캘핑 모드에서는 executor가 청산 처리
        if context.has_position:
            return TradingSignal.hold(stock_code, "포지션 보유 중 - executor 청산 대기")

        # 미보유 시 - 진입 조건 확인
        return self._check_entry_condition(context)

    def _check_entry_condition(self, context: StrategyContext) -> TradingSignal:
        """진입 조건 확인"""
        stock_code = context.stock_code

        # 분봉 데이터 → DataFrame 변환
        df = candles_to_dataframe(context.price_history)
        if df.empty or len(df) < self.MIN_DATA_REQUIRED:
            return TradingSignal.hold(stock_code, "데이터 부족")

        # 피처 계산 (129개)
        try:
            df = calculate_features(df)
        except Exception as e:
            logger.warning(f"[{stock_code}] 피처 계산 실패: {e}")
            return TradingSignal.hold(stock_code, f"피처 계산 실패: {e}")

        # 피처 컬럼 확인
        if not self._feature_cols:
            logger.warning(f"[{stock_code}] 피처 컬럼 정보 없음")
            return TradingSignal.hold(stock_code, "피처 컬럼 없음")

        # 누락된 피처 확인
        missing_cols = [c for c in self._feature_cols if c not in df.columns]
        if missing_cols:
            logger.warning(
                f"[{stock_code}] 누락된 피처 {len(missing_cols)}개: "
                f"{missing_cols[:5]}..."
            )
            return TradingSignal.hold(stock_code, f"피처 누락: {len(missing_cols)}개")

        # 마지막 행의 피처 추출
        try:
            features = df[self._feature_cols].iloc[-1:].values
        except Exception as e:
            logger.warning(f"[{stock_code}] 피처 추출 실패: {e}")
            return TradingSignal.hold(stock_code, f"피처 추출 실패: {e}")

        # 확률 예측
        try:
            proba = self._model.get_signal_probability(features)
        except Exception as e:
            logger.warning(f"[{stock_code}] 모델 예측 실패: {e}")
            return TradingSignal.hold(stock_code, f"예측 실패: {e}")

        # 임계값 확인
        if proba < self._threshold:
            logger.debug(
                f"[{stock_code}] 임계값 미달: "
                f"proba={proba:.4f} < threshold={self._threshold:.2f}"
            )
            return TradingSignal.hold(
                stock_code,
                f"임계값 미달 (proba={proba:.2%})"
            )

        # 지정가 매수 가격 계산: 이전 봉 종가 * (1 - discount)
        prev_close = int(df["close"].iloc[-1])
        buy_price = round_to_tick_size(
            int(prev_close * (1 - self._buy_discount_pct)), direction="down"
        )
        sell_price = round_to_tick_size(
            int(buy_price * (1 + self._sell_profit_pct)), direction="up"
        )

        logger.info(
            f"[{stock_code}] BUY 시그널 발생 | "
            f"proba={proba:.2%} >= {self._threshold:.0%} | "
            f"prev_close={prev_close:,} | "
            f"buy={buy_price:,} (-{self._buy_discount_pct*100:.1f}%) | "
            f"sell={sell_price:,} (+{self._sell_profit_pct*100:.1f}%)"
        )

        # 메타데이터에 지정가 정보 포함
        signal = TradingSignal.buy(
            stock_code=stock_code,
            quantity=self._position_size,
            reason=f"ML 확률 {proba:.1%} >= {self._threshold:.0%}",
            confidence=proba,
        )
        signal.metadata = {
            "limit_price": buy_price,
            "sell_price": sell_price,
            "timeout_seconds": self._timeout_seconds,
            "proba": proba,
            "prev_close": prev_close,
        }

        return signal

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        """진입 완료 콜백"""
        self._entry_time = context.current_time
        self._entry_price = signal.metadata.get("limit_price", context.current_price)
        logger.info(
            f"[{self.name}] 진입: {context.stock_code} @ "
            f"{self._entry_price:,}원, qty={signal.quantity}"
        )

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        """청산 완료 콜백"""
        if self._entry_price and self._entry_price > 0:
            exit_price = context.current_price
            pnl_pct = (exit_price - self._entry_price) / self._entry_price * 100
            holding_sec = 0
            if self._entry_time:
                holding_sec = (context.current_time - self._entry_time).total_seconds()

            logger.info(
                f"[{self.name}] 청산: {context.stock_code} @ {exit_price:,}원 | "
                f"pnl={pnl_pct:+.2f}% | holding={holding_sec:.0f}초"
            )

        self._entry_time = None
        self._entry_price = None

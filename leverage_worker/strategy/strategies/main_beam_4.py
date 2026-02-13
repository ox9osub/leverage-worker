"""
main_beam_4 전략 (Look-Ahead Bias 수정 버전)

ML 기반 4분 지정가 스캘핑 전략
- 2단계 필터링 (앙상블 → 메타학습 RF)
- Look-Ahead Bias 완전 제거
- 129개 피처 사용
- 지정가 매수/매도로 슬리피지 최소화
- 4분 타임아웃 손절
- 체결봉 SL 제외 (시그널 후 60초)

매매 로직:
1. 매분 1초에 데이터 조회
2. n-1분 봉의 피처 계산 (129개)
3. 2단계 필터링:
   - Stage 1: 앙상블 모델 확률 >= 0.75
   - Stage 2: 메타학습 모델 확률 >= 0.65
4. 이전 봉 종가 * 0.999 지정가 매수
5. 매수 체결 시 매수가 * 1.001 지정가 매도
6. 시그널 후 60초간 SL 제외, 4분 경과 시 시장가 손절

대상: KODEX 레버리지 (122630)

파라미터:
    model_path: 모델 파일 경로 (main_beam_4.joblib)
    old_threshold: Stage 1 임계값 (기본 0.75)
    new_threshold: Stage 2 임계값 (기본 0.65)
    buy_discount_pct: 매수 할인율 (기본 0.001 = -0.1%)
    sell_profit_pct: 매도 수익율 (기본 0.001 = +0.1%)
    timeout_seconds: 타임아웃 (기본 240초 = 4분)
    sl_check_from_next_bar: 체결봉 SL 제외 (기본 True)
    * 수량은 config의 allocation 비율로 자동 계산

백테스트 결과:
    - 보유시간: 4분
    - 승률: 86.78%
    - 총수익: 56.76%
    - 거래수: 3,539회
"""

import csv
import logging
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, Optional

from leverage_worker.ml.data_utils import candles_to_dataframe
from leverage_worker.ml.two_stage_classifier import TwoStageClassifier
from leverage_worker.ml.features_limit_order import calculate_features
from leverage_worker.scalping.executor import round_to_tick_size
from leverage_worker.strategy.base import BaseStrategy, StrategyContext, TradingSignal
from leverage_worker.strategy.registry import register_strategy

logger = logging.getLogger(__name__)


@register_strategy("main_beam_4")
class MainBeam4Strategy(BaseStrategy):
    """
    main_beam_4 전략 (Look-Ahead Bias 수정 버전)

    ML 기반 4분 스캘핑 전략
    - 2단계 필터링 (앙상블 0.75 → 메타학습 RF 0.65)
    - Look-Ahead Bias 완전 제거
    - 지정가 매수/매도로 슬리피지 최소화
    - 체결봉 SL 제외 (시그널 후 60초)
    """

    # 최소 필요 분봉 데이터 개수 (60분 이평선 + 여유분)
    MIN_DATA_REQUIRED = 100

    # 기본 모델 경로
    DEFAULT_MODEL_PATH = "data/ml_models/main_beam_4/main_beam_4.joblib"

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        super().__init__(name, params)

        # 파라미터 로드
        self._model_path = self.get_param("model_path", self.DEFAULT_MODEL_PATH)
        self._old_threshold = self.get_param("old_threshold", 0.75)
        self._new_threshold = self.get_param("new_threshold", 0.65)
        self._buy_discount_pct = self.get_param("buy_discount_pct", 0.001)  # -0.1%
        self._sell_profit_pct = self.get_param("sell_profit_pct", 0.001)    # +0.1%
        self._timeout_seconds = self.get_param("timeout_seconds", 240)      # 4분
        self._sl_check_from_next_bar = self.get_param("sl_check_from_next_bar", True)

        # 거래 시간 설정
        self._trading_start = self.get_param("trading_start", "09:00")
        self._trading_end = self.get_param("trading_end", "15:19")

        # 모델 초기화
        self._model: Optional[TwoStageClassifier] = None
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

            self._model = TwoStageClassifier.load(str(model_path))
            self._feature_cols = self._model.feature_cols

            # 모델에서 threshold 로드 (있으면 덮어쓰기)
            if self._model.old_threshold != self._old_threshold:
                logger.info(
                    f"[{self.name}] 모델 old_threshold 적용: "
                    f"{self._old_threshold} → {self._model.old_threshold}"
                )
                self._old_threshold = self._model.old_threshold

            if self._model.new_threshold != self._new_threshold:
                logger.info(
                    f"[{self.name}] 모델 new_threshold 적용: "
                    f"{self._new_threshold} → {self._model.new_threshold}"
                )
                self._new_threshold = self._model.new_threshold

            self._model_loaded = True
            logger.info(
                f"[{self.name}] 모델 로드 완료: {model_path.name} | "
                f"피처 {len(self._feature_cols or [])}개 | "
                f"old_threshold={self._old_threshold} | "
                f"new_threshold={self._new_threshold}"
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

        1. 미보유 시: 진입 조건 확인 (2단계 필터링)
        2. 보유 시: 스캘핑 executor가 청산 처리
        """
        stock_code = context.stock_code

        # 포지션 보유 시 - 스캘핑 모드에서는 executor가 청산 처리
        if context.has_position:
            return TradingSignal.hold(stock_code, "포지션 보유 중 - executor 청산 대기")

        # 미보유 시 - 진입 조건 확인
        return self._check_entry_condition(context)

    def _check_entry_condition(self, context: StrategyContext) -> TradingSignal:
        """진입 조건 확인 (2단계 필터링)"""
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

        # 2단계 필터링
        try:
            old_proba, new_proba, has_signal = self._model.get_two_stage_probability(features)
        except Exception as e:
            logger.warning(f"[{stock_code}] 모델 예측 실패: {e}")
            return TradingSignal.hold(stock_code, f"예측 실패: {e}")

        # OHLCV + 확률 로그 출력
        last_row = df.iloc[-1]
        logger.info(
            f"[{self.name}] prob({old_proba:.1%}/{new_proba:.1%}) "
            f"O({int(last_row['open']):,}) H({int(last_row['high']):,}) "
            f"L({int(last_row['low']):,}) C({int(last_row['close']):,}) "
            f"V({int(last_row['volume']):,})"
        )

        # CSV 시그널 기록 (분석/백테스트용)
        self._record_signal_to_csv(context, df, old_proba, new_proba, has_signal)

        # 2단계 필터링 결과 확인
        if not has_signal:
            if old_proba < self._old_threshold:
                reason = f"Stage1 미달: {old_proba:.1%} < {self._old_threshold:.0%}"
            else:
                reason = f"Stage2 미달: {new_proba:.1%} < {self._new_threshold:.0%}"

            logger.info(f"[{stock_code}] {reason}")
            return TradingSignal.hold(stock_code, reason)

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
            f"Stage1: {old_proba:.1%} >= {self._old_threshold:.0%} | "
            f"Stage2: {new_proba:.1%} >= {self._new_threshold:.0%} | "
            f"prev_close={prev_close:,} | "
            f"buy={buy_price:,} (-{self._buy_discount_pct*100:.1f}%) | "
            f"sell={sell_price:,} (+{self._sell_profit_pct*100:.1f}%)"
        )

        # 메타데이터에 지정가 정보 포함 (quantity=0: executor에서 allocation 기반 계산)
        signal = TradingSignal.buy(
            stock_code=stock_code,
            quantity=0,
            reason=f"2단계 통과: Stage1={old_proba:.1%}, Stage2={new_proba:.1%}",
            confidence=new_proba,
        )
        signal.metadata = {
            "limit_price": buy_price,
            "sell_price": sell_price,
            "timeout_seconds": self._timeout_seconds,
            "old_proba": old_proba,
            "new_proba": new_proba,
            "prev_close": prev_close,
            "sl_check_from_next_bar": self._sl_check_from_next_bar,
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

    def _record_signal_to_csv(
        self,
        context: StrategyContext,
        df,
        old_proba: float,
        new_proba: float,
        has_signal: bool
    ) -> None:
        """시그널 정보를 CSV에 기록 (분석/백테스트용)"""
        try:
            last_row = df.iloc[-1]

            # 기본 정보 계산
            prev_close = int(df["close"].iloc[-1])
            change_rate = (context.current_price - prev_close) / prev_close * 100 if prev_close > 0 else 0
            pos_str = "보유" if context.has_position else ""
            signal_label = "BUY" if has_signal else "HOLD"

            # 지정가 정보 (BUY 시그널인 경우에만 의미있음)
            if has_signal:
                buy_price = round_to_tick_size(
                    int(prev_close * (1 - self._buy_discount_pct)), direction="down"
                )
                sell_price = round_to_tick_size(
                    int(buy_price * (1 + self._sell_profit_pct)), direction="up"
                )
            else:
                buy_price = 0
                sell_price = 0

            # CSV 경로
            csv_dir = Path(__file__).resolve().parent.parent.parent / "data" / "signals"
            csv_dir.mkdir(parents=True, exist_ok=True)
            csv_path = csv_dir / f"{context.current_time.strftime('%Y%m%d')}-{self.name}.csv"

            # 헤더 정의 (35개 필드 - old_proba, new_proba 추가)
            headers = [
                # Tier 1: 기본 정보
                "시간", "현재가", "등락률", "포지션", "시그널",
                "old_proba", "new_proba", "old_threshold", "new_threshold",
                # Tier 2: 주문 정보
                "limit_price", "sell_price", "prev_close",
                # Tier 3: 기술지표
                "rsi_7", "rsi_14", "macd", "macd_signal", "macd_hist",
                "bb_position_20", "bb_width_20", "stoch_k_7", "stoch_d_7",
                # Tier 4: 거래량/모멘텀/변동성
                "volume_ratio_20", "volume_surge", "momentum_5", "momentum_10", "momentum_20",
                "volatility_20", "atr_pct",
                # Tier 5: 캔들 패턴
                "is_bullish", "is_bearish", "consecutive_up", "consecutive_down",
                # Tier 6: 시장 상태/시간
                "daily_position", "price_vs_day_open",
                "minutes_since_open", "is_opening_30min", "is_closing_30min", "session_progress"
            ]

            # 데이터 행
            row = [
                # Tier 1: 기본 정보
                context.current_time.strftime("%H:%M:%S"),
                context.current_price,
                f"{change_rate:+.2f}",
                pos_str,
                signal_label,
                f"{old_proba:.4f}",
                f"{new_proba:.4f}",
                f"{self._old_threshold:.2f}",
                f"{self._new_threshold:.2f}",
                # Tier 2: 주문 정보
                buy_price,
                sell_price,
                prev_close,
                # Tier 3: 기술지표
                f"{last_row.get('rsi_7', 0):.2f}",
                f"{last_row.get('rsi_14', 0):.2f}",
                f"{last_row.get('macd', 0):.4f}",
                f"{last_row.get('macd_signal', 0):.4f}",
                f"{last_row.get('macd_hist', 0):.4f}",
                f"{last_row.get('bb_position_20', 0):.4f}",
                f"{last_row.get('bb_width_20', 0):.4f}",
                f"{last_row.get('stoch_k_7', 0):.2f}",
                f"{last_row.get('stoch_d_7', 0):.2f}",
                # Tier 4: 거래량/모멘텀/변동성
                f"{last_row.get('volume_ratio_20', 0):.4f}",
                int(last_row.get('volume_surge', 0)),
                f"{last_row.get('momentum_5', 0):+.0f}",
                f"{last_row.get('momentum_10', 0):+.0f}",
                f"{last_row.get('momentum_20', 0):+.0f}",
                f"{last_row.get('volatility_20', 0):.6f}",
                f"{last_row.get('atr_pct', 0):.6f}",
                # Tier 5: 캔들 패턴
                int(last_row.get('is_bullish', 0)),
                int(last_row.get('is_bearish', 0)),
                int(last_row.get('consecutive_up', 0)),
                int(last_row.get('consecutive_down', 0)),
                # Tier 6: 시장 상태/시간
                f"{last_row.get('daily_position', 0):.4f}",
                f"{last_row.get('price_vs_day_open', 0):.4f}",
                int(last_row.get('minutes_since_open', 0)),
                int(last_row.get('is_opening_30min', 0)),
                int(last_row.get('is_closing_30min', 0)),
                f"{last_row.get('session_progress', 0):.4f}"
            ]

            # CSV 기록
            write_header = not csv_path.exists()
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(headers)
                writer.writerow(row)

        except Exception as e:
            # CSV 기록 실패는 전략 실행에 영향 주지 않음
            logger.debug(f"[{context.stock_code}] CSV 기록 실패: {e}")

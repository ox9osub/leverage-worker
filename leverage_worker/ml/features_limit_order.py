"""
지정가 매수 전략용 129개 피처 계산

main_beam_1 전략 (90_1 백테스트) 전용
ML_LIMIT_ORDER_STRATEGY_RESULTS.md의 calculate_features() 함수 구현
"""
import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def calculate_features(bars: pd.DataFrame) -> pd.DataFrame:
    """
    봉 데이터로부터 129개 피처 계산

    Args:
        bars: OHLCV DataFrame
              columns: [timestamp, open, high, low, close, volume]
              - 최소 60개 봉 필요 (60분 이평선 피처)
              - 전날 데이터 포함 시 09:00부터 거래 가능

    Returns:
        피처가 추가된 DataFrame (마지막 행만 사용)

    Note:
        ML_LIMIT_ORDER_STRATEGY_RESULTS.md의 피처 계산 로직과 정확히 일치해야 함
    """
    df = bars.copy()

    # timestamp가 없으면 index 사용
    if "timestamp" not in df.columns and df.index.name == "timestamp":
        df = df.reset_index()

    # timestamp를 datetime으로 변환
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    # === 지정가 매수 특화 피처 ===
    df["prev_close"] = df["close"].shift(1)
    df["low_vs_prev_close"] = (df["low"] - df["prev_close"]) / (
        df["prev_close"] + 1e-10
    )
    df["entry_price"] = df["prev_close"] * 0.999  # -0.1%
    df["can_fill"] = (df["low"] < df["entry_price"]).astype(int)
    df["fill_margin"] = (df["entry_price"] - df["low"]) / (df["prev_close"] + 1e-10)
    df["price_drop_from_prev"] = (df["close"] - df["prev_close"]) / (
        df["prev_close"] + 1e-10
    )

    # === 시간 피처 ===
    if "timestamp" in df.columns:
        df["hour"] = df["timestamp"].dt.hour
        df["minute"] = df["timestamp"].dt.minute
        df["day_of_week"] = df["timestamp"].dt.dayofweek
        df["minutes_since_open"] = (df["hour"] - 9) * 60 + df["minute"]
        df["is_opening_30min"] = (
            (df["hour"] == 9) & (df["minute"] < 30)
        ).astype(int)
        df["is_closing_30min"] = (
            (df["hour"] == 15) | ((df["hour"] == 14) & (df["minute"] >= 50))
        ).astype(int)
        df["is_morning"] = (df["hour"] < 12).astype(int)
        df["is_afternoon"] = (df["hour"] >= 12).astype(int)
        df["session_progress"] = df["minutes_since_open"] / 379
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["minute_sin"] = np.sin(2 * np.pi * df["minute"] / 60)
        df["minute_cos"] = np.cos(2 * np.pi * df["minute"] / 60)

        # date 컬럼 생성 (일별 집계용)
        df["date"] = df["timestamp"].dt.date
    else:
        # timestamp 없으면 기본값
        df["hour"] = 10
        df["minute"] = 0
        df["day_of_week"] = 0
        df["minutes_since_open"] = 60
        df["is_opening_30min"] = 0
        df["is_closing_30min"] = 0
        df["is_morning"] = 1
        df["is_afternoon"] = 0
        df["session_progress"] = 0.16
        df["hour_sin"] = 0.0
        df["hour_cos"] = 1.0
        df["minute_sin"] = 0.0
        df["minute_cos"] = 1.0
        df["date"] = pd.Timestamp.now().date()

    # === 가격 피처 ===
    df["price_change"] = df["close"].diff()
    df["price_change_pct"] = df["close"].pct_change()
    df["price_range"] = df["high"] - df["low"]
    df["price_range_pct"] = df["price_range"] / (df["close"] + 1e-10)

    for window in [3, 5, 10, 20, 30, 60]:
        df[f"price_ma_{window}"] = (
            df["close"].rolling(window=window, min_periods=1).mean()
        )
        df[f"price_std_{window}"] = (
            df["close"].rolling(window=window, min_periods=1).std()
        )
        df[f"price_vs_ma_{window}"] = (df["close"] - df[f"price_ma_{window}"]) / (
            df[f"price_ma_{window}"] + 1e-10
        )
        df[f"price_above_ma_{window}"] = (
            df["close"] > df[f"price_ma_{window}"]
        ).astype(int)

    # 일별 피처
    df["day_open"] = df.groupby("date")["open"].transform("first")
    df["price_vs_day_open"] = (df["close"] - df["day_open"]) / (df["day_open"] + 1e-10)
    df["daily_high_so_far"] = df.groupby("date")["high"].transform(
        lambda x: x.expanding().max()
    )
    df["daily_low_so_far"] = df.groupby("date")["low"].transform(
        lambda x: x.expanding().min()
    )
    df["daily_range_so_far"] = df["daily_high_so_far"] - df["daily_low_so_far"]
    df["daily_position"] = (df["close"] - df["daily_low_so_far"]) / (
        df["daily_range_so_far"] + 1e-10
    )

    # === 거래량 피처 ===
    for window in [3, 5, 10, 20, 30, 60]:
        df[f"volume_ma_{window}"] = (
            df["volume"].rolling(window=window, min_periods=1).mean()
        )
        df[f"volume_std_{window}"] = (
            df["volume"].rolling(window=window, min_periods=1).std()
        )
        df[f"volume_ratio_{window}"] = df["volume"] / (df[f"volume_ma_{window}"] + 1e-10)
        df[f"volume_zscore_{window}"] = (df["volume"] - df[f"volume_ma_{window}"]) / (
            df[f"volume_std_{window}"] + 1e-10
        )

    df["volume_change_pct"] = df["volume"].pct_change()
    df["volume_surge"] = (df["volume_ratio_20"] > 2.0).astype(int)
    df["volume_dry"] = (df["volume_ratio_20"] < 0.5).astype(int)
    df["cumulative_volume"] = df.groupby("date")["volume"].cumsum()

    # === 기술지표 ===
    # RSI
    for period in [3, 7, 14]:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        df[f"rsi_{period}"] = 100 - (100 / (1 + rs))

    # MACD
    df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
    df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["macd_crossover"] = (
        (df["macd"] > df["macd_signal"])
        & (df["macd"].shift(1) <= df["macd_signal"].shift(1))
    ).astype(int)

    # Bollinger Bands
    for window in [20]:
        ma = df["close"].rolling(window=window, min_periods=1).mean()
        std = df["close"].rolling(window=window, min_periods=1).std()
        df[f"bb_upper_{window}"] = ma + 2 * std
        df[f"bb_lower_{window}"] = ma - 2 * std
        df[f"bb_width_{window}"] = (
            df[f"bb_upper_{window}"] - df[f"bb_lower_{window}"]
        ) / (ma + 1e-10)
        df[f"bb_position_{window}"] = (df["close"] - df[f"bb_lower_{window}"]) / (
            df[f"bb_upper_{window}"] - df[f"bb_lower_{window}"] + 1e-10
        )

    # Stochastic
    for period in [7, 14]:
        low_min = df["low"].rolling(window=period, min_periods=1).min()
        high_max = df["high"].rolling(window=period, min_periods=1).max()
        df[f"stoch_k_{period}"] = (
            (df["close"] - low_min) / (high_max - low_min + 1e-10) * 100
        )
        df[f"stoch_d_{period}"] = (
            df[f"stoch_k_{period}"].rolling(window=3, min_periods=1).mean()
        )

    # ATR
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1)),
        ),
    )
    df["atr_14"] = df["tr"].rolling(window=14, min_periods=1).mean()
    df["atr_pct"] = df["atr_14"] / (df["close"] + 1e-10)

    # === 캔들 패턴 ===
    body = df["close"] - df["open"]
    full_range = df["high"] - df["low"] + 1e-10
    df["candle_body_pct"] = abs(body) / full_range
    df["candle_upper_shadow_pct"] = (
        df["high"] - np.maximum(df["close"], df["open"])
    ) / full_range
    df["candle_lower_shadow_pct"] = (
        np.minimum(df["close"], df["open"]) - df["low"]
    ) / full_range
    df["is_bullish"] = (df["close"] > df["open"]).astype(int)
    df["is_bearish"] = (df["close"] < df["open"]).astype(int)
    df["is_doji"] = (df["candle_body_pct"] < 0.1).astype(int)

    # 연속 상승/하락
    df["consecutive_up"] = _count_consecutive(df["is_bullish"].values)
    df["consecutive_down"] = _count_consecutive(df["is_bearish"].values)

    # === 수익률 피처 ===
    for period in [1, 2, 3, 5, 10, 15, 20, 30]:
        df[f"return_{period}"] = df["close"].pct_change(period)

    for window in [5, 10, 20]:
        df[f"volatility_{window}"] = (
            df["return_1"].rolling(window=window, min_periods=1).std()
        )

    for period in [5, 10, 20]:
        df[f"momentum_{period}"] = df["close"] - df["close"].shift(period)
        df[f"momentum_pct_{period}"] = df[f"momentum_{period}"] / (
            df["close"].shift(period) + 1e-10
        )

    for window in [5, 10, 20]:
        high_max = df["high"].rolling(window=window, min_periods=1).max()
        low_min = df["low"].rolling(window=window, min_periods=1).min()
        df[f"dist_from_high_{window}"] = (high_max - df["close"]) / (df["close"] + 1e-10)
        df[f"dist_from_low_{window}"] = (df["close"] - low_min) / (df["close"] + 1e-10)
        df[f"price_position_{window}"] = (df["close"] - low_min) / (
            high_max - low_min + 1e-10
        )

    # NaN/Inf 처리
    df = df.fillna(0)
    df = df.replace([np.inf, -np.inf], 0)

    return df


def _count_consecutive(series: np.ndarray) -> List[int]:
    """연속 True 카운트"""
    result = []
    count = 0
    for val in series:
        count = count + 1 if val else 0
        result.append(count)
    return result


def get_feature_columns() -> List[str]:
    """
    129개 피처 컬럼 목록 반환

    Note:
        모델 학습 시 사용된 feature_cols와 순서가 일치해야 함
        실제로는 모델 파일에서 feature_cols를 로드해서 사용하는 것을 권장
    """
    return [
        # 지정가 매수 특화 (6개)
        "low_vs_prev_close",
        "entry_price",
        "can_fill",
        "fill_margin",
        "price_drop_from_prev",
        "prev_close",
        # 시간 피처 (14개)
        "hour",
        "minute",
        "day_of_week",
        "minutes_since_open",
        "is_opening_30min",
        "is_closing_30min",
        "is_morning",
        "is_afternoon",
        "session_progress",
        "hour_sin",
        "hour_cos",
        "minute_sin",
        "minute_cos",
        # 가격 피처 기본 (4개)
        "price_change",
        "price_change_pct",
        "price_range",
        "price_range_pct",
        # 가격 이동평균 (24개 = 6 windows * 4 features)
        "price_ma_3",
        "price_std_3",
        "price_vs_ma_3",
        "price_above_ma_3",
        "price_ma_5",
        "price_std_5",
        "price_vs_ma_5",
        "price_above_ma_5",
        "price_ma_10",
        "price_std_10",
        "price_vs_ma_10",
        "price_above_ma_10",
        "price_ma_20",
        "price_std_20",
        "price_vs_ma_20",
        "price_above_ma_20",
        "price_ma_30",
        "price_std_30",
        "price_vs_ma_30",
        "price_above_ma_30",
        "price_ma_60",
        "price_std_60",
        "price_vs_ma_60",
        "price_above_ma_60",
        # 일별 피처 (6개)
        "day_open",
        "price_vs_day_open",
        "daily_high_so_far",
        "daily_low_so_far",
        "daily_range_so_far",
        "daily_position",
        # 거래량 피처 (28개 = 6 windows * 4 features + 4)
        "volume_ma_3",
        "volume_std_3",
        "volume_ratio_3",
        "volume_zscore_3",
        "volume_ma_5",
        "volume_std_5",
        "volume_ratio_5",
        "volume_zscore_5",
        "volume_ma_10",
        "volume_std_10",
        "volume_ratio_10",
        "volume_zscore_10",
        "volume_ma_20",
        "volume_std_20",
        "volume_ratio_20",
        "volume_zscore_20",
        "volume_ma_30",
        "volume_std_30",
        "volume_ratio_30",
        "volume_zscore_30",
        "volume_ma_60",
        "volume_std_60",
        "volume_ratio_60",
        "volume_zscore_60",
        "volume_change_pct",
        "volume_surge",
        "volume_dry",
        "cumulative_volume",
        # RSI (3개)
        "rsi_3",
        "rsi_7",
        "rsi_14",
        # MACD (5개)
        "ema_12",
        "ema_26",
        "macd",
        "macd_signal",
        "macd_hist",
        "macd_crossover",
        # Bollinger Bands (4개)
        "bb_upper_20",
        "bb_lower_20",
        "bb_width_20",
        "bb_position_20",
        # Stochastic (4개)
        "stoch_k_7",
        "stoch_d_7",
        "stoch_k_14",
        "stoch_d_14",
        # ATR (3개)
        "tr",
        "atr_14",
        "atr_pct",
        # 캔들 패턴 (8개)
        "candle_body_pct",
        "candle_upper_shadow_pct",
        "candle_lower_shadow_pct",
        "is_bullish",
        "is_bearish",
        "is_doji",
        "consecutive_up",
        "consecutive_down",
        # 수익률 (8개)
        "return_1",
        "return_2",
        "return_3",
        "return_5",
        "return_10",
        "return_15",
        "return_20",
        "return_30",
        # 변동성 (3개)
        "volatility_5",
        "volatility_10",
        "volatility_20",
        # 모멘텀 (6개)
        "momentum_5",
        "momentum_pct_5",
        "momentum_10",
        "momentum_pct_10",
        "momentum_20",
        "momentum_pct_20",
        # 고저점 거리 (9개)
        "dist_from_high_5",
        "dist_from_low_5",
        "price_position_5",
        "dist_from_high_10",
        "dist_from_low_10",
        "price_position_10",
        "dist_from_high_20",
        "dist_from_low_20",
        "price_position_20",
    ]

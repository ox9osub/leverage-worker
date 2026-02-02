# -*- coding: utf-8 -*-
"""
트레이딩 피처 엔지니어링 모듈

ML 모델 학습을 위한 다양한 피처를 생성합니다.
- 시간 피처
- 가격 피처 (기술지표 포함)
- 거래량 피처
- 미시구조 피처

kis-trader에서 이식
"""

import pandas as pd
import numpy as np
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class FeatureConfig:
    """피처 설정"""
    ma_windows: List[int] = None
    rsi_periods: List[int] = None
    include_ta: bool = True          # 기술지표 포함
    include_microstructure: bool = True  # 미시구조 피처 포함

    def __post_init__(self):
        if self.ma_windows is None:
            self.ma_windows = [5, 10, 20, 30, 60]
        if self.rsi_periods is None:
            self.rsi_periods = [3, 7, 14]


class TradingFeatureEngineer:
    """
    트레이딩용 피처 엔지니어링

    OHLCV 데이터로부터 ML 학습에 필요한 피처를 생성합니다.
    """

    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        전체 피처 엔지니어링 파이프라인

        Args:
            df: OHLCV DataFrame (columns: timestamp, open, high, low, close, volume)

        Returns:
            피처가 추가된 DataFrame
        """
        df = df.copy()

        # 기본 피처
        df = self.add_time_features(df)
        df = self.add_price_features(df)
        df = self.add_volume_features(df)

        # 기술지표
        if self.config.include_ta:
            df = self.add_technical_indicators(df)

        # 미시구조 피처
        if self.config.include_microstructure:
            df = self.add_microstructure_features(df)

        # 수익률 피처
        df = self.add_return_features(df)

        # NaN 처리
        df = df.fillna(0)

        return df

    def add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """시간 기반 피처 추가"""
        df = df.copy()

        if 'timestamp' not in df.columns:
            return df

        # 기본 시간 피처
        df.loc[:, 'hour'] = df['timestamp'].dt.hour
        df.loc[:, 'minute'] = df['timestamp'].dt.minute
        df.loc[:, 'day_of_week'] = df['timestamp'].dt.dayofweek

        # 장 시작 이후 분
        df.loc[:, 'minutes_since_open'] = (df['hour'] - 9) * 60 + df['minute']

        # 시간대 카테고리
        df.loc[:, 'is_opening_30min'] = ((df['hour'] == 9) & (df['minute'] < 30)).astype(int)
        df.loc[:, 'is_closing_30min'] = (df['hour'] >= 15).astype(int)
        df.loc[:, 'is_morning'] = ((df['hour'] >= 9) & (df['hour'] < 12)).astype(int)
        df.loc[:, 'is_afternoon'] = ((df['hour'] >= 12) & (df['hour'] < 15)).astype(int)

        # 세션 진행률 (0~1)
        # 9:00 = 0, 15:30 = 1
        total_minutes = 6.5 * 60  # 6시간 30분
        df.loc[:, 'session_progress'] = df['minutes_since_open'] / total_minutes
        df.loc[:, 'session_progress'] = df['session_progress'].clip(0, 1)

        # 순환 인코딩 (시간의 주기성 반영)
        df.loc[:, 'hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df.loc[:, 'hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df.loc[:, 'minute_sin'] = np.sin(2 * np.pi * df['minute'] / 60)
        df.loc[:, 'minute_cos'] = np.cos(2 * np.pi * df['minute'] / 60)

        return df

    def add_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """가격 기반 피처 추가"""
        df = df.copy()

        # 가격 변화
        df.loc[:, 'price_change'] = df['close'].diff()
        df.loc[:, 'price_change_pct'] = df['close'].pct_change()

        # 가격 범위
        df.loc[:, 'price_range'] = df['high'] - df['low']
        df.loc[:, 'price_range_pct'] = df['price_range'] / df['close']

        # 이동평균
        for window in self.config.ma_windows:
            df.loc[:, f'price_ma_{window}'] = df['close'].rolling(window=window, min_periods=1).mean()
            df.loc[:, f'price_std_{window}'] = df['close'].rolling(window=window, min_periods=1).std()

        # 이동평균 대비 위치
        for window in self.config.ma_windows:
            df.loc[:, f'price_vs_ma_{window}'] = (df['close'] - df[f'price_ma_{window}']) / df[f'price_ma_{window}']
            df.loc[:, f'price_above_ma_{window}'] = (df['close'] > df[f'price_ma_{window}']).astype(int)

        # 당일 시가 대비
        if 'timestamp' in df.columns:
            df.loc[:, 'date'] = df['timestamp'].dt.date
            df.loc[:, 'day_open'] = df.groupby('date')['open'].transform('first')
            df.loc[:, 'price_vs_day_open'] = (df['close'] - df['day_open']) / df['day_open']

        return df

    def add_volume_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """거래량 기반 피처 추가"""
        df = df.copy()

        # 거래량 이동평균
        for window in self.config.ma_windows:
            df.loc[:, f'volume_ma_{window}'] = df['volume'].rolling(window=window, min_periods=1).mean()
            df.loc[:, f'volume_std_{window}'] = df['volume'].rolling(window=window, min_periods=1).std()

        # 거래량 비율
        for window in self.config.ma_windows:
            df.loc[:, f'volume_ratio_{window}'] = df['volume'] / (df[f'volume_ma_{window}'] + 1e-10)
            df.loc[:, f'volume_zscore_{window}'] = (
                (df['volume'] - df[f'volume_ma_{window}']) /
                (df[f'volume_std_{window}'] + 1e-10)
            )

        # 거래량 변화
        df.loc[:, 'volume_change'] = df['volume'].diff()
        df.loc[:, 'volume_change_pct'] = df['volume'].pct_change()

        # 거래량 급증 플래그
        df.loc[:, 'volume_surge'] = (df['volume_ratio_20'] > 2.0).astype(int)
        df.loc[:, 'volume_dry'] = (df['volume_ratio_20'] < 0.5).astype(int)

        # 누적 거래량 (당일)
        if 'date' in df.columns:
            df.loc[:, 'cumulative_volume'] = df.groupby('date')['volume'].cumsum()

        return df

    def add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """기술지표 추가"""
        df = df.copy()

        # RSI
        for period in self.config.rsi_periods:
            df.loc[:, f'rsi_{period}'] = self._calculate_rsi(df['close'], period)

        # MACD
        df.loc[:, 'ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
        df.loc[:, 'ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
        df.loc[:, 'macd'] = df['ema_12'] - df['ema_26']
        df.loc[:, 'macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df.loc[:, 'macd_hist'] = df['macd'] - df['macd_signal']

        # 볼린저 밴드
        for window in [20]:
            ma = df['close'].rolling(window=window, min_periods=1).mean()
            std = df['close'].rolling(window=window, min_periods=1).std()
            df.loc[:, f'bb_upper_{window}'] = ma + 2 * std
            df.loc[:, f'bb_lower_{window}'] = ma - 2 * std
            df.loc[:, f'bb_width_{window}'] = (df[f'bb_upper_{window}'] - df[f'bb_lower_{window}']) / ma
            # 볼린저 밴드 내 위치 (0~1)
            df.loc[:, f'bb_position_{window}'] = (
                (df['close'] - df[f'bb_lower_{window}']) /
                (df[f'bb_upper_{window}'] - df[f'bb_lower_{window}'] + 1e-10)
            )

        # 스토캐스틱
        for period in [14]:
            low_min = df['low'].rolling(window=period, min_periods=1).min()
            high_max = df['high'].rolling(window=period, min_periods=1).max()
            df.loc[:, f'stoch_k_{period}'] = (
                (df['close'] - low_min) / (high_max - low_min + 1e-10) * 100
            )
            df.loc[:, f'stoch_d_{period}'] = df[f'stoch_k_{period}'].rolling(window=3, min_periods=1).mean()

        # ATR (Average True Range)
        df.loc[:, 'tr'] = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift(1)),
                abs(df['low'] - df['close'].shift(1))
            )
        )
        df.loc[:, 'atr_14'] = df['tr'].rolling(window=14, min_periods=1).mean()
        df.loc[:, 'atr_pct'] = df['atr_14'] / df['close']

        # ADX (Average Directional Index) - 간소화 버전
        df.loc[:, 'dm_plus'] = np.where(
            (df['high'] - df['high'].shift(1)) > (df['low'].shift(1) - df['low']),
            np.maximum(df['high'] - df['high'].shift(1), 0),
            0
        )
        df.loc[:, 'dm_minus'] = np.where(
            (df['low'].shift(1) - df['low']) > (df['high'] - df['high'].shift(1)),
            np.maximum(df['low'].shift(1) - df['low'], 0),
            0
        )
        df.loc[:, 'di_plus_14'] = (df['dm_plus'].rolling(14).mean() / df['atr_14']) * 100
        df.loc[:, 'di_minus_14'] = (df['dm_minus'].rolling(14).mean() / df['atr_14']) * 100
        df.loc[:, 'dx'] = abs(df['di_plus_14'] - df['di_minus_14']) / (df['di_plus_14'] + df['di_minus_14'] + 1e-10) * 100
        df.loc[:, 'adx_14'] = df['dx'].rolling(14).mean()

        return df

    def _calculate_rsi(self, prices: pd.Series, period: int) -> pd.Series:
        """RSI 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))

    def add_microstructure_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """미시구조 피처 추가"""
        df = df.copy()

        # 캔들 분석
        body = df['close'] - df['open']
        full_range = df['high'] - df['low'] + 1e-10

        df.loc[:, 'candle_body'] = body
        df.loc[:, 'candle_body_pct'] = abs(body) / full_range
        df.loc[:, 'candle_upper_shadow'] = df['high'] - np.maximum(df['close'], df['open'])
        df.loc[:, 'candle_lower_shadow'] = np.minimum(df['close'], df['open']) - df['low']
        df.loc[:, 'candle_upper_shadow_pct'] = df['candle_upper_shadow'] / full_range
        df.loc[:, 'candle_lower_shadow_pct'] = df['candle_lower_shadow'] / full_range

        # 캔들 방향
        df.loc[:, 'is_bullish'] = (df['close'] > df['open']).astype(int)
        df.loc[:, 'is_bearish'] = (df['close'] < df['open']).astype(int)
        df.loc[:, 'is_doji'] = (df['candle_body_pct'] < 0.1).astype(int)

        # 연속 상승/하락 봉 수
        df.loc[:, 'consecutive_up'] = self._count_consecutive(df['is_bullish'])
        df.loc[:, 'consecutive_down'] = self._count_consecutive(df['is_bearish'])

        # 최근 고점/저점 대비 거리
        for window in [5, 10, 20]:
            df.loc[:, f'dist_from_high_{window}'] = (
                (df['high'].rolling(window=window, min_periods=1).max() - df['close']) /
                df['close']
            )
            df.loc[:, f'dist_from_low_{window}'] = (
                (df['close'] - df['low'].rolling(window=window, min_periods=1).min()) /
                df['close']
            )

        # 가격 위치 (최근 범위 내)
        for window in [20, 60]:
            high_max = df['high'].rolling(window=window, min_periods=1).max()
            low_min = df['low'].rolling(window=window, min_periods=1).min()
            df.loc[:, f'price_position_{window}'] = (
                (df['close'] - low_min) / (high_max - low_min + 1e-10)
            )

        return df

    def _count_consecutive(self, series: pd.Series) -> pd.Series:
        """연속 True 개수 카운트"""
        result = series.copy()
        count = 0
        counts = []
        for val in series:
            if val:
                count += 1
            else:
                count = 0
            counts.append(count)
        return pd.Series(counts, index=series.index)

    def add_return_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """수익률 피처 추가"""
        df = df.copy()

        # 과거 N분 수익률
        for period in [1, 3, 5, 10, 15, 20, 30]:
            df.loc[:, f'return_{period}'] = df['close'].pct_change(period)

        # 변동성 (수익률 표준편차)
        for window in [5, 10, 20]:
            df.loc[:, f'volatility_{window}'] = df['return_1'].rolling(window=window, min_periods=1).std()

        # 수익률 Z-score
        for window in [20]:
            mean_ret = df['return_1'].rolling(window=window, min_periods=1).mean()
            std_ret = df['return_1'].rolling(window=window, min_periods=1).std()
            df.loc[:, f'return_zscore_{window}'] = (df['return_1'] - mean_ret) / (std_ret + 1e-10)

        # 모멘텀
        df.loc[:, 'momentum_5'] = df['close'] - df['close'].shift(5)
        df.loc[:, 'momentum_10'] = df['close'] - df['close'].shift(10)
        df.loc[:, 'momentum_20'] = df['close'] - df['close'].shift(20)

        return df

    def get_feature_columns(self, df: pd.DataFrame) -> List[str]:
        """
        학습에 사용할 피처 컬럼 목록 반환

        Args:
            df: 피처가 추가된 DataFrame

        Returns:
            피처 컬럼 이름 리스트
        """
        # 제외할 컬럼
        exclude_cols = [
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'symbol', 'date', 'day_open', 'label', 'touch_minutes', 'touch_type'
        ]

        feature_cols = [col for col in df.columns if col not in exclude_cols]
        return feature_cols

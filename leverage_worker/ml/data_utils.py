# -*- coding: utf-8 -*-
"""
ML용 데이터 변환 유틸리티

MinuteCandle <-> DataFrame 변환
"""

from typing import List
from datetime import datetime
import pandas as pd

from leverage_worker.data.minute_candle_repository import MinuteCandle


def candles_to_dataframe(candles: List[MinuteCandle]) -> pd.DataFrame:
    """
    MinuteCandle 리스트를 DataFrame으로 변환

    Args:
        candles: MinuteCandle 리스트

    Returns:
        DataFrame with columns: timestamp, open, high, low, close, volume
    """
    if not candles:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    data = []
    for candle in candles:
        # candle_datetime: "YYYY-MM-DD HH:MM" -> datetime
        timestamp = datetime.strptime(candle.candle_datetime, "%Y-%m-%d %H:%M")

        data.append({
            'timestamp': timestamp,
            'open': float(candle.open_price),
            'high': float(candle.high_price),
            'low': float(candle.low_price),
            'close': float(candle.close_price),
            'volume': int(candle.volume),
        })

    df = pd.DataFrame(data)
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def get_daily_high_low(candles: List[MinuteCandle]) -> tuple:
    """
    분봉 리스트에서 당일 고저가 계산

    Args:
        candles: MinuteCandle 리스트 (당일 데이터)

    Returns:
        (daily_high, daily_low) 튜플
    """
    if not candles:
        return 0.0, float('inf')

    daily_high = max(c.high_price for c in candles)
    daily_low = min(c.low_price for c in candles)

    return float(daily_high), float(daily_low)


def filter_today_candles(candles: List[MinuteCandle], today_date: str = None) -> List[MinuteCandle]:
    """
    분봉 리스트에서 오늘 날짜 데이터만 필터링

    Args:
        candles: MinuteCandle 리스트
        today_date: 오늘 날짜 (YYYYMMDD, None이면 자동 계산)

    Returns:
        오늘 날짜의 MinuteCandle 리스트
    """
    if today_date is None:
        today_date = datetime.now().strftime("%Y%m%d")

    return [c for c in candles if c.trade_date == today_date]

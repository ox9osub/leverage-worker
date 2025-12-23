"""
시간 관련 유틸리티

매매 시간 체크, 종목별 실행 시점 판단
"""

from datetime import datetime, time as dt_time
from typing import Tuple


def parse_time_string(time_str: str) -> dt_time:
    """
    시간 문자열을 time 객체로 변환

    Args:
        time_str: "HH:MM" 형식 문자열

    Returns:
        time 객체
    """
    parts = time_str.split(":")
    return dt_time(int(parts[0]), int(parts[1]))


def is_trading_hours(
    now: datetime,
    trading_start: str,
    trading_end: str,
) -> bool:
    """
    현재 시간이 매매 시간인지 확인

    Args:
        now: 현재 시간
        trading_start: 매매 시작 시간 (HH:MM)
        trading_end: 매매 종료 시간 (HH:MM)

    Returns:
        매매 시간이면 True
    """
    current_time = now.time()
    start_time = parse_time_string(trading_start)
    end_time = parse_time_string(trading_end)

    return start_time <= current_time <= end_time


def should_execute_stock(
    now: datetime,
    interval_seconds: int,
    offset_seconds: int = 0,
) -> bool:
    """
    현재 시점에 해당 종목을 실행해야 하는지 확인

    Args:
        now: 현재 시간
        interval_seconds: 실행 간격 (초)
        offset_seconds: 기준초 오프셋

    Returns:
        실행해야 하면 True

    Example:
        interval=5, offset=0 → 00초, 05초, 10초, ...
        interval=5, offset=2 → 02초, 07초, 12초, ...
        interval=2, offset=0 → 00초, 02초, 04초, ...
    """
    current_second = now.second
    return (current_second - offset_seconds) % interval_seconds == 0


def get_current_minute_key(now: datetime) -> str:
    """
    현재 분봉 키 생성 (OHLCV upsert용)

    Args:
        now: 현재 시간

    Returns:
        "YYYYMMDD_HHMM" 형식 문자열
    """
    return now.strftime("%Y%m%d_%H%M")


def get_today_date_str() -> str:
    """오늘 날짜 문자열 (YYYYMMDD)"""
    return datetime.now().strftime("%Y%m%d")


def get_time_until_market_open(
    now: datetime,
    trading_start: str,
) -> Tuple[int, int, int]:
    """
    장 시작까지 남은 시간 계산

    Args:
        now: 현재 시간
        trading_start: 매매 시작 시간 (HH:MM)

    Returns:
        (시, 분, 초) 튜플. 이미 장중이면 (0, 0, 0)
    """
    start_time = parse_time_string(trading_start)
    current_time = now.time()

    if current_time >= start_time:
        return (0, 0, 0)

    # 시작 시간까지 남은 초 계산
    start_seconds = start_time.hour * 3600 + start_time.minute * 60
    current_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second

    remaining = start_seconds - current_seconds

    hours = remaining // 3600
    minutes = (remaining % 3600) // 60
    seconds = remaining % 60

    return (hours, minutes, seconds)


def get_time_until_market_close(
    now: datetime,
    trading_end: str,
) -> Tuple[int, int, int]:
    """
    장 마감까지 남은 시간 계산

    Args:
        now: 현재 시간
        trading_end: 매매 종료 시간 (HH:MM)

    Returns:
        (시, 분, 초) 튜플. 이미 장 마감 후면 (0, 0, 0)
    """
    end_time = parse_time_string(trading_end)
    current_time = now.time()

    if current_time >= end_time:
        return (0, 0, 0)

    # 종료 시간까지 남은 초 계산
    end_seconds = end_time.hour * 3600 + end_time.minute * 60
    current_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second

    remaining = end_seconds - current_seconds

    hours = remaining // 3600
    minutes = (remaining % 3600) // 60
    seconds = remaining % 60

    return (hours, minutes, seconds)


def is_weekday(now: datetime) -> bool:
    """
    평일인지 확인 (월~금)

    Args:
        now: 현재 시간

    Returns:
        평일이면 True
    """
    return now.weekday() < 5


def format_duration(seconds: int) -> str:
    """
    초를 읽기 쉬운 형식으로 변환

    Args:
        seconds: 초

    Returns:
        "Xh Ym Zs" 형식 문자열
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)

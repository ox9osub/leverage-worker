"""
안전한 수학 연산 유틸리티

ZeroDivision 및 기타 수학 에러 방지
"""

from typing import Union

Number = Union[int, float]


def safe_divide(
    numerator: Number,
    denominator: Number,
    default: Number = 0.0,
) -> float:
    """
    안전한 나눗셈

    Args:
        numerator: 분자
        denominator: 분모
        default: 분모가 0일 때 반환값

    Returns:
        나눗셈 결과 또는 default
    """
    if denominator == 0:
        return float(default)
    return numerator / denominator


def safe_percentage_change(
    current: Number,
    base: Number,
    default: Number = 0.0,
) -> float:
    """
    안전한 변화율(%) 계산

    Args:
        current: 현재 값
        base: 기준 값
        default: 기준이 0일 때 반환값

    Returns:
        변화율 (%)
    """
    if base == 0:
        return float(default)
    return ((current - base) / base) * 100


def safe_profit_rate(
    sell_price: Number,
    buy_price: Number,
    default: Number = 0.0,
) -> float:
    """
    안전한 수익률(%) 계산

    Args:
        sell_price: 매도가
        buy_price: 매수가
        default: 매수가가 0일 때 반환값

    Returns:
        수익률 (%)
    """
    return safe_percentage_change(sell_price, buy_price, default)


def safe_profit_loss(
    sell_price: Number,
    buy_price: Number,
    quantity: int,
) -> int:
    """
    안전한 손익(원) 계산

    Args:
        sell_price: 매도가
        buy_price: 매수가
        quantity: 수량

    Returns:
        손익 (원)
    """
    return int((sell_price - buy_price) * quantity)


def clamp(value: Number, min_value: Number, max_value: Number) -> Number:
    """
    값을 범위 내로 제한

    Args:
        value: 원본 값
        min_value: 최소값
        max_value: 최대값

    Returns:
        범위 내로 제한된 값
    """
    return max(min_value, min(max_value, value))


def round_to_tick(price: float, tick_size: float = 1.0) -> int:
    """
    가격을 틱 단위로 반올림

    Args:
        price: 원본 가격
        tick_size: 틱 크기 (기본 1원)

    Returns:
        틱 단위로 반올림된 가격
    """
    if tick_size <= 0:
        return int(round(price))
    return int(round(price / tick_size) * tick_size)


def calculate_allocation_amount(
    total_capital: int,
    allocation_percent: float,
    current_price: int,
) -> int:
    """
    할당 비율에 따른 매수 수량 계산

    Args:
        total_capital: 총 자본금
        allocation_percent: 할당 비율 (%)
        current_price: 현재가

    Returns:
        매수 가능 수량
    """
    if current_price <= 0:
        return 0

    allocation_amount = total_capital * (allocation_percent / 100)
    quantity = int(allocation_amount / current_price)
    return max(0, quantity)

"""
실시간 가격 범위 추적기

WebSocket tick 데이터의 롤링 윈도우를 유지하며 percentile 기반 가격 계산 제공
"""

import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, List, Optional, Tuple

from leverage_worker.utils.logger import get_logger

logger = get_logger("scalping.price_tracker")


class PriceRangeTracker:
    """
    실시간 가격 롤링 윈도우 추적기

    최대 max_window_seconds(60초) 데이터를 보관하고,
    조회 시 window_seconds 범위 내 데이터만 사용하여 percentile 계산.
    """

    def __init__(
        self,
        window_seconds: int = 10,
        max_window_seconds: int = 60,
    ) -> None:
        self._window_seconds = window_seconds
        self._max_window_seconds = max_window_seconds
        self._ticks: Deque[Tuple[datetime, int]] = deque()
        self._lock = threading.Lock()

    def add_tick(self, timestamp: datetime, price: int) -> None:
        """tick 추가 및 만료 데이터 제거"""
        with self._lock:
            self._ticks.append((timestamp, price))
            self._prune(timestamp)

    def get_percentile(
        self,
        percentile: float,
        window_seconds: Optional[int] = None,
    ) -> Optional[int]:
        """
        윈도우 내 가격의 N번째 percentile 반환

        Args:
            percentile: 0~100 사이 값 (예: 10 → P10 = 하단 10%)
            window_seconds: 사용할 윈도우 크기 (None이면 기본값)

        Returns:
            해당 percentile의 가격, 데이터 부족 시 None
        """
        prices = self._get_prices(window_seconds)
        if not prices:
            return None

        prices.sort()
        idx = int(len(prices) * percentile / 100)
        idx = min(idx, len(prices) - 1)
        return prices[idx]

    def get_range(
        self,
        window_seconds: Optional[int] = None,
    ) -> Optional[Tuple[int, int]]:
        """윈도우 내 (최저가, 최고가) 반환"""
        prices = self._get_prices(window_seconds)
        if not prices:
            return None
        return (min(prices), max(prices))

    def get_volatility(
        self,
        window_seconds: Optional[int] = None,
    ) -> Optional[float]:
        """
        윈도우 내 변동폭 비율 반환

        Returns:
            (max - min) / min (예: 0.005 = 0.5%)
        """
        price_range = self.get_range(window_seconds)
        if price_range is None:
            return None
        low, high = price_range
        if low <= 0:
            return None
        return (high - low) / low

    def get_adaptive_window(self) -> int:
        """
        변동성 기반 동적 윈도우 크기 결정

        30초 변동폭 기준:
        - > high_volatility_threshold → min_window
        - < low_volatility_threshold → max_window
        - 사이 → 기본 window_seconds
        """
        volatility = self.get_volatility(window_seconds=30)
        if volatility is None:
            return self._window_seconds

        # 높은 변동성 → 작은 윈도우 (빠른 반응)
        if volatility > 0.005:
            return 15
        # 낮은 변동성 → 큰 윈도우 (안정적)
        if volatility < 0.002:
            return 60
        return 30

    def get_tick_count(self, window_seconds: Optional[int] = None) -> int:
        """윈도우 내 tick 수"""
        return len(self._get_prices(window_seconds))

    def is_ready(self, min_ticks: int = 10) -> bool:
        """매매 가능 최소 tick 수 충족 여부"""
        return self.get_tick_count() >= min_ticks

    def get_current_window_seconds(self, adaptive: bool = False) -> int:
        """현재 사용 중인 윈도우 크기"""
        if adaptive:
            return self.get_adaptive_window()
        return self._window_seconds

    def get_uptick_ratio(
        self, window_seconds: Optional[int] = None
    ) -> Optional[float]:
        """
        윈도우 내 상승틱 비율 반환.

        연속된 틱 쌍에서 가격이 올라간 비율 계산.
        예: 틱 [100, 99, 100, 101, 100] → 변화: [-1, +1, +1, -1] → 상승 2/4 = 0.5

        Returns:
            0.0~1.0 사이 비율. 데이터 부족 시 None.
        """
        prices = self._get_prices(window_seconds)
        if len(prices) < 4:
            return None

        up_count = 0
        total_changes = 0
        for i in range(1, len(prices)):
            if prices[i] != prices[i - 1]:
                total_changes += 1
                if prices[i] > prices[i - 1]:
                    up_count += 1

        if total_changes == 0:
            return 0.5  # 가격 변화 없음 → 중립

        return up_count / total_changes

    def reset(self) -> None:
        """전체 데이터 초기화"""
        with self._lock:
            self._ticks.clear()

    def _get_prices(self, window_seconds: Optional[int] = None) -> List[int]:
        """윈도우 내 가격 리스트 반환 (thread-safe)"""
        ws = window_seconds if window_seconds is not None else self._window_seconds
        with self._lock:
            if not self._ticks:
                return []
            cutoff = self._ticks[-1][0] - timedelta(seconds=ws)
            return [price for ts, price in self._ticks if ts >= cutoff]

    def _prune(self, current_time: datetime) -> None:
        """max_window_seconds보다 오래된 데이터 제거 (lock 내부 호출)"""
        cutoff = current_time - timedelta(seconds=self._max_window_seconds)
        while self._ticks and self._ticks[0][0] < cutoff:
            self._ticks.popleft()

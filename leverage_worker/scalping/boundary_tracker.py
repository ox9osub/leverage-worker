"""
동적 바운더리 추적 및 매수 시그널 생성

틱 기반 마이크로 윈도우로 상/하단 바운더리를 추적하고,
바운더리 range 0.1%~0.15%가 1초 유지 시 P10에서 매수 시그널 생성.
"""

import threading
import time
from collections import deque
from typing import Deque, Optional, Tuple

from leverage_worker.utils.logger import get_logger

logger = get_logger("scalping.boundary_tracker")


class AdaptiveBoundaryTracker:
    """
    틱 기반 동적 바운더리 추적기

    최근 N틱의 최고/최저가를 바운더리로 유지하며,
    하단 이탈 시 즉시 리셋하여 하락 추세를 추적.
    DIP 패턴 감지로 매수 시그널 생성.
    """

    def __init__(
        self,
        boundary_window_ticks: int = 45,
        max_boundary_breaches: int = 5,
        min_consecutive_downticks: int = 3,
        dip_margin_pct: float = 0.1,
        lower_history_size: int = 3,
        min_boundary_range_pct: float = 0.001,
        max_boundary_range_pct: float = 0.002,
        boundary_hold_seconds: float = 0.7,
        boundary_window_seconds: float = 1.0,
        percentile_threshold: float = 20.0,
    ) -> None:
        """
        Args:
            boundary_window_ticks: 바운더리 계산에 사용할 틱 수
            max_boundary_breaches: 최대 허용 breach 횟수
            min_consecutive_downticks: DEPRECATED (하위 호환용)
            dip_margin_pct: DEPRECATED (하위 호환용)
            lower_history_size: 하한 바운더리 히스토리 크기
            min_boundary_range_pct: 바운더리 최소 range 비율 (0.001 = 0.1%)
            max_boundary_range_pct: 바운더리 최대 range 비율 (0.0015 = 0.15%)
            boundary_hold_seconds: range 유효 구간 유지 시간 (초)
            boundary_window_seconds: 바운더리 시간 윈도우 (초), 틱 수와 OR 조건
            percentile_threshold: 매수 가격 퍼센타일 (10.0 = P10)
        """
        self._boundary_window_ticks = boundary_window_ticks
        self._max_boundary_breaches = max_boundary_breaches
        self._lower_history_size = lower_history_size
        self._min_boundary_range_pct = min_boundary_range_pct
        self._max_boundary_range_pct = max_boundary_range_pct
        self._boundary_hold_seconds = boundary_hold_seconds
        self._boundary_window_seconds = boundary_window_seconds
        self._percentile_threshold = percentile_threshold

        # 틱 데이터 (시간+가격 저장, 시간/틱 이중 윈도우)
        self._ticks: Deque[Tuple[float, int]] = deque()

        # 바운더리 상태
        self._upper_boundary: Optional[int] = None
        self._lower_boundary: Optional[int] = None
        self._breach_count: int = 0

        # DIP 트리거 상태 (range 구간 유지 시간)
        self._range_qualified_at: Optional[float] = None
        self._dip_fired: bool = False

        # 하한 바운더리 히스토리
        self._lower_boundary_history: Deque[int] = deque(maxlen=lower_history_size)

        self._lock = threading.Lock()

    def add_tick(self, price: int) -> Optional[str]:
        """
        새 틱 추가 및 바운더리 갱신

        Args:
            price: 현재가

        Returns:
            "BREACH": 하단 바운더리 이탈 (바운더리 리셋됨)
            "DIP": range 0.1%~0.15% 1초 유지 (매수 시그널)
            None: 정상 (대기)
        """
        with self._lock:
            # 1. Breach 체크 (바운더리 확립된 경우만)
            if self._lower_boundary is not None:
                if price < self._lower_boundary:
                    self._breach_count += 1
                    logger.info(
                        f"[boundary] BREACH 감지: {price:,}원 < "
                        f"하단 {self._lower_boundary:,}원 "
                        f"(총 {self._breach_count}회)"
                    )
                    self._reset_boundary()
                    return "BREACH"

            # 2. 틱 추가 (시간+가격)
            now = time.monotonic()
            self._ticks.append((now, price))

            # 만료: 시간 윈도우 밖 + 틱 수 초과분 제거
            cutoff = now - self._boundary_window_seconds
            while (
                len(self._ticks) > self._boundary_window_ticks
                and self._ticks[0][0] < cutoff
            ):
                self._ticks.popleft()

            # 3. 바운더리 재계산 (틱 수 OR 시간 윈도우 충족 시)
            time_span = now - self._ticks[0][0] if self._ticks else 0
            if (
                len(self._ticks) >= self._boundary_window_ticks
                or time_span >= self._boundary_window_seconds
            ):
                self._update_boundary()

            # 4. DIP 조건: range 0.1%~0.15% 구간 1초 유지
            if (
                self._lower_boundary is not None
                and self._upper_boundary is not None
                and self._lower_boundary > 0
            ):
                range_pct = (
                    (self._upper_boundary - self._lower_boundary)
                    / self._lower_boundary
                )
                in_zone = (
                    self._min_boundary_range_pct
                    <= range_pct
                    <= self._max_boundary_range_pct
                )

                if in_zone:
                    now = time.monotonic()
                    if self._range_qualified_at is None:
                        self._range_qualified_at = now
                        logger.debug(
                            f"[boundary] range 유효 구간 진입: "
                            f"{range_pct:.3%} "
                            f"({self._lower_boundary:,}~{self._upper_boundary:,})"
                        )
                    elif (
                        not self._dip_fired
                        and now - self._range_qualified_at
                        >= self._boundary_hold_seconds
                    ):
                        self._dip_fired = True
                        logger.info(
                            f"[boundary] DIP 감지: range={range_pct:.3%} "
                            f"({self._boundary_hold_seconds}초 유지), "
                            f"P{self._percentile_threshold:.0f} 매수 트리거 "
                            f"({self._lower_boundary:,}~{self._upper_boundary:,})"
                        )
                        return "DIP"
                else:
                    if self._range_qualified_at is not None:
                        logger.debug(
                            f"[boundary] range 구간 이탈: "
                            f"{range_pct:.3%}"
                        )
                    self._range_qualified_at = None

            return None

    def get_buy_price(self) -> Optional[int]:
        """
        DIP 감지 시 매수 가격 반환 (P10 = 10th percentile)

        Returns:
            바운더리 틱의 percentile_threshold 가격, 미확립 시 None
        """
        with self._lock:
            if self._lower_boundary is None or len(self._ticks) == 0:
                return None

            sorted_ticks = sorted(t[1] for t in self._ticks)
            idx = max(
                0,
                int(len(sorted_ticks) * self._percentile_threshold / 100.0) - 1,
            )
            return sorted_ticks[idx]

    def get_percentile_price(self, percentile: float) -> Optional[int]:
        """바운더리 틱의 N-th percentile 가격 반환

        Args:
            percentile: 0~100 사이의 퍼센타일 값 (예: 20.0 = P20)

        Returns:
            해당 퍼센타일 가격, 틱 없으면 None
        """
        with self._lock:
            if len(self._ticks) == 0:
                return None
            sorted_ticks = sorted(t[1] for t in self._ticks)
            idx = max(0, int(len(sorted_ticks) * percentile / 100.0) - 1)
            return sorted_ticks[idx]

    def is_trading_allowed(self) -> bool:
        """
        거래 허용 여부 (최대 breach 횟수 체크)

        Returns:
            True if breach_count < max, False otherwise
        """
        with self._lock:
            return self._breach_count < self._max_boundary_breaches

    def reset(self) -> None:
        """
        전체 초기화 (신규 시그널 시작 시)
        """
        with self._lock:
            self._ticks.clear()
            self._reset_boundary()
            self._breach_count = 0
            self._lower_boundary_history.clear()  # NEW: 히스토리도 초기화
            logger.debug("[boundary] 전체 리셋 (신규 시그널)")

    def get_breach_count(self) -> int:
        """현재 breach 횟수 반환"""
        with self._lock:
            return self._breach_count

    def get_boundary_info(self) -> tuple[Optional[int], Optional[int], int]:
        """
        바운더리 정보 반환

        Returns:
            (lower_boundary, upper_boundary, tick_count)
        """
        with self._lock:
            return (
                self._lower_boundary,
                self._upper_boundary,
                len(self._ticks),
            )

    def _update_boundary(self) -> None:
        """
        바운더리 갱신 (윈도우 full일 때만 호출)

        내부 메서드, lock 내부에서만 호출됨
        """
        if not self._ticks:
            return

        prev_lower = self._lower_boundary
        prev_upper = self._upper_boundary

        prices = [t[1] for t in self._ticks]
        self._upper_boundary = max(prices)
        self._lower_boundary = min(prices)

        # 하한 바운더리 히스토리 업데이트
        self._lower_boundary_history.append(self._lower_boundary)

        # 바운더리 확립/변경 로그
        if prev_lower is None or prev_upper is None:
            logger.info(
                f"[boundary] 바운더리 확립: "
                f"{self._lower_boundary:,}원 ~ {self._upper_boundary:,}원 "
                f"({len(self._ticks)}틱)"
            )
        elif (
            self._lower_boundary != prev_lower
            or self._upper_boundary != prev_upper
        ):
            logger.debug(
                f"[boundary] 바운더리 갱신: "
                f"{self._lower_boundary:,}원 ~ {self._upper_boundary:,}원"
            )

    def _reset_boundary(self) -> None:
        """
        바운더리만 리셋 (breach 발생 시)

        틱 데이터는 유지, 바운더리 및 DIP 상태만 초기화.

        내부 메서드, lock 내부에서만 호출됨
        """
        self._upper_boundary = None
        self._lower_boundary = None
        self._range_qualified_at = None
        self._dip_fired = False
        logger.debug("[boundary] 바운더리 리셋 (새 윈도우 시작)")

    def reset_for_new_cycle(self) -> None:
        """사이클 간 리셋 (DIP/바운더리만 초기화, 틱/breach 유지)"""
        with self._lock:
            self._reset_boundary()
            logger.debug("[boundary] 사이클 리셋 (DIP/바운더리 초기화)")

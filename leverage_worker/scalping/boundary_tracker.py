"""
동적 바운더리 추적 및 DIP 패턴 감지

틱 기반 마이크로 윈도우로 상/하단 바운더리를 추적하고,
DIP 패턴 (3연속 하락 + 하단 근접 + 매수세 우위)을 감지하여 매수 시그널 생성.
"""

import threading
from collections import deque
from typing import Deque, Optional

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
        boundary_window_ticks: int = 15,
        max_boundary_breaches: int = 5,
        min_consecutive_downticks: int = 3,
        dip_margin_pct: float = 0.1,
        lower_history_size: int = 3,
    ) -> None:
        """
        Args:
            boundary_window_ticks: 바운더리 계산에 사용할 틱 수
            max_boundary_breaches: 최대 허용 breach 횟수
            min_consecutive_downticks: DIP 판정 최소 연속 하락틱
            dip_margin_pct: 하단 근접 판정 마진 (바운더리 range의 %)
            lower_history_size: 하한 바운더리 히스토리 크기
        """
        self._boundary_window_ticks = boundary_window_ticks
        self._max_boundary_breaches = max_boundary_breaches
        self._min_consecutive_downticks = min_consecutive_downticks
        self._dip_margin_pct = dip_margin_pct
        self._lower_history_size = lower_history_size

        # 틱 데이터 (가격만 저장, 시간 불필요)
        self._ticks: Deque[int] = deque(maxlen=boundary_window_ticks)

        # 바운더리 상태
        self._upper_boundary: Optional[int] = None
        self._lower_boundary: Optional[int] = None
        self._breach_count: int = 0

        # DIP 패턴 추적
        self._consecutive_downticks: int = 0
        self._buy_pressure: float = 0.0  # 윈도우 내 누적 상승폭
        self._sell_pressure: float = 0.0  # 윈도우 내 누적 하락폭

        # NEW: 하한 바운더리 히스토리
        self._lower_boundary_history: Deque[int] = deque(maxlen=lower_history_size)

        self._lock = threading.Lock()

    def add_tick(self, price: int) -> Optional[str]:
        """
        새 틱 추가 및 바운더리 갱신

        Args:
            price: 현재가

        Returns:
            "BREACH": 하단 바운더리 이탈 (바운더리 리셋됨)
            "DIP": DIP 조건 만족 (매수 시그널)
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

            # 2. 틱 추가 (이전 가격 저장)
            prev_price = self._ticks[-1] if self._ticks else price
            self._ticks.append(price)

            # 3. 바운더리 재계산 (윈도우 full일 때만)
            if len(self._ticks) >= self._boundary_window_ticks:
                self._update_boundary()

            # 4. 연속 하락틱 추적 및 pressure 계산
            if price < prev_price:
                self._consecutive_downticks += 1
                self._sell_pressure += (prev_price - price)
            elif price > prev_price:
                self._consecutive_downticks = 0  # 상승 시 리셋
                self._buy_pressure += (price - prev_price)
            # else: 동일 가격 → downticks 유지, pressure 변화 없음

            # 5. DIP 조건 확인
            if self._check_dip_condition(price):
                logger.info(
                    f"[boundary] DIP 감지: {price:,}원 "
                    f"(하단 {self._lower_boundary:,}원, "
                    f"연속 하락 {self._consecutive_downticks}틱, "
                    f"매수세 {self._buy_pressure:.0f} > "
                    f"매도세 {self._sell_pressure:.0f})"
                )
                return "DIP"

            return None

    def get_buy_price(self) -> Optional[int]:
        """
        DIP 감지 시 매수 가격 반환

        Returns:
            하단 바운더리 가격, 미확립 시 None
        """
        with self._lock:
            return self._lower_boundary

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
        if len(self._ticks) < self._boundary_window_ticks:
            return

        prev_lower = self._lower_boundary
        prev_upper = self._upper_boundary

        self._upper_boundary = max(self._ticks)
        self._lower_boundary = min(self._ticks)

        # NEW: 하한 바운더리 히스토리 업데이트
        if self._lower_boundary is not None:
            self._lower_boundary_history.append(self._lower_boundary)

        # 바운더리 확립/변경 로그 (초기 또는 변경 시)
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

    def _check_dip_condition(self, price: int) -> bool:
        """
        DIP 조건 확인

        조건:
        1. 바운더리 확립됨
        2. N연속 하락틱 (default 3)
        3. 가격이 하단 근접 (lower + margin 이내)
        4. 매수세 > 매도세

        Args:
            price: 현재가

        Returns:
            True if all conditions met, False otherwise

        내부 메서드, lock 내부에서만 호출됨
        """
        # 조건 1: 바운더리 확립
        if self._lower_boundary is None or self._upper_boundary is None:
            return False

        # 조건 2: N연속 하락틱
        if self._consecutive_downticks < self._min_consecutive_downticks:
            return False

        # 조건 3: 하단 근접
        boundary_range = self._upper_boundary - self._lower_boundary
        if boundary_range <= 0:
            # 모든 틱이 동일 가격 → margin=0, 정확히 동일해야 통과
            margin = 0
        else:
            margin = int(boundary_range * self._dip_margin_pct)

        if price > (self._lower_boundary + margin):
            return False

        # 조건 4: 매수세 > 매도세
        if self._buy_pressure <= self._sell_pressure:
            return False

        # NEW: 조건 5: 하한 바운더리 하락 추세 체크
        if self._is_lower_boundary_falling():
            logger.debug(
                f"[boundary] 하한 하락 추세 감지 → DIP 거부 "
                f"(history: {list(self._lower_boundary_history)})"
            )
            return False

        return True

    def _is_lower_boundary_falling(self) -> bool:
        """
        하한 바운더리가 지속적으로 하락 중인지 체크

        Returns:
            True if 최근 N개가 모두 연속 하락 (예: [100, 99, 98])
            False otherwise

        내부 메서드, lock 내부에서만 호출됨
        """
        if len(self._lower_boundary_history) < self._lower_history_size:
            return False  # 히스토리 부족

        # 최근 N개가 연속 하락인지 체크
        history = list(self._lower_boundary_history)
        for i in range(len(history) - 1):
            if history[i] <= history[i + 1]:
                return False  # 하락 아님 또는 동일

        return True  # 모두 연속 하락

    def _reset_boundary(self) -> None:
        """
        바운더리만 리셋 (breach 발생 시)

        틱 데이터는 유지 (deque maxlen으로 자동 관리),
        바운더리 및 패턴 추적 상태만 초기화.

        내부 메서드, lock 내부에서만 호출됨
        """
        self._upper_boundary = None
        self._lower_boundary = None
        self._consecutive_downticks = 0
        self._buy_pressure = 0.0
        self._sell_pressure = 0.0
        logger.debug("[boundary] 바운더리 리셋 (새 윈도우 시작)")

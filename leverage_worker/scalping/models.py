"""
스캘핑 전략 데이터 모델
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict


class ScalpingState(Enum):
    """스캘핑 실행기 상태"""

    IDLE = "idle"  # 시그널 대기
    MONITORING = "monitoring"  # 시그널 활성, P10 모니터링
    BUY_PENDING = "buy_pending"  # 지정가 매수 주문 중
    POSITION_HELD = "position_held"  # 매수 체결, 매도 대기
    SELL_PENDING = "sell_pending"  # 지정가 매도 주문 중
    STOP_LOSS = "stop_loss"  # SL 도달, 시장가 매도
    COOLDOWN = "cooldown"  # 매도 완료, 사이클 대기


@dataclass
class ScalpingConfig:
    """스캘핑 실행 설정"""

    # 변동폭 윈도우
    window_seconds: int = 10  # n초 변동폭 윈도우
    adaptive_window: bool = False  # 동적 윈도우 활성화 여부
    max_window_seconds: int = 60  # 동적 모드 최대 윈도우
    min_window_seconds: int = 15  # 동적 모드 최소 윈도우
    high_volatility_threshold: float = 0.005  # 변동폭 > 0.5% → 최소 윈도우
    low_volatility_threshold: float = 0.002  # 변동폭 < 0.2% → 최대 윈도우

    # 스캘핑 실행
    percentile_threshold: float = 10.0  # 하단 N% 기준 (P10 매수)
    sell_profit_pct: float = 0.001  # +0.1% 매도 목표
    position_size: int = 1  # 매수 수량

    # 시그널 수명 (반복 매매 기간)
    stop_loss_pct: float = 0.01  # 시그널가 대비 1% 하락 시 종료
    take_profit_pct: float = 0.003  # 시그널가 대비 0.3% 상승 시 종료
    max_signal_minutes: int = 60  # 시그널 유효 시간 (분)

    # 주문 관리
    buy_timeout_seconds: int = 10  # 매수 미체결 타임아웃
    max_cycles: int = 20  # 시그널당 최대 반복 횟수
    cooldown_seconds: float = 1.0  # 사이클 간 쿨다운
    min_ticks_for_trade: int = 10  # 최소 tick 수

    # 추세 필터
    trend_filter_enabled: bool = True       # 추세 필터 활성화 여부
    min_uptick_ratio: float = 0.4           # 상승틱 비율 최소값 (0.0~1.0)

    # === Dynamic Boundary Tracking ===
    boundary_window_ticks: int = 15         # 바운더리 계산 틱 수
    max_boundary_breaches: int = 5          # 최대 이탈 횟수
    min_consecutive_downticks: int = 3      # DEPRECATED (하위 호환용)
    dip_margin_pct: float = 0.1             # DEPRECATED (하위 호환용)
    lower_history_size: int = 3             # 하한 바운더리 히스토리 크기
    min_boundary_range_pct: float = 0.001   # 바운더리 최소 range (0.1%)
    max_boundary_range_pct: float = 0.0015  # 바운더리 최대 range (0.15%)
    boundary_hold_seconds: float = 1.0      # range 유지 시간 (초)

    @classmethod
    def from_params(cls, params: Dict[str, Any]) -> "ScalpingConfig":
        """전략 파라미터 딕셔너리로부터 생성"""
        return cls(
            window_seconds=params.get("window_seconds", 10),
            adaptive_window=params.get("adaptive_window", False),
            max_window_seconds=params.get("max_window_seconds", 60),
            min_window_seconds=params.get("min_window_seconds", 15),
            high_volatility_threshold=params.get("high_volatility_threshold", 0.005),
            low_volatility_threshold=params.get("low_volatility_threshold", 0.002),
            percentile_threshold=params.get("percentile_threshold", 10.0),
            sell_profit_pct=params.get("sell_profit_pct", 0.001),
            position_size=params.get("position_size", 1),
            stop_loss_pct=params.get("stop_loss_pct", 0.01),
            take_profit_pct=params.get("take_profit_pct", 0.003),
            max_signal_minutes=params.get("max_signal_minutes", 60),
            buy_timeout_seconds=params.get("buy_timeout_seconds", 10),
            max_cycles=params.get("max_cycles", 20),
            cooldown_seconds=params.get("cooldown_seconds", 1.0),
            min_ticks_for_trade=params.get("min_ticks_for_trade", 10),
            trend_filter_enabled=params.get("trend_filter_enabled", True),
            min_uptick_ratio=params.get("min_uptick_ratio", 0.4),
            # Boundary tracking params
            boundary_window_ticks=params.get("boundary_window_ticks", 15),
            max_boundary_breaches=params.get("max_boundary_breaches", 5),
            min_consecutive_downticks=params.get("min_consecutive_downticks", 3),
            dip_margin_pct=params.get("dip_margin_pct", 0.1),
            lower_history_size=params.get("lower_history_size", 3),
            min_boundary_range_pct=params.get("min_boundary_range_pct", 0.001),
            max_boundary_range_pct=params.get("max_boundary_range_pct", 0.0015),
            boundary_hold_seconds=params.get("boundary_hold_seconds", 1.0),
        )


@dataclass
class ScalpingSignalContext:
    """활성 시그널의 수명 관리 컨텍스트"""

    signal_price: int  # 시그널 발생 시점 가격 (매수 상한)
    signal_time: datetime  # 시그널 발생 시간
    tp_pct: float  # 시그널 수명 TP (시그널가 대비)
    sl_pct: float  # 시그널 수명 SL (시그널가 대비)
    timeout_minutes: int  # 시그널 수명 타임아웃

    # 실행 통계
    cycle_count: int = 0  # 완료된 매매 사이클 수
    total_pnl: int = 0  # 누적 손익 (원)
    total_trades: int = 0  # 총 매매 횟수

    @property
    def tp_price(self) -> int:
        """시그널 수명 TP 가격"""
        return int(self.signal_price * (1 + self.tp_pct))

    @property
    def sl_price(self) -> int:
        """시그널 수명 SL 가격"""
        return int(self.signal_price * (1 - self.sl_pct))

    def is_expired(self, current_time: datetime, current_price: int) -> tuple[bool, str]:
        """
        시그널 수명 만료 여부 확인

        Returns:
            (만료 여부, 사유)
        """
        # TP 도달
        if current_price >= self.tp_price:
            return True, f"시그널 TP 도달: {current_price:,} >= {self.tp_price:,}"

        # SL 도달
        if current_price <= self.sl_price:
            return True, f"시그널 SL 도달: {current_price:,} <= {self.sl_price:,}"

        # 타임아웃
        elapsed_minutes = (current_time - self.signal_time).total_seconds() / 60
        if elapsed_minutes >= self.timeout_minutes:
            return True, f"시그널 타임아웃: {elapsed_minutes:.0f}분 >= {self.timeout_minutes}분"

        return False, ""

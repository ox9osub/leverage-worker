"""
전략 기본 클래스 모듈

BaseStrategy 추상 클래스 및 관련 데이터 구조
- StrategyContext: 전략 실행에 필요한 컨텍스트
- TradingSignal: 매매 시그널
- BaseStrategy: 전략 추상 클래스
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from leverage_worker.data.minute_candle_repository import MinuteCandle as OHLCV
from leverage_worker.trading.broker import Position


class SignalType(Enum):
    """시그널 타입"""
    HOLD = "hold"      # 대기 (아무것도 하지 않음)
    BUY = "buy"        # 매수
    SELL = "sell"      # 매도


@dataclass
class TradingSignal:
    """
    매매 시그널

    전략에서 생성하는 매매 결정
    """
    signal_type: SignalType
    stock_code: str
    quantity: int = 0
    reason: str = ""
    confidence: float = 1.0  # 0.0 ~ 1.0 (신뢰도)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def hold(cls, stock_code: str, reason: str = "") -> "TradingSignal":
        """대기 시그널 생성"""
        return cls(
            signal_type=SignalType.HOLD,
            stock_code=stock_code,
            reason=reason,
        )

    @classmethod
    def buy(
        cls,
        stock_code: str,
        quantity: int,
        reason: str = "",
        confidence: float = 1.0,
    ) -> "TradingSignal":
        """매수 시그널 생성"""
        return cls(
            signal_type=SignalType.BUY,
            stock_code=stock_code,
            quantity=quantity,
            reason=reason,
            confidence=confidence,
        )

    @classmethod
    def sell(
        cls,
        stock_code: str,
        quantity: int,
        reason: str = "",
        confidence: float = 1.0,
    ) -> "TradingSignal":
        """매도 시그널 생성"""
        return cls(
            signal_type=SignalType.SELL,
            stock_code=stock_code,
            quantity=quantity,
            reason=reason,
            confidence=confidence,
        )

    @property
    def is_hold(self) -> bool:
        return self.signal_type == SignalType.HOLD

    @property
    def is_buy(self) -> bool:
        return self.signal_type == SignalType.BUY

    @property
    def is_sell(self) -> bool:
        return self.signal_type == SignalType.SELL


@dataclass
class PriceValidationResult:
    """가격 데이터 검증 결과"""
    is_valid: bool
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class StrategyContext:
    """
    전략 실행 컨텍스트

    전략 generate_signal() 메서드에 전달되는 정보
    """
    stock_code: str
    stock_name: str
    current_price: int
    current_time: datetime

    # 가격 히스토리 (분봉 OHLCV)
    price_history: List[OHLCV]

    # 현재 포지션 (보유 중이면 Position, 아니면 None)
    position: Optional[Position]

    # 오늘 해당 종목의 거래 횟수
    today_trade_count: int = 0

    # 추가 정보
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def has_position(self) -> bool:
        """포지션 보유 여부"""
        return self.position is not None and self.position.quantity > 0

    @property
    def position_quantity(self) -> int:
        """보유 수량"""
        if self.position:
            return self.position.quantity
        return 0

    @property
    def avg_price(self) -> float:
        """평균 매입가"""
        if self.position:
            return self.position.avg_price
        return 0.0

    @property
    def profit_rate(self) -> float:
        """현재 수익률 (%)"""
        if self.position and self.position.avg_price > 0:
            return ((self.current_price - self.position.avg_price) /
                    self.position.avg_price) * 100
        return 0.0

    def get_recent_prices(self, count: int = 10) -> List[int]:
        """최근 N개 종가 리스트"""
        return [p.close_price for p in self.price_history[-count:]]

    def get_recent_volumes(self, count: int = 10) -> List[int]:
        """최근 N개 거래량 리스트"""
        return [p.volume for p in self.price_history[-count:]]

    def get_sma(self, period: int) -> Optional[float]:
        """단순 이동평균 계산"""
        prices = self.get_recent_prices(period)
        if len(prices) < period:
            return None
        return sum(prices) / len(prices)

    def has_sufficient_data(self, min_required: int) -> bool:
        """
        충분한 가격 데이터가 있는지 확인

        Args:
            min_required: 최소 필요 데이터 개수

        Returns:
            True if sufficient data, False otherwise
        """
        return len(self.price_history) >= min_required

    def validate_price_data(self, spike_threshold: float = 0.30) -> PriceValidationResult:
        """
        가격 데이터 유효성 검증

        Args:
            spike_threshold: 급등락 임계값 (기본 30%)

        Returns:
            PriceValidationResult 객체
        """
        warnings = []
        errors = []

        # 현재가 검증
        if self.current_price <= 0:
            errors.append(f"Invalid current price: {self.current_price}")

        # 가격 히스토리 검증
        if not self.price_history:
            errors.append("Empty price history")
        else:
            # 가격 스파이크 검증
            for i in range(1, len(self.price_history)):
                prev_close = self.price_history[i - 1].close_price
                curr_close = self.price_history[i].close_price

                if prev_close > 0:
                    change_rate = abs(curr_close - prev_close) / prev_close
                    if change_rate > spike_threshold:
                        warnings.append(
                            f"Price spike detected: {prev_close} -> {curr_close} "
                            f"({change_rate * 100:.1f}%)"
                        )

            # 현재가와 최신 히스토리 가격 비교
            latest_close = self.price_history[-1].close_price
            if latest_close > 0:
                current_change = abs(self.current_price - latest_close) / latest_close
                if current_change > spike_threshold:
                    warnings.append(
                        f"Current price spike: history={latest_close}, "
                        f"current={self.current_price} ({current_change * 100:.1f}%)"
                    )

            # 0 또는 음수 가격 검증
            for i, candle in enumerate(self.price_history):
                if candle.close_price <= 0:
                    errors.append(f"Invalid close price at index {i}: {candle.close_price}")
                if candle.high_price < candle.low_price:
                    warnings.append(
                        f"High < Low at index {i}: high={candle.high_price}, low={candle.low_price}"
                    )

        is_valid = len(errors) == 0
        return PriceValidationResult(is_valid=is_valid, warnings=warnings, errors=errors)


class BaseStrategy(ABC):
    """
    전략 추상 기본 클래스

    모든 전략은 이 클래스를 상속받아야 함
    """

    # 기본 최소 데이터 요구량 (하위 클래스에서 오버라이드 가능)
    MIN_DATA_REQUIRED = 20

    def __init__(self, name: str, params: Optional[Dict[str, Any]] = None):
        """
        Args:
            name: 전략 이름 (고유 식별자)
            params: 전략 파라미터
        """
        self._name = name
        self._params = params or {}

    @property
    def name(self) -> str:
        """전략 이름"""
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        """전략 파라미터"""
        return self._params

    @property
    def min_data_required(self) -> int:
        """
        전략이 필요로 하는 최소 데이터 개수

        하위 클래스에서 MIN_DATA_REQUIRED를 오버라이드하거나
        이 프로퍼티를 오버라이드하여 동적으로 계산 가능
        """
        return self.MIN_DATA_REQUIRED

    def get_param(self, key: str, default: Any = None) -> Any:
        """파라미터 값 조회"""
        return self._params.get(key, default)

    def can_generate_signal(self, context: StrategyContext) -> bool:
        """
        시그널 생성이 가능한지 확인

        Args:
            context: 전략 실행 컨텍스트

        Returns:
            True if signal generation is possible
        """
        # 충분한 데이터가 있는지 확인
        if not context.has_sufficient_data(self.min_data_required):
            return False

        # 가격 데이터 유효성 확인
        validation = context.validate_price_data()
        if not validation.is_valid:
            return False

        return True

    @abstractmethod
    def generate_signal(self, context: StrategyContext) -> TradingSignal:
        """
        매매 시그널 생성 (추상 메서드)

        현재 포지션 상태와 시장 데이터를 기반으로 매매 시그널 생성

        - 미보유 상태: BUY 또는 HOLD 반환
        - 보유 상태: SELL 또는 HOLD 반환

        Args:
            context: 전략 실행 컨텍스트

        Returns:
            TradingSignal 객체
        """
        pass

    def on_entry(self, context: StrategyContext, signal: TradingSignal) -> None:
        """
        진입 시 콜백 (선택적 오버라이드)

        매수 주문 실행 직전에 호출됨
        """
        pass

    def on_exit(self, context: StrategyContext, signal: TradingSignal) -> None:
        """
        청산 시 콜백 (선택적 오버라이드)

        매도 주문 실행 직전에 호출됨
        """
        pass

    def on_fill(
        self,
        context: StrategyContext,
        signal: TradingSignal,
        filled_price: int,
        filled_qty: int,
    ) -> None:
        """
        체결 시 콜백 (선택적 오버라이드)

        주문 체결 확인 후 호출됨
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self._name}', params={self._params})"

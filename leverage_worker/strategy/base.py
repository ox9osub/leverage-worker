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


class BaseStrategy(ABC):
    """
    전략 추상 기본 클래스

    모든 전략은 이 클래스를 상속받아야 함
    """

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

    def get_param(self, key: str, default: Any = None) -> Any:
        """파라미터 값 조회"""
        return self._params.get(key, default)

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

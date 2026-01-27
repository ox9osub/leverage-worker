"""
당일 청산 관리 모듈

15:19에 모든 포지션을 시장가로 자동 청산
- 청산 오케스트레이션
- 에러 처리
- 결과 리포팅
"""

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from leverage_worker.notification.slack_notifier import SlackNotifier
from leverage_worker.trading.order_manager import OrderManager
from leverage_worker.trading.position_manager import ManagedPosition, PositionManager
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LiquidationResult:
    """청산 실행 결과"""

    started_at: datetime
    completed_at: datetime
    total_positions: int
    successful_orders: int
    failed_orders: int
    partial_fills: Dict[str, int] = field(default_factory=dict)  # stock_code -> unfilled_qty
    errors: List[Tuple[str, str]] = field(default_factory=list)  # (stock_code, error_message)
    total_liquidation_value: int = 0
    total_pnl: int = 0  # 청산된 포지션의 손익 합계


class DailyLiquidationManager:
    """
    당일 청산 관리자

    15:19에 모든 포지션을 시장가로 자동 청산
    """

    def __init__(
        self,
        order_manager: OrderManager,
        position_manager: PositionManager,
        slack_notifier: SlackNotifier,
    ):
        self._order_manager = order_manager
        self._position_manager = position_manager
        self._slack = slack_notifier

        # 청산 상태
        self._in_progress = False

        self._logger = get_logger(__name__)

    def execute_liquidation(self) -> LiquidationResult:
        """
        당일 청산 실행

        Process:
        1. 청산 모드 활성화 (신규 매수 차단)
        2. 모든 미체결 주문 취소
        3. 모든 포지션 조회
        4. 포지션별 시장가 매도 (병렬 처리, 최대 10개)
        5. 체결 확인 (20초 대기)
        6. 부분 체결 처리
        7. 포지션 동기화
        8. 청산 모드 해제
        9. 결과 리포팅
        """
        started_at = datetime.now()
        self._in_progress = True

        try:
            # 1. 청산 모드 활성화 (신규 매수 차단)
            self._order_manager.enable_liquidation_mode()
            logger.info("[Liquidation] Mode enabled - Buy orders blocked")

            # 2. 모든 미체결 주문 취소
            cancelled = self._order_manager.cancel_all_pending()
            logger.info(f"[Liquidation] Cancelled {cancelled} pending orders")

            # 3. 모든 포지션 조회
            positions = self._position_manager.get_all_positions()
            total_positions = len(positions)

            if total_positions == 0:
                logger.info("[Liquidation] No positions to liquidate")
                return LiquidationResult(
                    started_at=started_at,
                    completed_at=datetime.now(),
                    total_positions=0,
                    successful_orders=0,
                    failed_orders=0,
                )

            logger.info(f"[Liquidation] Found {total_positions} positions to liquidate")
            for stock_code, position in positions.items():
                logger.info(
                    f"  - {position.stock_name}({stock_code}): "
                    f"{position.quantity}주 @ {position.avg_price:,.0f}원"
                )

            # Slack 시작 알림 (상세)
            self._send_start_notification(positions)

            # 4. 병렬 매도 (최대 10개 동시)
            order_results = self._execute_parallel_sell_orders(positions)

            # 5. 체결 확인 (20초 대기)
            self._wait_for_fills(20)

            # 6. 결과 집계
            result = self._collect_results(started_at, positions, order_results)

            # 7. 포지션 동기화 (브로커 잔고와 비교)
            logger.info("[Liquidation] Syncing positions with broker...")
            self._position_manager.sync_with_broker()

            # 8. 청산 모드 해제
            self._order_manager.disable_liquidation_mode()
            logger.info("[Liquidation] Mode disabled - Buy orders re-enabled")

            return result

        except Exception as e:
            logger.error(f"[Liquidation] Fatal error: {e}", exc_info=True)
            # 에러 발생 시에도 청산 모드 해제
            self._order_manager.disable_liquidation_mode()
            raise

        finally:
            self._in_progress = False

    def _send_start_notification(self, positions: Dict[str, ManagedPosition]) -> None:
        """청산 시작 알림 전송"""
        total_eval = sum(p.eval_amount for p in positions.values())
        position_count = len(positions)

        message = (
            f"⏰ [청산시작] 15:19 당일 청산\n"
            f"보유 포지션: {position_count}개\n"
            f"총 평가금액: {total_eval:,}원\n"
            f"전략: 시장가 즉시 청산"
        )
        self._slack.send_message(message)

    def _execute_parallel_sell_orders(
        self, positions: Dict[str, ManagedPosition]
    ) -> Dict[str, Optional[str]]:
        """
        병렬 시장가 매도 주문

        Returns:
            stock_code -> order_id (실패 시 None)
        """
        order_results: Dict[str, Optional[str]] = {}

        # 병렬 실행 (최대 10개 동시)
        with ThreadPoolExecutor(max_workers=min(10, len(positions))) as executor:
            future_to_stock = {
                executor.submit(
                    self._place_sell_order_with_retry, stock_code, position
                ): stock_code
                for stock_code, position in positions.items()
            }

            for future in future_to_stock:
                stock_code = future_to_stock[future]
                try:
                    order_id = future.result()
                    order_results[stock_code] = order_id
                except Exception as e:
                    logger.error(f"[Liquidation] Sell order error [{stock_code}]: {e}")
                    order_results[stock_code] = None

        return order_results

    def _place_sell_order_with_retry(
        self, stock_code: str, position: ManagedPosition, max_retries: int = 2
    ) -> Optional[str]:
        """
        재시도가 포함된 시장가 매도 주문

        Args:
            stock_code: 종목 코드
            position: 포지션 정보
            max_retries: 최대 재시도 횟수

        Returns:
            order_id (실패 시 None)
        """
        for attempt in range(max_retries + 1):
            try:
                order_id = self._order_manager.place_sell_order(
                    stock_code=stock_code,
                    stock_name=position.stock_name,
                    quantity=position.quantity,
                    strategy_name=position.strategy_name or "liquidation",
                )

                if order_id:
                    logger.info(
                        f"[Liquidation] Sell order placed: {position.stock_name}({stock_code}) "
                        f"{position.quantity}주 (order_id: {order_id})"
                    )
                    return order_id
                else:
                    logger.warning(
                        f"[Liquidation] Sell order failed: {position.stock_name}({stock_code}) "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )

            except Exception as e:
                logger.error(
                    f"[Liquidation] Sell order exception [{stock_code}] "
                    f"(attempt {attempt + 1}/{max_retries + 1}): {e}"
                )

            # 재시도 전 대기
            if attempt < max_retries:
                time.sleep(0.5)

        # 모든 재시도 실패
        logger.error(
            f"[Liquidation] Sell order failed after {max_retries + 1} attempts: "
            f"{position.stock_name}({stock_code})"
        )
        return None

    def _wait_for_fills(self, duration_seconds: int) -> None:
        """
        체결 확인 대기

        Args:
            duration_seconds: 대기 시간 (초)
        """
        logger.info(f"[Liquidation] Waiting {duration_seconds}s for order fills...")

        for i in range(duration_seconds):
            # check_fills는 OrderManager가 주기적으로 호출하므로
            # 여기서는 단순히 대기만 함
            time.sleep(1)

            # 진행 상황 로그 (5초마다)
            if (i + 1) % 5 == 0:
                logger.debug(f"[Liquidation] Fill check: {i + 1}/{duration_seconds}s")

    def _collect_results(
        self,
        started_at: datetime,
        positions: Dict[str, ManagedPosition],
        order_results: Dict[str, Optional[str]],
    ) -> LiquidationResult:
        """
        청산 결과 집계

        Args:
            started_at: 청산 시작 시각
            positions: 원래 포지션
            order_results: 주문 결과 (stock_code -> order_id)

        Returns:
            LiquidationResult
        """
        completed_at = datetime.now()
        successful_orders = sum(1 for order_id in order_results.values() if order_id is not None)
        failed_orders = len(order_results) - successful_orders

        # 에러 목록
        errors: List[Tuple[str, str]] = []
        for stock_code, order_id in order_results.items():
            if order_id is None:
                position = positions[stock_code]
                errors.append((stock_code, f"{position.stock_name} - Order placement failed"))

        # 부분 체결 확인 (현재 남은 포지션 확인)
        remaining_positions = self._position_manager.get_all_positions()
        partial_fills: Dict[str, int] = {}

        for stock_code in positions.keys():
            if stock_code in remaining_positions:
                remaining_qty = remaining_positions[stock_code].quantity
                if remaining_qty > 0:
                    partial_fills[stock_code] = remaining_qty
                    logger.warning(
                        f"[Liquidation] Partial fill detected: "
                        f"{remaining_positions[stock_code].stock_name}({stock_code}) "
                        f"- {remaining_qty}주 미체결"
                    )

        # 총 청산 금액 및 손익 계산 (체결된 것만)
        total_liquidation_value = 0
        total_pnl = 0

        for stock_code, position in positions.items():
            if stock_code not in remaining_positions:
                # 전량 청산됨
                total_liquidation_value += position.eval_amount
                total_pnl += position.profit_loss
            elif stock_code in partial_fills:
                # 부분 청산됨
                filled_qty = position.quantity - partial_fills[stock_code]
                if filled_qty > 0:
                    filled_value = position.current_price * filled_qty
                    filled_pnl = int((position.current_price - position.avg_price) * filled_qty)
                    total_liquidation_value += filled_value
                    total_pnl += filled_pnl

        result = LiquidationResult(
            started_at=started_at,
            completed_at=completed_at,
            total_positions=len(positions),
            successful_orders=successful_orders,
            failed_orders=failed_orders,
            partial_fills=partial_fills,
            errors=errors,
            total_liquidation_value=total_liquidation_value,
            total_pnl=total_pnl,
        )

        # 결과 로깅
        duration = (completed_at - started_at).total_seconds()
        logger.info(f"[Liquidation] Completed in {duration:.1f}s")
        logger.info(f"[Liquidation] Success: {successful_orders}/{len(positions)}")
        logger.info(f"[Liquidation] Failed: {failed_orders}")
        logger.info(f"[Liquidation] Partial fills: {len(partial_fills)}")
        logger.info(f"[Liquidation] Total value: {total_liquidation_value:,}원")
        logger.info(f"[Liquidation] Total PnL: {total_pnl:+,}원")

        return result

    @property
    def is_in_progress(self) -> bool:
        """청산 진행 중 여부"""
        return self._in_progress

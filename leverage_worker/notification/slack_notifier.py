"""
Slack 알림 모듈

Slack 알림 전송 (두 가지 방식 지원)
- Webhook URL 방식
- Bot Token + Channel 방식
- 매매 알림
- 오류 알림
- 일일 리포트
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


class SlackNotifier:
    """
    Slack 알림 클래스

    - 웹훅 또는 Bot Token을 통한 메시지 전송
    - Block Kit 포맷 지원
    """

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        token: Optional[str] = None,
        channel: Optional[str] = None,
        is_paper_mode: bool = False,
    ):
        """
        Args:
            webhook_url: Slack Webhook URL (None이면 token/channel 사용)
            token: Slack Bot Token (xoxb-...)
            channel: Slack Channel ID (C...)
            is_paper_mode: 모의투자 모드 여부 (True면 메시지에 [모의] 표시)
        """
        self._webhook_url = webhook_url
        self._token = token
        self._channel = channel
        self._is_paper_mode = is_paper_mode

        # 시그널 집계용 (종목-전략 pair별 첫 시그널만 즉시 전송)
        self._signal_count: Dict[Tuple[str, str], int] = {}  # (stock_code, strategy) -> 발생 횟수
        self._signal_first_sent: Set[Tuple[str, str]] = set()  # 첫 알림 전송된 pair
        self._signal_stock_names: Dict[str, str] = {}  # stock_code -> stock_name 매핑

        # 우선순위: token+channel > webhook_url
        self._use_token = token is not None and channel is not None
        self._enabled = self._use_token or (webhook_url is not None and len(webhook_url) > 0)

        if self._enabled:
            method = "Bot Token" if self._use_token else "Webhook"
            mode_str = "모의투자" if is_paper_mode else "실전투자"
            logger.info(f"SlackNotifier enabled ({method}, {mode_str})")
        else:
            logger.info("SlackNotifier disabled (no credentials)")

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def _get_mode_prefix(self) -> str:
        """모의투자 모드면 [모의] prefix 반환"""
        return "[모의] " if self._is_paper_mode else ""

    def send_message(self, text: str, blocks: Optional[List[Dict]] = None) -> bool:
        """
        메시지 전송

        Args:
            text: 기본 텍스트 (fallback)
            blocks: Block Kit 블록 (선택)

        Returns:
            성공 여부
        """
        if not self._enabled:
            logger.debug(f"Slack disabled, message not sent: {text[:50]}...")
            return False

        if self._use_token:
            return self._send_via_token(text, blocks)
        else:
            return self._send_via_webhook(text, blocks)

    def _send_via_webhook(self, text: str, blocks: Optional[List[Dict]] = None) -> bool:
        """Webhook 방식으로 메시지 전송"""
        payload = {"text": text}
        if blocks:
            payload["blocks"] = blocks

        try:
            response = requests.post(
                self._webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                logger.debug("Slack message sent successfully (webhook)")
                return True
            else:
                logger.error(
                    f"Slack webhook error: {response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"Slack webhook send error: {e}")
            return False

    def _send_via_token(self, text: str, blocks: Optional[List[Dict]] = None) -> bool:
        """Bot Token 방식으로 메시지 전송 (chat.postMessage API)"""
        payload = {
            "channel": self._channel,
            "text": text,
        }
        if blocks:
            payload["blocks"] = blocks

        try:
            response = requests.post(
                "https://slack.com/api/chat.postMessage",
                json=payload,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )

            result = response.json()
            if result.get("ok"):
                logger.debug("Slack message sent successfully (token)")
                return True
            else:
                logger.error(f"Slack API error: {result.get('error', 'unknown')}")
                return False

        except Exception as e:
            logger.error(f"Slack token send error: {e}")
            return False

    def notify_buy(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: int,
        strategy_name: str,
        reason: str = "",
        strategy_win_rate: Optional[float] = None,
    ) -> bool:
        """매수 주문 접수 알림"""
        total_amount = quantity * price
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 전략명에 승률 포함
        strategy_display = strategy_name
        if strategy_win_rate is not None:
            strategy_display = f"{strategy_name}({strategy_win_rate:.1f}%)"

        lines = [
            f"{self._get_mode_prefix()}[매수주문]",
            f"{stock_name}({stock_code}) / {quantity}주 / {price:,}원 / {total_amount:,}원",
            f"전략: {strategy_display}" + (f" / {reason}" if reason else ""),
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def notify_sell(
        self,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: int,
        profit_loss: int,
        profit_rate: float,
        strategy_name: str,
        reason: str = "",
        strategy_win_rate: Optional[float] = None,
    ) -> bool:
        """매도 주문 접수 알림"""
        total_amount = quantity * price
        sign = "+" if profit_loss >= 0 else ""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 전략명에 승률 포함
        strategy_display = strategy_name
        if strategy_win_rate is not None:
            strategy_display = f"{strategy_name}({strategy_win_rate:.1f}%)"

        lines = [
            f"{self._get_mode_prefix()}[매도주문]",
            f"{stock_name}({stock_code}) / {quantity}주 / {price:,}원 / {total_amount:,}원",
            f"손익: {sign}{profit_loss:,}원 ({sign}{profit_rate:.2f}%)",
            f"전략: {strategy_display}" + (f" / {reason}" if reason else ""),
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def notify_signal(
        self,
        signal_type: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: int,
        strategy_name: str,
        reason: str = "",
        strategy_win_rate: Optional[float] = None,
    ) -> bool:
        """시그널 발생 알림 (매수/매도 시그널)

        첫 번째 시그널만 즉시 전송, 이후 시그널은 카운트만 증가.
        send_signal_summary()로 요약 전송 가능.
        """
        key = (stock_code, strategy_name)

        # 카운트 증가 및 종목명 저장
        self._signal_count[key] = self._signal_count.get(key, 0) + 1
        self._signal_stock_names[stock_code] = stock_name

        # 첫 시그널이 아니면 전송하지 않고 성공 반환
        if key in self._signal_first_sent:
            logger.debug(
                f"Signal skipped (already sent): {stock_name} {strategy_name} "
                f"(count: {self._signal_count[key]})"
            )
            return True

        # 첫 시그널 전송 표시
        self._signal_first_sent.add(key)

        is_buy = signal_type.upper() == "BUY"
        signal_text = "매수시그널" if is_buy else "매도시그널"
        total_amount = quantity * price
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 전략명에 승률 포함
        strategy_display = strategy_name
        if strategy_win_rate is not None:
            strategy_display = f"{strategy_name}({strategy_win_rate:.1f}%)"

        lines = [
            f"{self._get_mode_prefix()}[{signal_text}]",
            f"{stock_name}({stock_code}) / {quantity}주 / {price:,}원 / {total_amount:,}원",
            f"전략: {strategy_display}" + (f" / {reason}" if reason else ""),
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def notify_fill(
        self,
        fill_type: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: int,
        strategy_name: str,
        profit_loss: int = 0,
        profit_rate: float = 0.0,
        strategy_win_rate: Optional[float] = None,
    ) -> bool:
        """체결 완료 알림"""
        is_buy = fill_type.upper() == "BUY"
        fill_text = "매수체결" if is_buy else "매도체결"
        total_amount = quantity * price
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 전략명에 승률 포함
        strategy_display = strategy_name
        if strategy_win_rate is not None:
            strategy_display = f"{strategy_name}({strategy_win_rate:.1f}%)"

        lines = [
            f"{self._get_mode_prefix()}[{fill_text}]",
            f"{stock_name}({stock_code}) / {quantity}주 / {price:,}원 / {total_amount:,}원",
        ]

        # 매도 체결 시 손익 정보 추가
        if not is_buy:
            sign = "+" if profit_loss >= 0 else ""
            lines.append(f"손익: {sign}{profit_loss:,}원 ({sign}{profit_rate:.2f}%)")

        lines.append(f"전략: {strategy_display}")
        lines.append(timestamp)

        return self.send_message("\n".join(lines))

    def notify_error(self, title: str, message: str) -> bool:
        """오류 알림"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{self._get_mode_prefix()}[오류]",
            f"{title}",
            message,
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def notify_start(self, mode: str, stocks_count: int) -> bool:
        """프로그램 시작 알림"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{self._get_mode_prefix()}[시작]",
            f"자동매매 시작 / 모드: {mode} / 관리종목: {stocks_count}개",
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def notify_stop(self, reason: str = "정상 종료") -> bool:
        """프로그램 종료 알림"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{self._get_mode_prefix()}[종료]",
            f"자동매매 종료 / 사유: {reason}",
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def send_market_open_notification(self) -> bool:
        """
        정규장 시작 알림 전송

        Returns:
            성공 여부
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{self._get_mode_prefix()}[정규장 시작]",
            "09:00 정규장이 시작되었습니다.",
            timestamp,
        ]

        return self.send_message("\n".join(lines))

    def send_daily_report(self, report: Dict[str, Any]) -> bool:
        """
        일일 리포트 전송

        Args:
            report: 리포트 데이터 딕셔너리

        Returns:
            성공 여부
        """
        date = report.get("date", datetime.now().strftime("%Y-%m-%d"))
        total_trades = report.get("total_trades", 0)
        buy_trades = report.get("buy_trades", 0)
        sell_trades = report.get("sell_trades", 0)
        realized_pnl = report.get("realized_pnl", 0)
        win_trades = report.get("win_trades", 0)
        lose_trades = report.get("lose_trades", 0)

        win_rate = 0
        if sell_trades > 0:
            win_rate = (win_trades / sell_trades) * 100

        pnl_sign = "+" if realized_pnl >= 0 else ""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            f"{self._get_mode_prefix()}[일일리포트] {date}",
            f"총거래: {total_trades}건 (매수 {buy_trades} / 매도 {sell_trades})",
            f"실현손익: {pnl_sign}{realized_pnl:,}원",
            f"승률: {win_rate:.1f}% ({win_trades}승 {lose_trades}패)",
        ]

        # 개별 거래 내역 (있으면)
        trades = report.get("trades", [])
        if trades:
            lines.append("---")
            for t in trades[:10]:  # 최대 10건
                sign = "+" if t.get("profit_loss", 0) >= 0 else ""
                lines.append(
                    f"{t['stock_name']} {t['side']} {t['quantity']}주 @ {t['price']:,}원"
                )

            if len(trades) > 10:
                lines.append(f"... 외 {len(trades) - 10}건")

        # 보유 포지션 (있으면)
        positions = report.get("positions", [])
        if positions:
            lines.append("---")
            lines.append("[보유포지션]")
            total_eval_pnl = 0
            for p in positions[:10]:  # 최대 10건
                pnl = p.get("profit_loss", 0)
                pnl_rate = p.get("profit_loss_rate", 0.0)
                total_eval_pnl += pnl
                pnl_sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"{p.get('stock_name', p.get('stock_code', '???'))} "
                    f"{p.get('quantity', 0)}주 | "
                    f"평가손익: {pnl_sign}{pnl:,}원 ({pnl_sign}{pnl_rate:.2f}%)"
                )

            if len(positions) > 10:
                lines.append(f"... 외 {len(positions) - 10}건")

            eval_sign = "+" if total_eval_pnl >= 0 else ""
            lines.append(f"총 평가손익: {eval_sign}{total_eval_pnl:,}원")

        lines.append(timestamp)

        return self.send_message("\n".join(lines))

    def reset_signal_history(self) -> None:
        """시그널 기록 초기화 (다음 날 준비)"""
        self._signal_count.clear()
        self._signal_first_sent.clear()
        self._signal_stock_names.clear()
        logger.debug("Signal history reset")

    def reset_signal_for_key(self, stock_code: str, strategy_name: str) -> None:
        """특정 종목-전략의 시그널 기록 초기화 (체결 완료 시 호출)"""
        key = (stock_code, strategy_name)
        if key in self._signal_first_sent:
            self._signal_first_sent.discard(key)
        if key in self._signal_count:
            del self._signal_count[key]
        logger.debug(f"Signal history reset for {stock_code}/{strategy_name}")

    def send_signal_summary(self) -> bool:
        """
        시그널 요약 전송

        저장된 시그널 중 2회 이상 발생한 것들의 요약을 전송.
        전송 후 시그널 기록 초기화.

        Returns:
            성공 여부 (시그널이 없거나 추가 시그널이 없으면 True)
        """
        if not self._signal_count:
            logger.debug("No signals to summarize")
            return True

        # 2회 이상 발생한 시그널만 필터링 (첫 번째는 이미 전송됨)
        summary_items = []
        for (stock_code, strategy), count in sorted(self._signal_count.items()):
            if count > 1:
                stock_name = self._signal_stock_names.get(stock_code, stock_code)
                summary_items.append(f"{strategy}: {count}회 ({stock_name})")

        # 추가 시그널이 없으면 전송하지 않음
        if not summary_items:
            logger.debug("No additional signals to summarize (all first-time only)")
            self.reset_signal_history()
            return True

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"{self._get_mode_prefix()}[시그널요약]"]
        lines.extend(summary_items)
        lines.append(timestamp)

        result = self.send_message("\n".join(lines))
        self.reset_signal_history()
        return result

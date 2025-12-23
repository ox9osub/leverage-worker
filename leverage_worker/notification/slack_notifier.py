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
from typing import Any, Dict, List, Optional

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
    ):
        """
        Args:
            webhook_url: Slack Webhook URL (None이면 token/channel 사용)
            token: Slack Bot Token (xoxb-...)
            channel: Slack Channel ID (C...)
        """
        self._webhook_url = webhook_url
        self._token = token
        self._channel = channel

        # 우선순위: token+channel > webhook_url
        self._use_token = token is not None and channel is not None
        self._enabled = self._use_token or (webhook_url is not None and len(webhook_url) > 0)

        if self._enabled:
            method = "Bot Token" if self._use_token else "Webhook"
            logger.info(f"SlackNotifier enabled ({method})")
        else:
            logger.info("SlackNotifier disabled (no credentials)")

    @property
    def is_enabled(self) -> bool:
        return self._enabled

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
    ) -> bool:
        """매수 알림"""
        text = f"[매수] {stock_name}({stock_code}) {quantity}주 @ {price:,}원"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📈 매수 주문",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*종목*\n{stock_name} ({stock_code})"},
                    {"type": "mrkdwn", "text": f"*수량*\n{quantity}주"},
                    {"type": "mrkdwn", "text": f"*가격*\n{price:,}원"},
                    {"type": "mrkdwn", "text": f"*전략*\n{strategy_name}"},
                ]
            },
        ]

        if reason:
            blocks.append({
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"💡 {reason}"}
                ]
            })

        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            ]
        })

        return self.send_message(text, blocks)

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
    ) -> bool:
        """매도 알림"""
        emoji = "🟢" if profit_loss >= 0 else "🔴"
        sign = "+" if profit_loss >= 0 else ""

        text = (
            f"[매도] {stock_name}({stock_code}) {quantity}주 @ {price:,}원 "
            f"({sign}{profit_loss:,}원, {sign}{profit_rate:.2f}%)"
        )

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "📉 매도 주문",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*종목*\n{stock_name} ({stock_code})"},
                    {"type": "mrkdwn", "text": f"*수량*\n{quantity}주"},
                    {"type": "mrkdwn", "text": f"*가격*\n{price:,}원"},
                    {"type": "mrkdwn", "text": f"*전략*\n{strategy_name}"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*손익*\n{emoji} {sign}{profit_loss:,}원"},
                    {"type": "mrkdwn", "text": f"*수익률*\n{emoji} {sign}{profit_rate:.2f}%"},
                ]
            },
        ]

        if reason:
            blocks.append({
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"💡 {reason}"}
                ]
            })

        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            ]
        })

        return self.send_message(text, blocks)

    def notify_error(self, title: str, message: str) -> bool:
        """오류 알림"""
        text = f"[오류] {title}: {message}"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ 오류 발생",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{title}*\n```{message}```",
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            },
        ]

        return self.send_message(text, blocks)

    def notify_start(self, mode: str, stocks_count: int) -> bool:
        """프로그램 시작 알림"""
        text = f"[시작] 자동매매 시작 (모드: {mode}, 종목: {stocks_count}개)"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚀 자동매매 시작",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*모드*\n{mode}"},
                    {"type": "mrkdwn", "text": f"*관리 종목*\n{stocks_count}개"},
                ]
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            },
        ]

        return self.send_message(text, blocks)

    def notify_stop(self, reason: str = "정상 종료") -> bool:
        """프로그램 종료 알림"""
        text = f"[종료] 자동매매 종료 ({reason})"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🛑 자동매매 종료",
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*사유*: {reason}",
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            },
        ]

        return self.send_message(text, blocks)

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

        pnl_emoji = "🟢" if realized_pnl >= 0 else "🔴"
        pnl_sign = "+" if realized_pnl >= 0 else ""

        text = f"[일일리포트] {date} - 거래 {total_trades}건, 손익 {pnl_sign}{realized_pnl:,}원"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"📊 일일 거래 리포트 ({date})",
                    "emoji": True,
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*총 거래*\n{total_trades}건"},
                    {"type": "mrkdwn", "text": f"*매수/매도*\n{buy_trades}/{sell_trades}건"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*실현손익*\n{pnl_emoji} {pnl_sign}{realized_pnl:,}원"},
                    {"type": "mrkdwn", "text": f"*승률*\n{win_rate:.1f}% ({win_trades}승 {lose_trades}패)"},
                ]
            },
        ]

        # 개별 거래 내역 (있으면)
        trades = report.get("trades", [])
        if trades:
            blocks.append({"type": "divider"})

            trade_text = "*거래 내역*\n"
            for t in trades[:10]:  # 최대 10건
                emoji = "🟢" if t.get("profit_loss", 0) >= 0 else "🔴"
                trade_text += (
                    f"{emoji} {t['stock_name']} {t['side']} "
                    f"{t['quantity']}주 @ {t['price']:,}원\n"
                )

            if len(trades) > 10:
                trade_text += f"... 외 {len(trades) - 10}건\n"

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": trade_text}
            })

        blocks.append({
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"⏰ 생성: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            ]
        })

        return self.send_message(text, blocks)

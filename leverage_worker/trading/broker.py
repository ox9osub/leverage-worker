"""
KIS API 브로커 모듈

한국투자증권 Open API 래퍼
- 현재가 조회
- 잔고 조회
- 시장가 주문 (매수/매도)
- 주문 취소
- 체결 조회
"""

import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any, Tuple

from leverage_worker.core.session_manager import SessionManager, APIResp
from leverage_worker.utils.logger import get_logger
from leverage_worker.utils.time_utils import get_today_date_str

logger = get_logger(__name__)


class OrderSide(Enum):
    """주문 방향"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """주문 상태"""
    PENDING = "pending"       # 접수 대기
    SUBMITTED = "submitted"   # 접수됨
    PARTIAL = "partial"       # 부분 체결
    FILLED = "filled"         # 전량 체결
    CANCELLED = "cancelled"   # 취소됨
    REJECTED = "rejected"     # 거부됨


@dataclass
class StockPrice:
    """주식 현재가 정보"""
    stock_code: str
    stock_name: str
    current_price: int
    prev_close: int
    change: int
    change_rate: float
    open_price: int
    high_price: int
    low_price: int
    volume: int
    trade_amount: int  # 거래대금


@dataclass
class Position:
    """보유 종목 정보"""
    stock_code: str
    stock_name: str
    quantity: int
    avg_price: float
    current_price: int
    eval_amount: int      # 평가금액
    profit_loss: int      # 평가손익
    profit_rate: float    # 수익률


@dataclass
class OrderResult:
    """주문 결과"""
    success: bool
    order_id: Optional[str]
    message: str
    stock_code: str
    side: OrderSide
    quantity: int
    price: int
    order_branch: Optional[str] = None  # 지점번호 (정정/취소에 필요)


@dataclass
class OrderInfo:
    """주문 정보 (체결 조회용)"""
    order_id: str
    order_no: str            # 원주문번호
    branch_no: str           # 지점번호
    stock_code: str
    stock_name: str
    side: OrderSide
    order_qty: int
    order_price: int
    filled_qty: int
    filled_price: int
    status: OrderStatus
    order_time: str


# KIS API 인증 관련 에러 코드 (토큰 재발급으로 복구 가능)
_AUTH_ERROR_CODES = frozenset({
    "OPSQ0013",   # 유효하지 않은 토큰
    "EGW00123",   # 기간이 만료된 token
    "EGW00121",   # 유효하지 않은 token
})

# 일시적 에러 코드 (단순 재시도로 복구 가능, WebSocket fallback 패턴)
_TRANSIENT_ERROR_CODES = frozenset({
    "OPSQ2000",   # INPUT INVALID_CHECK_ACNO (일시적 계좌 검증 실패)
})

# 일시적 에러 재시도 설정
_TRANSIENT_RETRY_DELAY = 1.0  # 재시도 간격 (초)
_TRANSIENT_MAX_RETRIES = 3    # 최대 재시도 횟수


class KISBroker:
    """
    한국투자증권 API 브로커

    - 현재가 조회
    - 잔고 조회
    - 시장가 주문
    - 주문 취소
    - 체결 조회
    """

    def __init__(self, session: SessionManager):
        self._session = session
        self._account_no, self._account_prod = session.get_account_info()
        logger.info(f"KISBroker initialized. Account: {self._account_no}")

    @staticmethod
    def _is_auth_error(res: APIResp) -> bool:
        """인증 관련 에러 여부 확인 (토큰 재발급 필요)"""
        return res.get_error_code() in _AUTH_ERROR_CODES

    @staticmethod
    def _is_transient_error(res: APIResp) -> bool:
        """일시적 에러 여부 확인 (단순 재시도로 복구 가능)"""
        return res.get_error_code() in _TRANSIENT_ERROR_CODES

    def get_current_price(self, stock_code: str) -> Optional[StockPrice]:
        """
        현재가 조회

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            StockPrice 객체 또는 None
        """
        api_url = "/uapi/domestic-stock/v1/quotations/inquire-price"
        tr_id = "FHKST01010100"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # KRX
            "FID_INPUT_ISCD": stock_code,
        }

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        if not res.is_ok():
            res.print_error(api_url)
            return None

        try:
            body = res.get_body()
            output = body.output

            # output이 dict인 경우와 객체인 경우 모두 처리
            def get_value(key: str, default=0):
                if isinstance(output, dict):
                    return output.get(key, default)
                return getattr(output, key, default)

            return StockPrice(
                stock_code=stock_code,
                stock_name=get_value("hts_kor_isnm", ""),
                current_price=int(get_value("stck_prpr", 0)),
                prev_close=int(get_value("stck_sdpr", 0)),
                change=int(get_value("prdy_vrss", 0)),
                change_rate=float(get_value("prdy_ctrt", 0)),
                open_price=int(get_value("stck_oprc", 0)),
                high_price=int(get_value("stck_hgpr", 0)),
                low_price=int(get_value("stck_lwpr", 0)),
                volume=int(get_value("acml_vol", 0)),
                trade_amount=int(get_value("acml_tr_pbmn", 0)),
            )
        except Exception as e:
            logger.error(f"Failed to parse price response: {e}")
            return None

    def get_asking_price(self, stock_code: str) -> Optional[int]:
        """
        매도호가1 (최우선 매도가) 조회

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            매도호가1 가격 또는 None
        """
        api_url = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        tr_id = "FHKST01010200"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # KRX
            "FID_INPUT_ISCD": stock_code,
        }

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        if not res.is_ok():
            res.print_error(api_url)
            return None

        try:
            body = res.get_body()
            output1 = body.output1

            # output1이 dict인 경우와 객체인 경우 모두 처리
            if isinstance(output1, dict):
                askp1 = output1.get("askp1", "0")
            else:
                askp1 = getattr(output1, "askp1", "0")

            return int(askp1)
        except Exception as e:
            logger.error(f"Failed to parse asking price response: {e}")
            return None

    def get_bidding_price(self, stock_code: str) -> Optional[int]:
        """
        매수호가1 (최우선 매수가) 조회

        Args:
            stock_code: 종목코드 (6자리)

        Returns:
            매수호가1 가격 또는 None
        """
        api_url = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        tr_id = "FHKST01010200"

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # KRX
            "FID_INPUT_ISCD": stock_code,
        }

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        if not res.is_ok():
            res.print_error(api_url)
            return None

        try:
            body = res.get_body()
            output1 = body.output1

            if isinstance(output1, dict):
                bidp1 = output1.get("bidp1", "0")
            else:
                bidp1 = getattr(output1, "bidp1", "0")

            return int(bidp1)
        except Exception as e:
            logger.error(f"Failed to parse bidding price response: {e}")
            return None

    def get_balance(self) -> Tuple[List[Position], Dict[str, Any]]:
        """
        잔고 조회

        Returns:
            (포지션 리스트, 계좌 요약) 튜플
        """
        api_url = "/uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "TTTC8434R"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "01",  # 대출일별 (모의투자 호환)
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",  # 전일매매포함
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        positions = []
        summary = {}

        if not res.is_ok():
            logger.error(
                f"get_balance failed - CANO: '{self._account_no}', "
                f"ACNT_PRDT_CD: '{self._account_prod}', "
                f"Error: {res.get_error_code()} {res.get_error_message()}"
            )

            # 인증 에러 시 토큰 재발급 후 1회 재시도
            if self._is_auth_error(res):
                logger.warning("Auth error detected. Attempting token refresh and retry...")
                if self._session.force_reauthenticate():
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if not res.is_ok():
                        logger.error(
                            f"get_balance still failed after token refresh: "
                            f"{res.get_error_code()} {res.get_error_message()}"
                        )
                        return positions, summary
                    logger.info("get_balance succeeded after token refresh")
                else:
                    logger.error("Token re-authentication failed")
                    return positions, summary
            # 일시적 에러 시 WebSocket fallback 패턴으로 재시도
            elif self._is_transient_error(res):
                for retry in range(_TRANSIENT_MAX_RETRIES):
                    logger.warning(
                        f"Transient error {res.get_error_code()}. "
                        f"Retry {retry + 1}/{_TRANSIENT_MAX_RETRIES} after {_TRANSIENT_RETRY_DELAY}s"
                    )
                    time.sleep(_TRANSIENT_RETRY_DELAY)
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if res.is_ok():
                        logger.info(f"get_balance: Transient error resolved after {retry + 1} retries")
                        break
                else:
                    logger.error(
                        f"get_balance: Transient error persisted after {_TRANSIENT_MAX_RETRIES} retries"
                    )
                    return positions, summary
            else:
                return positions, summary

        try:
            body = res.get_body()

            # dict/객체 모두 처리하는 헬퍼 함수
            def get_value(obj, key: str, default=0):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            # 보유 종목 (output1)
            if hasattr(body, "output1"):
                for item in body.output1:
                    qty = int(get_value(item, "hldg_qty", 0))
                    if qty <= 0:
                        continue

                    positions.append(Position(
                        stock_code=get_value(item, "pdno", ""),
                        stock_name=get_value(item, "prdt_name", ""),
                        quantity=qty,
                        avg_price=float(get_value(item, "pchs_avg_pric", 0)),
                        current_price=int(get_value(item, "prpr", 0)),
                        eval_amount=int(get_value(item, "evlu_amt", 0)),
                        profit_loss=int(get_value(item, "evlu_pfls_amt", 0)),
                        profit_rate=float(get_value(item, "evlu_pfls_rt", 0)),
                    ))

            # 계좌 요약 (output2)
            if hasattr(body, "output2") and body.output2:
                out2 = body.output2[0] if isinstance(body.output2, list) else body.output2
                summary = {
                    "total_eval": int(get_value(out2, "tot_evlu_amt", 0)),
                    "deposit": int(get_value(out2, "dnca_tot_amt", 0)),
                    "total_profit_loss": int(get_value(out2, "evlu_pfls_smtl_amt", 0)),
                }

        except Exception as e:
            logger.error(f"Failed to parse balance response: {e}")

        return positions, summary

    def place_market_order(
        self,
        stock_code: str,
        side: OrderSide,
        quantity: int,
    ) -> OrderResult:
        """
        시장가 주문

        Args:
            stock_code: 종목코드
            side: 매수/매도
            quantity: 수량

        Returns:
            OrderResult 객체
        """
        api_url = "/uapi/domestic-stock/v1/trading/order-cash"

        # TR ID 설정 (매수/매도)
        if side == OrderSide.BUY:
            tr_id = "TTTC0802U"
        else:
            tr_id = "TTTC0801U"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "PDNO": stock_code,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",  # 시장가는 0
            "EXCG_ID_DVSN_CD": "KRX",
        }

        # 매도 시 추가 파라미터
        if side == OrderSide.SELL:
            params["SLL_TYPE"] = "01"  # 일반매도

        res = self._session.url_fetch(api_url, tr_id, params=params, post_flag=True)
        self._session.smart_sleep()

        if not res.is_ok():
            error_msg = res.get_error_message()
            logger.error(f"Order failed: {side.value} {stock_code} x {quantity} - {error_msg}")
            return OrderResult(
                success=False,
                order_id=None,
                message=error_msg,
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=0,
            )

        try:
            body = res.get_body()
            output = body.output

            # output이 딕셔너리인 경우와 namedtuple인 경우 모두 처리
            if isinstance(output, dict):
                order_id = output.get("ODNO", "")
                order_time = output.get("ORD_TMD", "")
            else:
                order_id = getattr(output, "ODNO", "")
                order_time = getattr(output, "ORD_TMD", "")

            logger.info(
                f"Order placed: {side.value} {stock_code} x {quantity} - "
                f"OrderID: {order_id}, Time: {order_time}"
            )

            return OrderResult(
                success=True,
                order_id=order_id,
                message="Order placed successfully",
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=0,  # 시장가
            )

        except Exception as e:
            logger.error(f"Failed to parse order response: {e}")
            return OrderResult(
                success=False,
                order_id=None,
                message=str(e),
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=0,
            )

    def place_limit_order(
        self,
        stock_code: str,
        side: OrderSide,
        quantity: int,
        price: int,
    ) -> OrderResult:
        """
        지정가 주문

        Args:
            stock_code: 종목코드
            side: 매수/매도
            quantity: 수량
            price: 지정가

        Returns:
            OrderResult 객체
        """
        api_url = "/uapi/domestic-stock/v1/trading/order-cash"

        # TR ID 설정 (매수/매도)
        if side == OrderSide.BUY:
            tr_id = "TTTC0802U"
        else:
            tr_id = "TTTC0801U"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "PDNO": stock_code,
            "ORD_DVSN": "00",  # 지정가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price),  # 지정가
            "EXCG_ID_DVSN_CD": "KRX",
        }

        # 매도 시 추가 파라미터
        if side == OrderSide.SELL:
            params["SLL_TYPE"] = "01"  # 일반매도

        res = self._session.url_fetch(api_url, tr_id, params=params, post_flag=True)
        self._session.smart_sleep()

        if not res.is_ok():
            error_msg = res.get_error_message()
            logger.error(f"Limit order failed: {side.value} {stock_code} x {quantity} @ {price} - {error_msg}")
            return OrderResult(
                success=False,
                order_id=None,
                message=error_msg,
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=price,
            )

        try:
            body = res.get_body()
            output = body.output

            # output이 딕셔너리인 경우와 namedtuple인 경우 모두 처리
            if isinstance(output, dict):
                order_id = output.get("ODNO", "")
                order_time = output.get("ORD_TMD", "")
                order_branch = output.get("KRX_FWDG_ORD_ORGNO", "")
            else:
                order_id = getattr(output, "ODNO", "")
                order_time = getattr(output, "ORD_TMD", "")
                order_branch = getattr(output, "KRX_FWDG_ORD_ORGNO", "")

            logger.info(
                f"Limit order placed: {side.value} {stock_code} x {quantity} @ {price} - "
                f"OrderID: {order_id}, Time: {order_time}"
            )

            return OrderResult(
                success=True,
                order_id=order_id,
                message="Order placed successfully",
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=price,
                order_branch=order_branch,
            )

        except Exception as e:
            logger.error(f"Failed to parse order response: {e}")
            return OrderResult(
                success=False,
                order_id=None,
                message=str(e),
                stock_code=stock_code,
                side=side,
                quantity=quantity,
                price=price,
            )

    def cancel_order(
        self,
        order_id: str,
        order_branch: str,
        quantity: int,
    ) -> bool:
        """
        주문 취소

        Args:
            order_id: 주문번호
            order_branch: 지점번호
            quantity: 취소 수량

        Returns:
            성공 여부
        """
        api_url = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
        tr_id = "TTTC0803U"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "KRX_FWDG_ORD_ORGNO": order_branch,
            "ORGN_ODNO": order_id,
            "ORD_DVSN": "00",
            "RVSE_CNCL_DVSN_CD": "02",  # 취소
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y",  # 전량 취소
            "EXCG_ID_DVSN_CD": "KRX",
        }

        res = self._session.url_fetch(api_url, tr_id, params=params, post_flag=True)
        self._session.smart_sleep()

        if not res.is_ok():
            res.print_error(api_url)
            return False

        logger.info(f"Order cancelled: {order_id}")
        return True

    def modify_order(
        self,
        order_id: str,
        order_branch: str,
        quantity: int,
        new_price: int,
    ) -> Optional[str]:
        """
        주문 정정 (가격/수량 변경)

        Args:
            order_id: 주문번호
            order_branch: 지점번호
            quantity: 정정 수량
            new_price: 정정 가격

        Returns:
            새 주문번호 (성공 시) 또는 None (실패 시)
        """
        api_url = "/uapi/domestic-stock/v1/trading/order-rvsecncl"
        tr_id = "TTTC0803U"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "KRX_FWDG_ORD_ORGNO": order_branch,
            "ORGN_ODNO": order_id,
            "ORD_DVSN": "00",  # 지정가
            "RVSE_CNCL_DVSN_CD": "01",  # 정정
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(new_price),  # 정정 가격
            "QTY_ALL_ORD_YN": "N",  # 수량 지정
            "EXCG_ID_DVSN_CD": "KRX",
        }

        res = self._session.url_fetch(api_url, tr_id, params=params, post_flag=True)
        self._session.smart_sleep()

        if not res.is_ok():
            error_msg = res.get_error_message()
            # 체결 진행 중 정정 실패는 자연스러운 상황 (race condition)
            logger.warning(f"Order modify failed: {order_id} - {error_msg}")
            return None

        try:
            body = res.get_body()
            output = body.output

            # output이 딕셔너리인 경우와 namedtuple인 경우 모두 처리
            if isinstance(output, dict):
                new_order_id = output.get("ODNO", "")
            else:
                new_order_id = getattr(output, "ODNO", "")

            logger.info(
                f"Order modified: {order_id} -> {new_order_id}, "
                f"{quantity}주 @ {new_price:,}원"
            )
            return new_order_id if new_order_id else order_id

        except Exception as e:
            logger.error(f"Failed to parse modify order response: {e}")
            # 응답 파싱 실패해도 정정은 성공했으므로 원래 ID 반환
            return order_id

    def get_pending_orders(self) -> List[OrderInfo]:
        """
        미체결 주문 조회

        Returns:
            미체결 주문 리스트
        """
        return self._get_orders(filled_only=False)

    def get_order_status(
        self,
        order_id: str,
        stock_code: str = "",
        order_qty: int = 0,
        side: Optional[OrderSide] = None,
    ) -> Tuple[int, int]:
        """
        주문의 체결/미체결 수량 조회

        stock_code + side가 전달되면 get_balance 기반으로 체결 확인 (PRIMARY).
        전달되지 않으면 _get_orders 사용 (레거시 호환).

        Args:
            order_id: 주문번호
            stock_code: 종목코드
            order_qty: 주문수량
            side: 매수/매도 구분

        Returns:
            (체결수량, 미체결수량) 튜플
        """
        # stock_code + side가 있으면 get_balance 기반 (모의투자 호환)
        if stock_code and side:
            return self._get_order_status_from_balance(stock_code, order_qty, side)

        # 레거시: _get_orders 사용
        orders = self._get_orders(all_orders=True)
        for order in orders:
            if order.order_id == order_id:
                unfilled_qty = order.order_qty - order.filled_qty
                return (order.filled_qty, unfilled_qty)

        logger.warning(f"Order not found: {order_id}")
        return (0, 0)

    def _get_order_status_from_balance(
        self,
        stock_code: str,
        order_qty: int,
        side: OrderSide,
    ) -> Tuple[int, int]:
        """
        잔고 조회 기반 체결 상태 추론 (inquire-daily-ccld 실패 시 fallback)

        Args:
            stock_code: 종목코드
            order_qty: 주문수량
            side: 매수/매도 구분

        Returns:
            (체결수량, 미체결수량) 튜플
        """
        positions, _ = self.get_balance()
        held_qty = 0
        for pos in positions:
            if pos.stock_code == stock_code:
                held_qty = pos.quantity
                break

        if side == OrderSide.BUY:
            # 잔고에 있으면 체결된 것으로 판단
            if held_qty > 0:
                filled = min(held_qty, order_qty)
                unfilled = max(0, order_qty - held_qty)
                logger.info(
                    f"[balance fallback] BUY: held={held_qty}, "
                    f"filled={filled}, unfilled={unfilled}"
                )
                return (filled, unfilled)
            return (0, order_qty)
        else:  # SELL
            # 잔고에 없으면 매도 체결된 것으로 판단
            if held_qty == 0:
                logger.info(
                    f"[balance fallback] SELL: no position → fully filled"
                )
                return (order_qty, 0)
            sold = max(0, order_qty - held_qty)
            remaining = order_qty - sold
            logger.info(
                f"[balance fallback] SELL: held={held_qty}, "
                f"sold={sold}, remaining={remaining}"
            )
            return (sold, remaining)

    def get_today_orders(self) -> List[OrderInfo]:
        """
        당일 전체 주문 조회

        Returns:
            당일 주문 리스트
        """
        return self._get_orders(filled_only=False, all_orders=True)

    def _get_orders(
        self,
        filled_only: bool = False,
        all_orders: bool = False,
    ) -> List[OrderInfo]:
        """
        주문/체결 조회 내부 함수

        Args:
            filled_only: 체결 건만 조회
            all_orders: 전체 조회 (False면 미체결만)

        Returns:
            주문 리스트
        """
        api_url = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        tr_id = "TTTC8001R"

        today = get_today_date_str()

        # 체결구분: 00=전체, 01=체결, 02=미체결
        if all_orders:
            ccld_dvsn = "00"
        elif filled_only:
            ccld_dvsn = "01"
        else:
            ccld_dvsn = "02"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "INQR_STRT_DT": today,
            "INQR_END_DT": today,
            "SLL_BUY_DVSN_CD": "00",  # 전체
            "INQR_DVSN": "00",  # 역순
            "PDNO": "",
            "CCLD_DVSN": ccld_dvsn,
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",  # 전체
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        orders = []

        if not res.is_ok():
            logger.error(
                f"_get_orders failed - CANO: '{self._account_no}', "
                f"ACNT_PRDT_CD: '{self._account_prod}', "
                f"Error: {res.get_error_code()} {res.get_error_message()}"
            )

            # 인증 에러 시 토큰 재발급 후 1회 재시도
            if self._is_auth_error(res):
                logger.warning("_get_orders: Auth error detected. Attempting token refresh and retry...")
                if self._session.force_reauthenticate():
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if not res.is_ok():
                        logger.error(
                            f"_get_orders still failed after token refresh: "
                            f"{res.get_error_code()} {res.get_error_message()}"
                        )
                        return orders
                    logger.info("_get_orders succeeded after token refresh")
                else:
                    logger.error("Token re-authentication failed")
                    return orders
            # 일시적 에러 시 WebSocket fallback 패턴으로 재시도
            elif self._is_transient_error(res):
                for retry in range(_TRANSIENT_MAX_RETRIES):
                    logger.warning(
                        f"_get_orders: Transient error {res.get_error_code()}. "
                        f"Retry {retry + 1}/{_TRANSIENT_MAX_RETRIES} after {_TRANSIENT_RETRY_DELAY}s"
                    )
                    time.sleep(_TRANSIENT_RETRY_DELAY)
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if res.is_ok():
                        logger.info(f"_get_orders: Transient error resolved after {retry + 1} retries")
                        break
                else:
                    logger.error(
                        f"_get_orders: Transient error persisted after {_TRANSIENT_MAX_RETRIES} retries"
                    )
                    return orders
            else:
                res.print_error(api_url)
                return orders

        try:
            body = res.get_body()

            # dict/객체 모두 처리하는 헬퍼 함수
            def get_value(obj, key: str, default=0):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            if hasattr(body, "output1"):
                for item in body.output1:
                    order_qty = int(get_value(item, "ord_qty", 0) or 0)
                    filled_qty = int(get_value(item, "tot_ccld_qty", 0) or 0)

                    # 상태 판단
                    if filled_qty == 0:
                        status = OrderStatus.PENDING
                    elif filled_qty < order_qty:
                        status = OrderStatus.PARTIAL
                    else:
                        status = OrderStatus.FILLED

                    # 매수/매도 구분
                    sll_buy = get_value(item, "sll_buy_dvsn_cd", "")
                    side = OrderSide.BUY if sll_buy == "02" else OrderSide.SELL

                    orders.append(OrderInfo(
                        order_id=get_value(item, "odno", ""),
                        order_no=get_value(item, "orgn_odno", ""),
                        branch_no=get_value(item, "ord_gno_brno", ""),
                        stock_code=get_value(item, "pdno", ""),
                        stock_name=get_value(item, "prdt_name", ""),
                        side=side,
                        order_qty=order_qty,
                        order_price=int(get_value(item, "ord_unpr", 0) or 0),
                        filled_qty=filled_qty,
                        filled_price=int(get_value(item, "avg_prvs", 0) or 0),
                        status=status,
                        order_time=get_value(item, "ord_tmd", ""),
                    ))

        except Exception as e:
            logger.error(f"Failed to parse orders response: {e}")

        return orders

    def cancel_all_pending_orders(self) -> int:
        """
        모든 미체결 주문 취소

        Returns:
            취소된 주문 수
        """
        pending_orders = self.get_pending_orders()
        cancelled = 0

        for order in pending_orders:
            if order.status in (OrderStatus.PENDING, OrderStatus.PARTIAL):
                remaining = order.order_qty - order.filled_qty
                if remaining > 0:
                    if self.cancel_order(order.order_id, order.branch_no, remaining):
                        cancelled += 1

        return cancelled

    def get_buyable_quantity(self, stock_code: str, current_price: int = 0) -> tuple[int, int]:
        """
        매수 가능 수량 및 최대매수금액 조회

        최대매수금액(max_buy_amt)을 현재가로 나누어 MTS와 동일한 방식으로
        매수 가능 수량을 계산합니다.

        Args:
            stock_code: 종목코드
            current_price: 현재가 (0이면 API의 가능수량계산단가 사용)

        Returns:
            (매수 가능 수량, 최대매수금액) 튜플
        """
        api_url = "/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        tr_id = "TTTC8908R"

        params = {
            "CANO": self._account_no,
            "ACNT_PRDT_CD": self._account_prod,
            "PDNO": stock_code,
            "ORD_UNPR": "0",
            "ORD_DVSN": "01",  # 시장가
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N",
        }

        logger.debug(
            f"[get_buyable_quantity] API 호출 - "
            f"TR_ID: {tr_id}, CANO: {self._account_no}, "
            f"ACNT_PRDT_CD: {self._account_prod}, PDNO: {stock_code}"
        )

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        if not res.is_ok():
            # 인증 에러 시 토큰 재발급 후 1회 재시도
            if self._is_auth_error(res):
                logger.warning(
                    "get_buyable_quantity: Auth error detected. "
                    "Attempting token refresh and retry..."
                )
                if self._session.force_reauthenticate():
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if not res.is_ok():
                        logger.error(
                            f"get_buyable_quantity still failed after token refresh: "
                            f"{res.get_error_code()} {res.get_error_message()}"
                        )
                        return 0, 0
                    logger.info("get_buyable_quantity succeeded after token refresh")
                else:
                    logger.error("Token re-authentication failed")
                    return 0, 0
            # 일시적 에러 시 WebSocket fallback 패턴으로 재시도
            elif self._is_transient_error(res):
                for retry in range(_TRANSIENT_MAX_RETRIES):
                    logger.warning(
                        f"get_buyable_quantity: Transient error {res.get_error_code()}. "
                        f"Retry {retry + 1}/{_TRANSIENT_MAX_RETRIES} after {_TRANSIENT_RETRY_DELAY}s"
                    )
                    time.sleep(_TRANSIENT_RETRY_DELAY)
                    res = self._session.url_fetch(api_url, tr_id, params=params)
                    self._session.smart_sleep()
                    if res.is_ok():
                        logger.info(
                            f"get_buyable_quantity: Transient error resolved after {retry + 1} retries"
                        )
                        break
                else:
                    logger.error(
                        f"get_buyable_quantity: Transient error persisted after "
                        f"{_TRANSIENT_MAX_RETRIES} retries"
                    )
                    return 0, 0
            else:
                res.print_error(api_url)
                return 0, 0

        try:
            body = res.get_body()
            output = body.output

            # output이 딕셔너리인 경우와 namedtuple인 경우 모두 처리
            def get_value(key: str, default: int = 0) -> int:
                if isinstance(output, dict):
                    return int(output.get(key, default) or default)
                return int(getattr(output, key, default) or default)

            ord_psbl_cash = get_value("ord_psbl_cash")  # 주문가능현금
            psbl_qty_calc_unpr = get_value("psbl_qty_calc_unpr")  # 가능수량계산단가
            nrcvb_buy_qty = get_value("nrcvb_buy_qty")  # 미수없는매수수량

            # 주문가능현금을 현재가로 나누어 계산 (MTS 현금매수가능과 동일)
            # current_price=0인 경우 (prefetch용) 예수금만 반환
            if current_price > 0:
                calculated_qty = ord_psbl_cash // current_price
                logger.info(
                    f"[{stock_code}] 매수수량 계산: "
                    f"{ord_psbl_cash:,} / {current_price:,} = {calculated_qty}주"
                )
                return calculated_qty, ord_psbl_cash

            # current_price=0: prefetch용 - 예수금만 반환 (수량 계산 생략)
            if psbl_qty_calc_unpr > 0:
                calculated_qty = ord_psbl_cash // psbl_qty_calc_unpr
                logger.debug(f"[{stock_code}] 예수금 조회: {ord_psbl_cash:,}원")
                return calculated_qty, ord_psbl_cash

            # fallback: 기존 방식 (계산단가가 없는 경우)
            logger.warning(
                f"[{stock_code}] 계산단가 없음, 미수없는매수수량 사용: {nrcvb_buy_qty}주"
            )
            return nrcvb_buy_qty, ord_psbl_cash

        except Exception as e:
            logger.error(f"Failed to get buyable quantity: {e}")
            return 0, 0

    def get_deposit(self) -> int:
        """
        예수금 조회

        Returns:
            예수금 (원)
        """
        _, summary = self.get_balance()
        return summary.get("deposit", 0)

    def get_daily_candles(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        일봉 데이터 조회

        Args:
            stock_code: 종목코드 (6자리)
            start_date: 조회 시작일 (YYYYMMDD)
            end_date: 조회 종료일 (YYYYMMDD)

        Returns:
            일봉 데이터 리스트 (최신순)
            [
                {
                    "trade_date": "20231225",
                    "open_price": 10000,
                    "high_price": 10500,
                    "low_price": 9800,
                    "close_price": 10200,
                    "volume": 1000000,
                    "trade_amount": 10000000000,
                    "change_rate": 2.0
                },
                ...
            ]
        """
        try:
            api_url = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
            tr_id = "FHKST03010100"

            params = {
                "FID_COND_MRKT_DIV_CODE": "J",  # KRX
                "FID_INPUT_ISCD": stock_code,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",  # 일봉
                "FID_ORG_ADJ_PRC": "0",  # 수정주가
            }

            resp: APIResp = self._session.url_fetch(api_url, tr_id, params=params)
            if not resp.is_ok():
                error_code = resp.get_error_code()
                error_msg = resp.get_error_message()
                logger.warning(
                    f"Failed to get daily candles: {stock_code}, "
                    f"error: {error_code} - {error_msg}"
                )
                return []

            body = resp.get_body()
            output2 = getattr(body, "output2", None)

            # output2 검증
            if output2 is None:
                logger.warning(f"No output2 in response for {stock_code}")
                return []

            if not isinstance(output2, (list, tuple)):
                logger.warning(f"output2 is not a list: {type(output2)}")
                return []

            # dict/object 모두 처리하는 헬퍼 함수
            def get_value(obj, key: str, default=0):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            candles = []
            for item in output2:
                try:
                    candle = {
                        "trade_date": str(get_value(item, "stck_bsop_date", "")),
                        "open_price": float(get_value(item, "stck_oprc", 0)),
                        "high_price": float(get_value(item, "stck_hgpr", 0)),
                        "low_price": float(get_value(item, "stck_lwpr", 0)),
                        "close_price": float(get_value(item, "stck_clpr", 0)),
                        "volume": int(get_value(item, "acml_vol", 0)),
                        "trade_amount": int(get_value(item, "acml_tr_pbmn", 0)),
                        "change_rate": float(get_value(item, "prdy_ctrt", 0)),
                    }
                    if candle["close_price"] > 0:  # 유효한 데이터만
                        candles.append(candle)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse candle data: {e}")
                    continue

            logger.info(
                f"Loaded {len(candles)} daily candles for {stock_code} "
                f"({start_date} ~ {end_date})"
            )
            return candles

        except Exception as e:
            logger.error(f"Failed to get daily candles for {stock_code}: {e}")
            return []

    def get_minute_candles(
        self,
        stock_code: str,
        time_unit: str = "1",
        target_hour: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        분봉 데이터 조회 (기준 시간부터 최근 30개)

        Args:
            stock_code: 종목코드 (6자리)
            time_unit: 분봉 단위 (1, 3, 5, 10, 15, 30, 60)
            target_hour: 조회 기준 시간 (HHMMSS 형식, None이면 현재 시간)

        Returns:
            분봉 데이터 리스트 (최신순)
        """
        try:
            api_url = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
            tr_id = "FHKST03010200"

            # 기준 시간 설정 (target_hour가 없으면 현재 시간)
            if target_hour is None:
                fid_input_hour = datetime.now().strftime("%H%M%S")
            else:
                fid_input_hour = target_hour

            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
                "FID_INPUT_HOUR_1": fid_input_hour,
                "FID_PW_DATA_INCU_YN": "Y",  # 과거 데이터 포함
                "FID_ETC_CLS_CODE": "",  # 기타 구분 코드 (필수)
            }

            resp: APIResp = self._session.url_fetch(api_url, tr_id, params=params)
            if not resp.is_ok():
                error_code = resp.get_error_code()
                error_msg = resp.get_error_message()
                logger.warning(
                    f"Failed to get minute candles: {stock_code}, "
                    f"error: {error_code} - {error_msg}"
                )
                return []

            body = resp.get_body()
            output1 = getattr(body, "output1", None)
            output2 = getattr(body, "output2", None)

            # output2 검증
            if output2 is None:
                logger.warning(f"No output2 in minute candles response for {stock_code}")
                return []

            if not isinstance(output2, (list, tuple)):
                logger.warning(f"Minute candles output2 is not a list: {type(output2)}")
                return []

            # dict/object 모두 처리하는 헬퍼 함수
            def get_value(obj, key: str, default=0):
                if isinstance(obj, dict):
                    return obj.get(key, default)
                return getattr(obj, key, default)

            # output1에서 당일 등락률 추출 (전 분봉 공통 적용)
            change_rate = 0.0
            if output1:
                change_rate = float(get_value(output1, "prdy_ctrt", 0))

            candles = []
            for item in output2:
                try:
                    candle = {
                        "trade_date": str(get_value(item, "stck_bsop_date", "")),
                        "time": str(get_value(item, "stck_cntg_hour", "")),
                        "open_price": int(get_value(item, "stck_oprc", 0)),
                        "high_price": int(get_value(item, "stck_hgpr", 0)),
                        "low_price": int(get_value(item, "stck_lwpr", 0)),
                        "close_price": int(get_value(item, "stck_prpr", 0)),
                        "volume": int(get_value(item, "cntg_vol", 0)),
                        "change_rate": change_rate,  # output1의 당일 등락률 사용
                    }
                    if candle["close_price"] > 0:
                        candles.append(candle)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse minute candle: {e}")
                    continue

            return candles

        except Exception as e:
            logger.error(f"Failed to get minute candles for {stock_code}: {e}")
            return []

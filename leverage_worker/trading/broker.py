"""
KIS API 브로커 모듈

한국투자증권 Open API 래퍼
- 현재가 조회
- 잔고 조회
- 시장가 주문 (매수/매도)
- 주문 취소
- 체결 조회
"""

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

            return StockPrice(
                stock_code=stock_code,
                stock_name=getattr(output, "hts_kor_isnm", ""),
                current_price=int(getattr(output, "stck_prpr", 0)),
                prev_close=int(getattr(output, "stck_sdpr", 0)),
                change=int(getattr(output, "prdy_vrss", 0)),
                change_rate=float(getattr(output, "prdy_ctrt", 0)),
                open_price=int(getattr(output, "stck_oprc", 0)),
                high_price=int(getattr(output, "stck_hgpr", 0)),
                low_price=int(getattr(output, "stck_lwpr", 0)),
                volume=int(getattr(output, "acml_vol", 0)),
                trade_amount=int(getattr(output, "acml_tr_pbmn", 0)),
            )
        except Exception as e:
            logger.error(f"Failed to parse price response: {e}")
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
            "INQR_DVSN": "02",  # 종목별
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
            res.print_error(api_url)
            return positions, summary

        try:
            body = res.get_body()

            # 보유 종목 (output1)
            if hasattr(body, "output1"):
                for item in body.output1:
                    qty = int(getattr(item, "hldg_qty", 0))
                    if qty <= 0:
                        continue

                    positions.append(Position(
                        stock_code=getattr(item, "pdno", ""),
                        stock_name=getattr(item, "prdt_name", ""),
                        quantity=qty,
                        avg_price=float(getattr(item, "pchs_avg_pric", 0)),
                        current_price=int(getattr(item, "prpr", 0)),
                        eval_amount=int(getattr(item, "evlu_amt", 0)),
                        profit_loss=int(getattr(item, "evlu_pfls_amt", 0)),
                        profit_rate=float(getattr(item, "evlu_pfls_rt", 0)),
                    ))

            # 계좌 요약 (output2)
            if hasattr(body, "output2") and body.output2:
                out2 = body.output2[0] if isinstance(body.output2, list) else body.output2
                summary = {
                    "total_eval": int(getattr(out2, "tot_evlu_amt", 0)),
                    "deposit": int(getattr(out2, "dnca_tot_amt", 0)),
                    "total_profit_loss": int(getattr(out2, "evlu_pfls_smtl_amt", 0)),
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

    def get_pending_orders(self) -> List[OrderInfo]:
        """
        미체결 주문 조회

        Returns:
            미체결 주문 리스트
        """
        return self._get_orders(filled_only=False)

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
            res.print_error(api_url)
            return orders

        try:
            body = res.get_body()

            if hasattr(body, "output1"):
                for item in body.output1:
                    order_qty = int(getattr(item, "ord_qty", 0))
                    filled_qty = int(getattr(item, "tot_ccld_qty", 0))

                    # 상태 판단
                    if filled_qty == 0:
                        status = OrderStatus.PENDING
                    elif filled_qty < order_qty:
                        status = OrderStatus.PARTIAL
                    else:
                        status = OrderStatus.FILLED

                    # 매수/매도 구분
                    sll_buy = getattr(item, "sll_buy_dvsn_cd", "")
                    side = OrderSide.BUY if sll_buy == "02" else OrderSide.SELL

                    orders.append(OrderInfo(
                        order_id=getattr(item, "odno", ""),
                        order_no=getattr(item, "orgn_odno", ""),
                        branch_no=getattr(item, "ord_gno_brno", ""),
                        stock_code=getattr(item, "pdno", ""),
                        stock_name=getattr(item, "prdt_name", ""),
                        side=side,
                        order_qty=order_qty,
                        order_price=int(getattr(item, "ord_unpr", 0)),
                        filled_qty=filled_qty,
                        filled_price=int(getattr(item, "avg_prvs", 0)),
                        status=status,
                        order_time=getattr(item, "ord_tmd", ""),
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

        logger.info(f"Cancelled {cancelled} pending orders")
        return cancelled

    def get_buyable_quantity(self, stock_code: str) -> int:
        """
        매수 가능 수량 조회 (시장가 기준)

        Args:
            stock_code: 종목코드

        Returns:
            매수 가능 수량
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

        res = self._session.url_fetch(api_url, tr_id, params=params)
        self._session.smart_sleep()

        if not res.is_ok():
            res.print_error(api_url)
            return 0

        try:
            body = res.get_body()
            output = body.output

            # 미수 없는 매수 수량
            qty = int(getattr(output, "nrcvb_buy_qty", 0))
            return qty

        except Exception as e:
            logger.error(f"Failed to get buyable quantity: {e}")
            return 0

    def get_deposit(self) -> int:
        """
        예수금 조회

        Returns:
            예수금 (원)
        """
        _, summary = self.get_balance()
        return summary.get("deposit", 0)

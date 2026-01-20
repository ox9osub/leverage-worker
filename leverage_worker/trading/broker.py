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
    ) -> bool:
        """
        주문 정정 (가격/수량 변경)

        Args:
            order_id: 주문번호
            order_branch: 지점번호
            quantity: 정정 수량
            new_price: 정정 가격

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
            logger.error(f"Order modify failed: {order_id} - {error_msg}")
            return False

        logger.info(f"Order modified: {order_id} -> {quantity}주 @ {new_price:,}원")
        return True

    def get_pending_orders(self) -> List[OrderInfo]:
        """
        미체결 주문 조회

        Returns:
            미체결 주문 리스트
        """
        return self._get_orders(filled_only=False)

    def get_order_status(self, order_id: str) -> Tuple[int, int]:
        """
        주문의 체결/미체결 수량 조회

        Args:
            order_id: 주문번호

        Returns:
            (체결수량, 미체결수량) 튜플
        """
        # 전체 주문 조회 (체결 + 미체결)
        orders = self._get_orders(all_orders=True)

        for order in orders:
            if order.order_id == order_id:
                unfilled_qty = order.order_qty - order.filled_qty
                return (order.filled_qty, unfilled_qty)

        # 주문을 찾지 못한 경우 (전량 체결 등)
        logger.warning(f"Order not found: {order_id}")
        return (0, 0)

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

            # output이 딕셔너리인 경우와 namedtuple인 경우 모두 처리
            if isinstance(output, dict):
                qty = int(output.get("nrcvb_buy_qty", 0) or 0)
            else:
                qty = int(getattr(output, "nrcvb_buy_qty", 0) or 0)
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

            logger.info(f"[DEBUG] Daily candles API params: {params}")
            resp: APIResp = self._session.url_fetch(api_url, tr_id, params=params)

            # 디버깅: API 응답 상태 확인
            logger.info(f"[DEBUG] API response ok: {resp.is_ok()}")
            if not resp.is_ok():
                error_code = resp.get_error_code()
                error_msg = resp.get_error_message()
                logger.warning(
                    f"Failed to get daily candles: {stock_code}, "
                    f"error: {error_code} - {error_msg}"
                )
                return []

            body = resp.get_body()

            # 디버깅: 응답 구조 확인
            if hasattr(body, "_fields"):
                logger.info(f"[DEBUG] Response body fields: {body._fields}")
            else:
                logger.info(f"[DEBUG] Response body type: {type(body)}")

            output2 = getattr(body, "output2", None)

            # output2 검증
            if output2 is None:
                logger.warning(f"No output2 in response for {stock_code}")
                # 다른 필드 확인
                for attr in ["output", "output1", "output2", "output3"]:
                    val = getattr(body, attr, "NOT_FOUND")
                    if val != "NOT_FOUND":
                        logger.info(f"[DEBUG]   Found {attr}: type={type(val)}, len={len(val) if hasattr(val, '__len__') else 'N/A'}")
                return []

            if not isinstance(output2, (list, tuple)):
                logger.warning(f"output2 is not a list: {type(output2)}")
                return []

            logger.info(f"[DEBUG] output2 length: {len(output2)}")

            # 첫 번째 아이템 구조 확인 (있는 경우)
            if len(output2) > 0:
                first_item = output2[0]
                if hasattr(first_item, "_fields"):
                    logger.info(f"[DEBUG] First item fields: {first_item._fields}")
                elif isinstance(first_item, dict):
                    logger.info(f"[DEBUG] First item keys: {list(first_item.keys())}")
                else:
                    logger.info(f"[DEBUG] First item type: {type(first_item)}")

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

            logger.info(f"[DEBUG] Minute candles API params: {params}")
            resp: APIResp = self._session.url_fetch(api_url, tr_id, params=params)

            # 디버깅: API 응답 상태 확인
            logger.info(f"[DEBUG] API response ok: {resp.is_ok()}")
            if not resp.is_ok():
                error_code = resp.get_error_code()
                error_msg = resp.get_error_message()
                logger.warning(
                    f"Failed to get minute candles: {stock_code}, "
                    f"error: {error_code} - {error_msg}"
                )
                return []

            body = resp.get_body()

            # 디버깅: 응답 구조 확인
            if hasattr(body, "_fields"):
                logger.info(f"[DEBUG] Response body fields: {body._fields}")
            else:
                logger.info(f"[DEBUG] Response body type: {type(body)}")

            output2 = getattr(body, "output2", None)

            # output2 검증
            if output2 is None:
                logger.warning(f"No output2 in minute candles response for {stock_code}")
                return []

            if not isinstance(output2, (list, tuple)):
                logger.warning(f"Minute candles output2 is not a list: {type(output2)}")
                return []

            logger.info(f"[DEBUG] Minute candles output2 length: {len(output2)}")

            # 첫 번째 아이템 구조 확인 (있는 경우)
            if len(output2) > 0:
                first_item = output2[0]
                if hasattr(first_item, "_fields"):
                    logger.info(f"[DEBUG] Minute first item fields: {first_item._fields}")
                elif isinstance(first_item, dict):
                    logger.info(f"[DEBUG] Minute first item keys: {list(first_item.keys())}")
                else:
                    logger.info(f"[DEBUG] Minute first item type: {type(first_item)}")

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
                        "time": str(get_value(item, "stck_cntg_hour", "")),
                        "open_price": int(get_value(item, "stck_oprc", 0)),
                        "high_price": int(get_value(item, "stck_hgpr", 0)),
                        "low_price": int(get_value(item, "stck_lwpr", 0)),
                        "close_price": int(get_value(item, "stck_prpr", 0)),
                        "volume": int(get_value(item, "cntg_vol", 0)),
                    }
                    if candle["close_price"] > 0:
                        candles.append(candle)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse minute candle: {e}")
                    continue

            logger.info(f"Loaded {len(candles)} minute candles for {stock_code}")
            return candles

        except Exception as e:
            logger.error(f"Failed to get minute candles for {stock_code}: {e}")
            return []

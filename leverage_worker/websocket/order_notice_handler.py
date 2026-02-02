"""
실시간 체결통보 데이터 파싱 모듈

WebSocket을 통해 수신한 체결통보(H0STCNI0/H0STCNI9) 데이터를 파싱하여
OrderNoticeData 객체로 변환
"""

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OrderNoticeData:
    """실시간 체결통보 데이터"""

    stock_code: str  # 종목코드 (STCK_SHRN_ISCD)
    order_no: str  # 주문번호 (ODER_NO)
    is_filled: bool  # 체결여부 (CNTG_YN == "2")
    filled_qty: int  # 체결수량 (CNTG_QTY)
    filled_price: int  # 체결단가 (CNTG_UNPR)
    side: str  # 매도매수구분 (SELN_BYOV_CLS): "01"=매도, "02"=매수
    order_qty: int  # 주문수량 (ODER_QTY)
    fill_time: str  # 체결시간 (STCK_CNTG_HOUR)


class OrderNoticeHandler:
    """
    체결통보 데이터 파서

    H0STCNI0 (실전) / H0STCNI9 (모의) 체결통보 데이터를 파싱
    CNTG_YN이 "2"인 체결 통보만 처리 (접수/취소/거부는 무시)
    """

    # H0STCNI0 컬럼명 (ccnl_notice columns 기준)
    COL_STOCK_CODE = "STCK_SHRN_ISCD"  # 종목코드
    COL_ORDER_NO = "ODER_NO"  # 주문번호
    COL_FILL_YN = "CNTG_YN"  # 체결여부 (2=체결, 1=접수/취소/거부)
    COL_FILL_QTY = "CNTG_QTY"  # 체결수량
    COL_FILL_PRICE = "CNTG_UNPR"  # 체결단가
    COL_SIDE = "SELN_BYOV_CLS"  # 매도매수구분
    COL_ORDER_QTY = "ODER_QTY"  # 주문수량
    COL_FILL_TIME = "STCK_CNTG_HOUR"  # 체결시간

    def parse(self, df: pd.DataFrame) -> Optional[OrderNoticeData]:
        """
        DataFrame에서 체결통보 데이터 파싱

        Args:
            df: WebSocket에서 수신한 DataFrame (한 행)

        Returns:
            OrderNoticeData 객체 (체결인 경우) 또는 None (접수/취소/거부)
        """
        if df.empty:
            return None

        try:
            row = df.iloc[0]

            # 체결여부 확인 (2=체결, 1=접수/취소/거부)
            cntg_yn = str(row[self.COL_FILL_YN]).strip()
            if cntg_yn != "2":
                logger.debug(
                    f"체결통보 접수/취소/거부 (CNTG_YN={cntg_yn}): "
                    f"{row.get(self.COL_STOCK_CODE, 'N/A')}"
                )
                return None

            filled_qty = int(row[self.COL_FILL_QTY])
            filled_price = int(row[self.COL_FILL_PRICE])

            return OrderNoticeData(
                stock_code=str(row[self.COL_STOCK_CODE]).strip(),
                order_no=str(row[self.COL_ORDER_NO]).strip(),
                is_filled=True,
                filled_qty=filled_qty,
                filled_price=filled_price,
                side=str(row[self.COL_SIDE]).strip(),
                order_qty=int(row[self.COL_ORDER_QTY]),
                fill_time=str(row[self.COL_FILL_TIME]).strip(),
            )

        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Order notice parse error: {e}")
            return None

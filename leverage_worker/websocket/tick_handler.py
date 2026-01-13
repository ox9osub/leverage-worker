"""
실시간 체결 데이터 파싱 모듈

WebSocket을 통해 수신한 체결 데이터를 파싱하여 TickData 객체로 변환
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TickData:
    """실시간 체결 데이터"""

    stock_code: str  # 종목코드 (MKSC_SHRN_ISCD)
    price: int  # 현재가/체결가 (STCK_PRPR)
    volume: int  # 체결수량 (CNTG_VOL)
    accumulated_volume: int  # 누적거래량 (ACML_VOL)
    change: int  # 전일대비 (PRDY_VRSS)
    change_rate: float  # 전일대비율 (PRDY_CTRT)
    open_price: int  # 시가 (STCK_OPRC)
    high_price: int  # 고가 (STCK_HGPR)
    low_price: int  # 저가 (STCK_LWPR)
    timestamp: datetime  # 체결시간


class TickHandler:
    """
    체결 데이터 파서

    H0STCNT0 (국내주식 실시간체결가) 데이터를 파싱
    """

    # H0STCNT0 컬럼 인덱스 (ccnl_krx columns 기준)
    # columns = ["MKSC_SHRN_ISCD", "STCK_CNTG_HOUR", "STCK_PRPR", "PRDY_VRSS_SIGN",
    #            "PRDY_VRSS", "PRDY_CTRT", "WGHN_AVRG_STCK_PRC", "STCK_OPRC",
    #            "STCK_HGPR", "STCK_LWPR", "ASKP1", "BIDP1", "CNTG_VOL", "ACML_VOL", ...]

    COL_STOCK_CODE = "MKSC_SHRN_ISCD"  # 종목코드
    COL_TIME = "STCK_CNTG_HOUR"  # 체결시간
    COL_PRICE = "STCK_PRPR"  # 현재가
    COL_CHANGE_SIGN = "PRDY_VRSS_SIGN"  # 전일대비 부호
    COL_CHANGE = "PRDY_VRSS"  # 전일대비
    COL_CHANGE_RATE = "PRDY_CTRT"  # 전일대비율
    COL_OPEN = "STCK_OPRC"  # 시가
    COL_HIGH = "STCK_HGPR"  # 고가
    COL_LOW = "STCK_LWPR"  # 저가
    COL_VOLUME = "CNTG_VOL"  # 체결수량
    COL_ACCUM_VOL = "ACML_VOL"  # 누적거래량

    def parse(self, df: pd.DataFrame, tr_id: str) -> Optional[TickData]:
        """
        DataFrame에서 체결 데이터 파싱

        Args:
            df: WebSocket에서 수신한 DataFrame (한 행)
            tr_id: TR ID (H0STCNT0 등)

        Returns:
            TickData 객체 또는 None
        """
        if tr_id != "H0STCNT0":
            return None

        if df.empty:
            return None

        try:
            row = df.iloc[0]

            # 시간 파싱 (HHMMSS)
            time_str = str(row[self.COL_TIME])
            hour = int(time_str[0:2])
            minute = int(time_str[2:4])
            second = int(time_str[4:6])
            now = datetime.now()
            timestamp = now.replace(hour=hour, minute=minute, second=second, microsecond=0)

            # 전일대비 부호 처리
            change_sign = str(row[self.COL_CHANGE_SIGN])
            change = int(row[self.COL_CHANGE])
            if change_sign in ["5", "4"]:  # 하락, 상한
                change = -change

            return TickData(
                stock_code=str(row[self.COL_STOCK_CODE]),
                price=int(row[self.COL_PRICE]),
                volume=int(row[self.COL_VOLUME]),
                accumulated_volume=int(row[self.COL_ACCUM_VOL]),
                change=change,
                change_rate=float(row[self.COL_CHANGE_RATE]),
                open_price=int(row[self.COL_OPEN]),
                high_price=int(row[self.COL_HIGH]),
                low_price=int(row[self.COL_LOW]),
                timestamp=timestamp,
            )

        except (KeyError, ValueError, IndexError) as e:
            logger.error(f"Tick parse error: {e}")
            return None

"""
세션 관리 모듈

KIS Open API 인증 및 토큰 관리
- 토큰 발급/갱신
- 8시간 전 자동 갱신
- API 호출 공통 함수
"""

import copy
import json
import threading
import time
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

import requests
import yaml

from leverage_worker.config.settings import Settings, TradingMode
from leverage_worker.utils.logger import get_logger

logger = get_logger(__name__)


class APIResp:
    """API 응답 처리 클래스"""

    def __init__(self, resp: requests.Response):
        self._rescode = resp.status_code
        self._resp = resp
        self._header = self._set_header()
        self._body = self._set_body()
        self._err_code = getattr(self._body, "msg_cd", "")
        self._err_message = getattr(self._body, "msg1", "")

    def _set_header(self):
        fld = {}
        for x in self._resp.headers.keys():
            if x.islower():
                fld[x] = self._resp.headers.get(x)
        _th_ = namedtuple("header", fld.keys())
        return _th_(**fld)

    def _set_body(self):
        _tb_ = namedtuple("body", self._resp.json().keys())
        return _tb_(**self._resp.json())

    def get_res_code(self) -> int:
        return self._rescode

    def get_header(self):
        return self._header

    def get_body(self):
        return self._body

    def get_response(self) -> requests.Response:
        return self._resp

    def is_ok(self) -> bool:
        try:
            return self.get_body().rt_cd == "0"
        except Exception:
            return False

    def get_error_code(self) -> str:
        return self._err_code

    def get_error_message(self) -> str:
        return self._err_message

    def print_error(self, url: str = "") -> None:
        logger.error(
            f"API Error - Code: {self.get_error_code()}, "
            f"Message: {self.get_error_message()}, URL: {url}"
        )


class APIRespError(APIResp):
    """API 에러 응답 클래스"""

    def __init__(self, status_code: int, error_text: str):
        self.status_code = status_code
        self.error_text = error_text
        self._error_code = str(status_code)
        self._error_message = error_text

    def is_ok(self) -> bool:
        return False

    def get_error_code(self) -> str:
        return self._error_code

    def get_error_message(self) -> str:
        return self._error_message

    def get_body(self):
        class EmptyBody:
            def __getattr__(self, name):
                return None
        return EmptyBody()

    def get_header(self):
        class EmptyHeader:
            tr_cont = ""
            def __getattr__(self, name):
                return ""
        return EmptyHeader()


class SessionManager:
    """
    세션 및 토큰 관리 클래스

    - 토큰 발급 및 저장
    - 만료 8시간 전 자동 갱신
    - API 호출 공통 함수
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._last_auth_time: Optional[datetime] = None

        # 기본 헤더
        self._base_headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8",
            "User-Agent": "Mozilla/5.0",
        }

        # 토큰 파일 경로
        self._token_dir = Path.home() / "KIS" / "config"
        self._token_dir.mkdir(parents=True, exist_ok=True)

        # 자동 갱신 스레드
        self._refresh_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # 모의/실전 구분
        self._is_paper = settings.mode == TradingMode.PAPER
        self._smart_sleep = 0.5 if self._is_paper else 0.05

    def _get_token_file_path(self) -> Path:
        """토큰 파일 경로 (일별)"""
        mode_suffix = "paper" if self._is_paper else "prod"
        return self._token_dir / f"KIS_{mode_suffix}_{datetime.now().strftime('%Y%m%d')}"

    def _save_token(self, token: str, expired: str) -> None:
        """토큰 저장"""
        valid_date = datetime.strptime(expired, "%Y-%m-%d %H:%M:%S")
        token_path = self._get_token_file_path()

        with open(token_path, "w", encoding="utf-8") as f:
            f.write(f"token: {token}\n")
            f.write(f"valid-date: {valid_date}\n")

        logger.debug(f"Token saved to {token_path}")

    def _read_token(self) -> Optional[str]:
        """저장된 토큰 읽기"""
        token_path = self._get_token_file_path()

        if not token_path.exists():
            return None

        try:
            with open(token_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if data is None:
                return None

            exp_dt = datetime.strftime(data["valid-date"], "%Y-%m-%d %H:%M:%S")
            now_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if exp_dt > now_dt:
                return data["token"]
            return None

        except Exception as e:
            logger.warning(f"Failed to read token: {e}")
            return None

    def _get_base_header(self) -> Dict[str, str]:
        """기본 헤더 복사본 반환"""
        return copy.deepcopy(self._base_headers)

    def authenticate(self) -> bool:
        """
        인증 수행 (토큰 발급)

        Returns:
            성공 여부
        """
        with self._lock:
            # 기존 토큰 확인
            saved_token = self._read_token()

            if saved_token is None:
                # 신규 토큰 발급
                if not self._request_new_token():
                    return False
            else:
                self._token = saved_token
                self._token_expires_at = datetime.now() + timedelta(hours=24)
                logger.info("Using existing token")

            # 헤더에 인증 정보 설정
            self._base_headers["authorization"] = f"Bearer {self._token}"
            self._base_headers["appkey"] = self._settings.app_key
            self._base_headers["appsecret"] = self._settings.app_secret

            self._last_auth_time = datetime.now()
            logger.info(f"Authentication completed. Mode: {self._settings.mode.value}")

            return True

    def _request_new_token(self) -> bool:
        """신규 토큰 발급 요청"""
        url = f"{self._settings.get_server_url()}/oauth2/tokenP"

        payload = {
            "grant_type": "client_credentials",
            "appkey": self._settings.app_key,
            "appsecret": self._settings.app_secret,
        }

        try:
            res = requests.post(
                url,
                data=json.dumps(payload),
                headers=self._get_base_header(),
                timeout=10,
            )

            if res.status_code == 200:
                data = res.json()
                self._token = data["access_token"]
                token_expired = data["access_token_token_expired"]

                self._save_token(self._token, token_expired)
                self._token_expires_at = datetime.strptime(
                    token_expired, "%Y-%m-%d %H:%M:%S"
                )

                logger.info("New token issued successfully")
                return True
            else:
                logger.error(f"Token request failed: {res.status_code} - {res.text}")
                return False

        except Exception as e:
            logger.error(f"Token request error: {e}")
            return False

    def start_auto_refresh(self) -> None:
        """토큰 자동 갱신 스레드 시작"""
        self._running = True
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            daemon=True,
            name="TokenRefreshThread"
        )
        self._refresh_thread.start()
        logger.info("Token auto-refresh thread started")

    def stop_auto_refresh(self) -> None:
        """토큰 자동 갱신 스레드 중지"""
        self._running = False
        if self._refresh_thread:
            self._refresh_thread.join(timeout=5)
        logger.info("Token auto-refresh thread stopped")

    def _refresh_loop(self) -> None:
        """토큰 갱신 루프 (만료 8시간 전 갱신)"""
        refresh_hours = self._settings.session.token_refresh_hours_before

        while self._running:
            try:
                if self._token_expires_at:
                    time_until_expiry = self._token_expires_at - datetime.now()
                    refresh_threshold = timedelta(hours=refresh_hours)

                    if time_until_expiry <= refresh_threshold:
                        logger.info("Token refresh triggered")
                        self._refresh_token()

                # 1분마다 체크
                time.sleep(60)

            except Exception as e:
                logger.error(f"Token refresh loop error: {e}")
                time.sleep(60)

    def _refresh_token(self) -> None:
        """토큰 갱신"""
        with self._lock:
            if self._request_new_token():
                self._base_headers["authorization"] = f"Bearer {self._token}"
                logger.info("Token refreshed successfully")
            else:
                logger.error("Token refresh failed")

    @property
    def is_authenticated(self) -> bool:
        """인증 상태 확인"""
        return (
            self._token is not None
            and self._token_expires_at is not None
            and datetime.now() < self._token_expires_at
        )

    def smart_sleep(self) -> None:
        """API 호출 간 대기 (Rate Limit 대응)"""
        time.sleep(self._smart_sleep)

    def url_fetch(
        self,
        api_url: str,
        tr_id: str,
        tr_cont: str = "",
        params: Optional[Dict[str, Any]] = None,
        append_headers: Optional[Dict[str, str]] = None,
        post_flag: bool = False,
    ) -> APIResp:
        """
        API 호출 공통 함수

        Args:
            api_url: API URL 경로 (예: "/uapi/domestic-stock/v1/quotations/inquire-price")
            tr_id: 트랜잭션 ID
            tr_cont: 연속 조회 여부 ("", "N", "M")
            params: 요청 파라미터
            append_headers: 추가 헤더
            post_flag: POST 요청 여부

        Returns:
            APIResp 객체
        """
        url = f"{self._settings.get_server_url()}{api_url}"
        headers = self._get_base_header()

        # TR ID 변환 (모의투자)
        actual_tr_id = tr_id
        if tr_id[0] in ("T", "J", "C") and self._is_paper:
            actual_tr_id = "V" + tr_id[1:]

        headers["tr_id"] = actual_tr_id
        headers["custtype"] = "P"
        headers["tr_cont"] = tr_cont

        # 추가 헤더
        if append_headers:
            headers.update(append_headers)

        params = params or {}

        try:
            if post_flag:
                res = requests.post(
                    url,
                    headers=headers,
                    data=json.dumps(params),
                    timeout=30,
                )
            else:
                res = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=30,
                )

            if res.status_code == 200:
                return APIResp(res)
            else:
                logger.error(f"API Error: {res.status_code} - {res.text}")
                return APIRespError(res.status_code, res.text)

        except requests.exceptions.Timeout:
            logger.error(f"API Timeout: {url}")
            return APIRespError(408, "Request Timeout")

        except Exception as e:
            logger.error(f"API Exception: {e}")
            return APIRespError(500, str(e))

    def get_account_info(self) -> tuple:
        """계좌 정보 반환 (계좌번호, 상품코드)"""
        return self._settings.account_number, self._settings.account_product_code

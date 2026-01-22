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
    - 재시도 로직 (지수 백오프)
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self._last_auth_time: Optional[datetime] = None
        self._token_valid: bool = False  # 토큰 유효성 플래그

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

        # Slack 알림 콜백 (외부에서 설정)
        self._on_token_refresh_failed: Optional[callable] = None

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

    def _read_token(self) -> Optional[tuple[str, datetime]]:
        """저장된 토큰 읽기 (토큰, 만료시간 반환)"""
        token_path = self._get_token_file_path()

        if not token_path.exists():
            return None

        try:
            with open(token_path, encoding="utf-8") as f:
                data = yaml.safe_load(f)

            if data is None:
                return None

            valid_date: datetime = data["valid-date"]

            if valid_date > datetime.now():
                return (data["token"], valid_date)
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
            saved = self._read_token()

            if saved is None:
                # 신규 토큰 발급
                if not self._request_new_token():
                    return False
            else:
                token, expires_at = saved
                self._token = token
                self._token_expires_at = expires_at
                remaining = expires_at - datetime.now()
                logger.info(f"Using existing token (expires: {expires_at}, remaining: {remaining})")

            # 헤더에 인증 정보 설정
            self._base_headers["authorization"] = f"Bearer {self._token}"
            self._base_headers["appkey"] = self._settings.app_key
            self._base_headers["appsecret"] = self._settings.app_secret

            self._last_auth_time = datetime.now()
            self._token_valid = True  # 토큰 유효 플래그 설정
            logger.info(f"Authentication completed. Mode: {self._settings.mode.value}")

            return True

    def set_token_refresh_failed_callback(self, callback: callable) -> None:
        """토큰 갱신 실패 시 호출할 콜백 설정 (예: Slack 알림)"""
        self._on_token_refresh_failed = callback

    def _request_new_token(self) -> bool:
        """신규 토큰 발급 요청"""
        server_url = self._settings.get_server_url()
        url = f"{server_url}/oauth2/tokenP"
        app_key = self._settings.app_key
        mode = self._settings.mode.value

        # AppKey 마스킹 (앞 4자리 + ... + 뒤 4자리)
        masked_key = f"{app_key[:4]}...{app_key[-4:]}" if len(app_key) > 8 else "***"

        logger.info(f"토큰 발급 요청 - 모드: {mode}, 서버: {server_url}, AppKey: {masked_key}")

        payload = {
            "grant_type": "client_credentials",
            "appkey": app_key,
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

                logger.info(f"토큰 발급 성공 - 만료: {token_expired}")
                return True
            else:
                logger.error(
                    f"토큰 발급 실패 - 모드: {mode}, 서버: {server_url}, "
                    f"AppKey: {masked_key}, 상태코드: {res.status_code}, "
                    f"응답: {res.text}"
                )
                return False

        except Exception as e:
            logger.error(f"토큰 요청 오류 - 모드: {mode}, 서버: {server_url}, 에러: {e}")
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

    def _refresh_token(self, max_retries: int = 3) -> bool:
        """
        토큰 갱신 (재시도 로직 포함)

        Args:
            max_retries: 최대 재시도 횟수

        Returns:
            갱신 성공 여부
        """
        retry_delays = [5, 10, 20]  # 5초, 10초, 20초 후 재시도

        for attempt in range(max_retries + 1):
            with self._lock:
                if self._request_new_token():
                    self._base_headers["authorization"] = f"Bearer {self._token}"
                    self._token_valid = True
                    logger.info("Token refreshed successfully")
                    return True

            # 재시도 필요
            if attempt < max_retries:
                delay = retry_delays[attempt] if attempt < len(retry_delays) else retry_delays[-1]
                logger.warning(
                    f"Token refresh failed, attempt {attempt + 1}/{max_retries + 1}, "
                    f"retrying in {delay}s"
                )
                time.sleep(delay)
            else:
                # 모든 재시도 실패
                self._token_valid = False
                logger.error(f"Token refresh failed after {max_retries + 1} attempts")

                # 알림 콜백 호출
                if self._on_token_refresh_failed:
                    try:
                        self._on_token_refresh_failed(
                            "토큰 갱신 실패",
                            f"토큰 갱신이 {max_retries + 1}회 시도 후 실패했습니다. "
                            f"API 호출이 중단될 수 있습니다."
                        )
                    except Exception as e:
                        logger.error(f"Failed to send token refresh failure notification: {e}")

                return False

        return False

    @property
    def is_authenticated(self) -> bool:
        """인증 상태 확인 (토큰 존재 + 만료 안됨 + 유효 플래그)"""
        return (
            self._token is not None
            and self._token_expires_at is not None
            and datetime.now() < self._token_expires_at
            and self._token_valid
        )

    @property
    def is_token_valid(self) -> bool:
        """토큰 유효성 플래그 (갱신 실패 시 False)"""
        return self._token_valid

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
        max_retries: int = 3,
    ) -> APIResp:
        """
        API 호출 공통 함수 (재시도 로직 포함)

        Args:
            api_url: API URL 경로 (예: "/uapi/domestic-stock/v1/quotations/inquire-price")
            tr_id: 트랜잭션 ID
            tr_cont: 연속 조회 여부 ("", "N", "M")
            params: 요청 파라미터
            append_headers: 추가 헤더
            post_flag: POST 요청 여부
            max_retries: 최대 재시도 횟수 (기본: 3)

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

        # 재시도 로직 (지수 백오프)
        base_delay = 1.0  # 초기 대기 시간 (초)
        max_delay = 10.0  # 최대 대기 시간 (초)
        last_error: Optional[Exception] = None
        last_status_code: int = 0

        for attempt in range(max_retries + 1):
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

                # 성공
                if res.status_code == 200:
                    return APIResp(res)

                # Rate Limit (429) - 재시도
                if res.status_code == 429:
                    last_status_code = 429
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            f"Rate limit hit (429), attempt {attempt + 1}/{max_retries + 1}, "
                            f"retrying in {delay:.1f}s - {api_url}"
                        )
                        time.sleep(delay)
                        continue
                    logger.error(f"Rate limit exceeded after {max_retries + 1} attempts: {api_url}")
                    return APIRespError(429, "Rate limit exceeded after retries")

                # 서버 에러 (5xx) - 재시도
                if res.status_code >= 500:
                    last_status_code = res.status_code
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            f"Server error ({res.status_code}), attempt {attempt + 1}/{max_retries + 1}, "
                            f"retrying in {delay:.1f}s - {api_url}"
                        )
                        time.sleep(delay)
                        continue
                    logger.error(f"Server error after {max_retries + 1} attempts: {api_url}")
                    return APIRespError(res.status_code, res.text)

                # 기타 에러 (재시도 안함)
                logger.error(f"API Error: {res.status_code} - {res.text}")
                return APIRespError(res.status_code, res.text)

            except requests.exceptions.Timeout:
                last_error = requests.exceptions.Timeout("Request Timeout")
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        f"Timeout, attempt {attempt + 1}/{max_retries + 1}, "
                        f"retrying in {delay:.1f}s - {api_url}"
                    )
                    time.sleep(delay)
                    continue
                logger.error(f"API Timeout after {max_retries + 1} attempts: {api_url}")
                return APIRespError(408, "Request Timeout after retries")

            except requests.exceptions.ConnectionError as e:
                last_error = e
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        f"Connection error, attempt {attempt + 1}/{max_retries + 1}, "
                        f"retrying in {delay:.1f}s - {api_url}"
                    )
                    time.sleep(delay)
                    continue
                logger.error(f"Connection error after {max_retries + 1} attempts: {api_url} - {e}")
                return APIRespError(503, f"Connection error: {e}")

            except Exception as e:
                logger.error(f"API Exception: {e}")
                return APIRespError(500, str(e))

        # 모든 재시도 실패 (이론상 여기 도달하지 않음)
        logger.error(f"All retries exhausted: {api_url}")
        return APIRespError(last_status_code or 500, str(last_error) if last_error else "Unknown error")

    def get_account_info(self) -> tuple:
        """계좌 정보 반환 (계좌번호, 상품코드)"""
        return self._settings.account_number, self._settings.account_product_code

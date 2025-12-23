# Project Overview: leverage-worker (KIS Open API)

## Purpose
한국투자증권(Korea Investment & Securities) Open API를 활용한 주식 자동매매 프로그램.
LLM(ChatGPT, Claude 등)과 Python 개발자 모두가 쉽게 이해하고 활용할 수 있도록 구성된 샘플 코드 모음.

## Tech Stack
- **Language**: Python 3.13+
- **Package Manager**: uv (권장)
- **Core Dependencies**:
  - `pandas` - 데이터 처리
  - `requests` - REST API 호출
  - `websockets` - 실시간 WebSocket 연결
  - `pyyaml` - YAML 설정 파일 파싱
  - `pycryptodome` - 암호화 처리
  - `pyqt6`, `pyside6` - GUI 지원

## Key Features
- REST API 및 WebSocket 기반 주식 시세 조회
- 주문/잔고 관리
- 실전투자/모의투자 환경 지원
- 국내주식, 해외주식, 선물옵션, ETF/ETN, ELW, 채권 등 다양한 상품 지원

## Authentication
- 설정 파일: `~/KIS/config/kis_devlp.yaml`
- 토큰 자동 저장/재사용 (일 단위)
- 인증 함수:
  - `ka.auth(svr="prod", product="01")` - 실전투자
  - `ka.auth(svr="vps")` - 모의투자
  - `ka.auth_ws()` - WebSocket 인증

## API Categories
| Category | Folder | Description |
|----------|--------|-------------|
| 국내주식 | `domestic_stock` | 시세, 주문, 잔고 |
| 해외주식 | `overseas_stock` | 해외 시세, 주문 |
| 국내선물옵션 | `domestic_futureoption` | 파생상품 |
| 해외선물옵션 | `overseas_futureoption` | 해외 파생 |
| 국내채권 | `domestic_bond` | 채권 거래 |
| ETF/ETN | `etfetn` | ETF 시세 |
| ELW | `elw` | ELW 시세 |

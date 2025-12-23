# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

특정 종목에 대해 매매 전략을 수행하는 주식 자동매매 프로그램. 한국투자증권(KIS) Open API를 기반으로 구축.

## Build & Run Commands

```bash
# uv 패키지 매니저 설치 (Windows PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# 의존성 설치
uv sync

# REST API 예제 실행 (examples_user/domestic_stock/)
python domestic_stock_examples.py

# WebSocket 실시간 예제 실행
python domestic_stock_examples_ws.py

# 단일 API 테스트 (examples_llm/domestic_stock/inquire_price/)
python chk_inquire_price.py
```

## Architecture

### 핵심 구조

**`examples_llm/`** - LLM 최적화 단일 함수 샘플:
- API별 개별 폴더 (예: `domestic_stock/inquire_price/`)
- `[함수명].py` - 한 줄 호출 가능한 최소 단위 함수
- `chk_[함수명].py` - 테스트/검증 파일

**`examples_user/`** - 통합 예제:
- 상품 카테고리별 구성 (예: `domestic_stock/`)
- `[카테고리]_functions.py` - 해당 카테고리 전체 API 함수 모음
- `[카테고리]_examples.py` - 사용 예제
- `*_ws.py` - WebSocket 버전

### API 카테고리

| 카테고리 | 폴더 | 용도 |
|----------|------|------|
| 국내주식 | `domestic_stock` | 시세, 주문, 잔고 |
| 해외주식 | `overseas_stock` | 해외 시세, 주문 |
| 국내선물옵션 | `domestic_futureoption` | 파생상품 |
| 해외선물옵션 | `overseas_futureoption` | 해외 파생 |
| 국내채권 | `domestic_bond` | 채권 거래 |
| ETF/ETN | `etfetn` | ETF 시세 |
| ELW | `elw` | ELW 시세 |

### 인증 (`kis_auth.py`)

```python
import kis_auth as ka

# 실전투자
ka.auth(svr="prod", product="01")

# 모의투자
ka.auth(svr="vps")

# WebSocket 인증
ka.auth_ws()
```

- 설정 파일: `~/KIS/config/kis_devlp.yaml` (App Key, App Secret, 계좌정보)
- 토큰은 자동 저장/재사용 (일 단위)

## Code Conventions

### 네이밍
- 모듈/변수/함수: `snake_case`
- 클래스: `PascalCase`
- 상수: `UPPER_SNAKE_CASE`

### 파일/폴더명
- API URL 경로에서 파생 (예: `/uapi/domestic-stock/v1/quotations/inquire-price` → `domestic_stock/inquire_price/`)

### Import 순서
1. 표준 라이브러리
2. 서드파티 패키지
3. 로컬 모듈

### 타입 힌트
- 함수 파라미터, 반환값, 변수에 필수 적용
- 복잡한 타입은 `Optional`, `Union`, `List`, `Dict` 사용

## Development Guidelines

- 코드 작성 전 계획을 세우고, 에러 없이 수행되도록 철저히 검토
- Python 전문가 수준의 코드 품질 유지
- 매매 로직은 모의투자(`svr="vps"`)에서 충분히 테스트 후 실전 적용

### 실행 방법
- bat 파일 사용 금지 (한글 깨짐, 가독성 저하)
- Python 직접 실행: `python 파일명.py`
- 실행 파라미터는 `파일명.txt`에 별도 저장

## Dependencies

핵심 패키지: `pandas`, `requests`, `websockets`, `pyyaml`, `pycryptodome`

Python 3.13+ 필요

# Codebase Structure

## Root Directory
```
leverage-worker/
├── CLAUDE.md                    # Claude Code 가이드
├── README.md                    # 프로젝트 설명서
├── pyproject.toml               # 프로젝트 의존성 (uv)
├── uv.lock                      # 의존성 락 파일
├── kis_devlp.yaml               # API 설정 파일 (개인정보)
├── docs/                        # 문서
│   └── convention.md            # 코딩 컨벤션 가이드
├── examples_llm/                # LLM용 단일 API 샘플
├── examples_user/               # 사용자용 통합 예제
├── legacy/                      # 구 샘플코드 보관
├── stocks_info/                 # 종목정보 참고 데이터
└── MCP/                         # MCP 관련 파일
```

## examples_llm/ Structure
LLM이 단일 API 기능을 쉽게 탐색/호출할 수 있도록 구성

```
examples_llm/
├── kis_auth.py                  # 인증 공통 함수
├── domestic_stock/              # 국내주식
│   ├── inquire_price/           # API별 개별 폴더
│   │   ├── inquire_price.py     # 한줄 호출 함수
│   │   └── chk_inquire_price.py # 테스트 파일
│   ├── order_cash/              # 현금주문
│   ├── inquire_balance/         # 잔고조회
│   └── ... (150+ API folders)
├── overseas_stock/
├── domestic_bond/
├── domestic_futureoption/
├── overseas_futureoption/
├── etfetn/
├── elw/
└── auth/
```

## examples_user/ Structure
사용자가 실제 투자/자동매매에 활용할 수 있도록 통합 구성

```
examples_user/
├── kis_auth.py                          # 인증 공통 함수
├── domestic_stock/                      # 국내주식
│   ├── domestic_stock_functions.py      # REST API 전체 함수 모음
│   ├── domestic_stock_examples.py       # REST 사용 예제
│   ├── domestic_stock_functions_ws.py   # WebSocket 함수 모음
│   └── domestic_stock_examples_ws.py    # WebSocket 사용 예제
├── overseas_stock/
├── domestic_bond/
├── domestic_futureoption/
├── overseas_futureoption/
├── etfetn/
├── elw/
└── auth/
```

## Key Files
- `kis_auth.py` - 인증 및 공통 기능 (토큰 관리, API 호출, 환경 전환)
- `*_functions.py` - 카테고리별 모든 API 함수 통합
- `*_examples.py` - 실제 사용 예제
- `*_ws.py` - WebSocket 버전

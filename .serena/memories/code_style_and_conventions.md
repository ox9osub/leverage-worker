# Code Style and Conventions

## Naming Conventions
- **Modules/Variables/Functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`

## File/Folder Naming
- API URL 경로에서 파생
- 예: `/uapi/domestic-stock/v1/quotations/inquire-price` → `domestic_stock/inquire_price/`

## File Types
- `[함수명].py` - 한 줄 호출 가능한 최소 단위 함수
- `chk_[함수명].py` - 테스트/검증 파일

## Import Order
1. 표준 라이브러리
2. 서드파티 패키지
3. 로컬 모듈

```python
# Example
import sys
import logging
import pandas as pd
import kis_auth as ka
from domestic_stock_functions import *
```

## Type Hints
- 함수 파라미터, 반환값, 변수에 필수 적용
- 복잡한 타입은 `Optional`, `Union`, `List`, `Dict` 사용

```python
def inquire_price(
    env_dv: str,
    fid_cond_mrkt_div_code: str,
    fid_input_iscd: str
) -> dict:
    ...
```

## Docstring
- Google/Sphinx/NumPy 형식 사용
- 목적, 인자, 반환 값, 예외, 예제 코드 포함
- 코드가 "무엇"을 하는지는 코드 자체로, 주석은 "왜"를 설명

## Error Handling
- `try-except` 블록 사용, 구체적인 예외 타입 명시
- `logging` 모듈로 이벤트/에러 기록

## Code Design
- 함수/클래스는 작고 단일 책임
- 추상화나 복잡한 디자인 패턴 지양
- 명확하고 직관적인 코드 선호
- Wildcard import 지양 (필요한 것만 명시적으로)

## Configuration
- API 키, 경로 등은 코드에서 분리
- 설정 파일: `kis_devlp.yaml`

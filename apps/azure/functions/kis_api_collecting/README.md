# KIS Volume Rank Collector Function


## 개요
- 위치: `apps/azure/functions/kis_api_collecting`
- 목적: KIS 오픈 API에서 거래량 상위 30개 종목을 5분마다 수집해 Azure Event Hub (`AnticSignalEventHubName`)로 전송합니다.
- Swift timer 설정: `0 */5 * * * *` (5분 마다 실행, `function_app.py` 참조).

## 주요 파일
- `function_app.py`: 최신 Python Programming Model 기반 엔트리 포인트 (`FunctionApp`)와 타이머 트리거 정의.
- `antic_signal_collect.py`: KIS API 호출, 토큰 발급 및 거래량 순위 데이터 가공 로직.
- `host.json`: Functions 호스트 전역 구성.
- `requirements.txt`: 함수 앱 종속성 목록.
- `local.settings.json`: 로컬 개발용 환경 변수. **실제 키/시크릿은 저장소에 커밋하지 않는 것을 권장합니다.**

## 로컬 실행 방법
1. Azure Functions Core Tools와 Python 3.12 환경을 준비합니다.
2. 필요 시 가상환경 생성 후 의존성 설치:
   ```bash
   cd apps/azure/functions/kis_api_collecting
   python -m venv .venv && source .venv/Scripts/activate
   pip install -r requirements.txt
   ```
3. `local.settings-default.json`에 KIS API Key/Secret, Event Hub 연결 문자열 등을 설정하고, `local.settings.json`으로 수정하여 사용합니다.
4. Functions 실행:
   ```bash
   func start
   ```

## 비고
- 폴더 내 `__azurite*` 파일 및 `__blobstorage__`, `__queuestorage__` 디렉터리는 Azurite 로컬 에뮬레이터가 생성한 개발용 데이터입니다. 필요 시 삭제 후 `func start` 실행 시 다시 생성할 수 있습니다.
- 배포 시에는 `local.settings.json`의 값을 Azure Functions App의 Application Settings로 이전하세요.

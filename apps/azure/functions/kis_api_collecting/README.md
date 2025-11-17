# KIS API Collector Function


## 개요
- 위치: `apps/azure/functions/kis_api_collecting`
- 목적: KIS 오픈 API에서 거래량 상위 30개 종목을 5분마다 수집해 Azure Event Hub (`AnticSignalEventHubName`)로 전송합니다.
- Swift timer 설정: `0 */5 * * * *` (5분 마다 실행, `function_app.py` 참조).

## 주요 파일
- `function_app.py`: 최신 Python Programming Model 기반 엔트리 포인트 (`FunctionApp`)와 타이머 트리거 정의.
- `host.json`: Functions 호스트 전역 구성.
- `requirements.txt`: 함수 앱 종속성 목록.
- `local.settings.json`: 로컬 개발용 환경 변수. **실제 키/시크릿은 저장소에 커밋하지 않는 것을 권장합니다.**

## 공유 패키지 빌드
`requirements.txt`가 `dist/kis_api-<버전>.whl`을 참조하므로, 개발/배포 전에 루트에서 wheel을 생성해야 합니다.
```bash
pip install -U build
./scripts/build_kis_shared.sh        # Linux/Mac
# 또는 PowerShell
powershell -ExecutionPolicy Bypass -File scripts/build_kis_shared.ps1
```
`packages/kis_api`를 수정할 때마다 wheel을 재생성해 주세요.

## 로컬 실행 방법
1. Azure Functions Core Tools와 Python 3.12 환경을 준비합니다 (위 wheel 생성 완료 가정).
2. 필요 시 가상환경 생성 후 의존성 설치:
   ```bash
   cd apps/azure/functions/kis_api_collecting
   python -m venv .venv && source .venv/Scripts/activate  # Linux/Mac: source .venv/bin/activate
   pip install -r requirements.dev.txt
   ```
   `requirements.dev.txt`는 `requirements.txt` + `-e ../../../../packages/kis_api`를 포함해 공유 모듈을 editable 상태로 로드합니다.
3. `local.settings-default.json`에 KIS API Key/Secret, Event Hub 연결 문자열 등을 설정하고, `local.settings.json`으로 복사 후 수정합니다.
4. Functions 실행:
   ```bash
   func start
   ```

## 배포 시 의존성 포함 방법
1. wheel이 최신인지 확인 후 함수 폴더에서 `.python_packages`를 준비:
   ```bash
   cd apps/azure/functions/kis_api_collecting
   python -m pip install -r requirements.txt
   ```
   `requirements.txt`는 `../../../../dist/kis_api-<버전>.whl`을 참조하므로, 해당 wheel이 존재하지 않으면 위 “공유 패키지 빌드” 단계를 먼저 수행해야 합니다.
2. 이후 `func azure functionapp publish <함수앱>`을 실행하면 `.python_packages` 디렉터리가 함께 업로드되어 공유 모듈을 사용할 수 있습니다.

## 비고
- 폴더 내 `__azurite*` 파일 및 `__blobstorage__`, `__queuestorage__` 디렉터리는 Azurite 로컬 에뮬레이터가 생성한 개발용 데이터입니다. 필요 시 삭제 후 `func start` 실행 시 다시 생성할 수 있습니다.
- 배포 시에는 `local.settings.json`의 값을 Azure Functions App의 Application Settings로 이전하세요.

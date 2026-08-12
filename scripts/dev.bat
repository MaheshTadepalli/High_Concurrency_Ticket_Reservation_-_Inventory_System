@echo off
setlocal
cd /d %~dp0

if not exist .venv (
  python -m venv .venv
)
call .venv\Scripts\activate.bat
pip install -q -r requirements.txt

docker compose up -d
echo Waiting for Postgres...
:wait
docker compose exec -T postgres pg_isready -U ticket -d tickets >nul 2>&1
if errorlevel 1 (
  timeout /t 1 /nobreak >nul
  goto wait
)

echo Starting API on http://127.0.0.1:8001 ...
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload

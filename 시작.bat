@echo off
chcp 65001 > nul
echo.
echo =========================================
echo   DB 비교 도구 (Streamlit)
echo =========================================
echo.

:: Python 찾기
set PYTHON=
for %%p in (python python3) do (
    where %%p >nul 2>&1 && set PYTHON=%%p && goto :found
)
echo [오류] Python이 설치되어 있지 않습니다.
echo   winget install Python.Python.3.12
pause
exit /b 1

:found
echo Python: %PYTHON%
echo.
echo [1/2] 패키지 설치 중...
%PYTHON% -m pip install -r requirements.txt -q --no-warn-script-location

echo [2/2] Streamlit 시작 중...
echo.
echo 브라우저에서 자동으로 열립니다.
echo 종료하려면 Ctrl+C 를 누르세요.
echo.
%PYTHON% -m streamlit run app.py
pause

@echo off
REM ============================================================================
REM run-worker-pc.cmd - canonical #899 wrapper template (windows_pc node).
REM Same shape as run-worker-desktop.cmd (read that header first): the lock
REM lives in serve (exit 3 + "ALREADY RUNNING"), this wrapper only backs off,
REM and restarts go through `run-worker-pc.cmd restart` — never through
REM "kill the child + Start-ScheduledTask" (the #899 double-executor incident
REM shape on windows_desktop; the same :loop lives here).
REM ============================================================================

setlocal
set "VENV_PY=C:\Users\ahmed\hermes-cluster\.venv\Scripts\python.exe"
set "CONFIG=C:\Users\ahmed\hermes-cluster\cluster-worker-pc.yaml"
set "TASK_NAME=Hermes-Worker-PC"

if /I "%~1"=="restart" goto :restart
if /I "%~1"=="stop"    goto :stop
if /I "%~1"=="status"  goto :status
goto :loop

:loop
"%VENV_PY%" -m hermes_cluster.serve --config "%CONFIG%"
set "RC=%ERRORLEVEL%"
if "%RC%"=="3" (
    echo [%date% %time%] lock held by a live sibling instance - backing off 60s (#899) 1>&2
    timeout /t 60 /nobreak >nul
    goto :loop
)
if "%RC%"=="4" (
    echo [%date% %time%] single-instance lock ERROR - backing off 300s, check port collisions 1>&2
    timeout /t 300 /nobreak >nul
    goto :loop
)
echo [%date% %time%] worker exited rc=%RC%, relaunching in 5s >> "%~dp0worker-pc.log"
timeout /t 5 /nobreak >nul
goto :loop

:stop
schtasks /End /TN "%TASK_NAME%" >nul 2>&1
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match 'hermes_cluster\.serve' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
exit /b 0

:restart
call :stop
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "for(;;){ $n=@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match 'hermes_cluster\.serve' }).Count; if($n -eq 0){exit 0}; Start-Sleep -Milliseconds 500 }"
if errorlevel 1 (
    echo [%date% %time%] restart aborted: serve processes never reached zero 1>&2
    exit /b 1
)
schtasks /Run /TN "%TASK_NAME%"
exit /b %ERRORLEVEL%

:status
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match 'hermes_cluster\.serve' }).Count"
exit /b 0

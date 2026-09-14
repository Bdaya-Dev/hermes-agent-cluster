@echo off
REM ============================================================================
REM run-worker-desktop.cmd - canonical #899 wrapper template
REM (Windows: windows_desktop node). The fleet's live copy may differ; keep
REM this file authoritative so every re-provisioning starts from the shape
REM below.
REM
REM WHAT #899 changed (measured 2026-09-14: a "restart" that was really
REM Start-ScheduledTask beside a still-looping wrapper produced TWO executors
REM for one node id -> every lane spawned twice, 81 orphans):
REM
REM   1. serve in worker role now takes a per-node-id single-instance lock
REM      (see hermes_cluster/core/single_instance.py) and exits with
REM      errorlevel 3 + "ALREADY RUNNING" on stderr when a live sibling
REM      holds it. THIS WRAPPER BACKS OFF on 3 instead of hammering.
REM   2. NEVER restart the worker by killing the python child and starting
REM      the task: the :loop below relaunches its own child. Use the
REM      documented restart path:
REM          run-worker-desktop.cmd restart
REM      which is: Stop-ScheduledTask -> wait until ZERO serve processes
REM      remain -> Start-ScheduledTask. (README: "Restarting a worker".)
REM ============================================================================

setlocal
set "VENV_PY=C:\Users\ahmed\hermes-cluster\.venv\Scripts\python.exe"
set "CONFIG=C:\Users\ahmed\hermes-cluster\cluster-worker-desktop.yaml"
set "TASK_NAME=Hermes-Worker-Desktop"

if /I "%~1"=="restart" goto :restart
if /I "%~1"=="stop"    goto :stop
if /I "%~1"=="status"  goto :status
goto :loop

REM ---------------------------------------------------------------------------
REM :loop - run the worker forever; back off politely when the lock is held.
REM ---------------------------------------------------------------------------
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
echo [%date% %time%] worker exited rc=%RC%, relaunching in 5s >> "%~dp0worker-desktop.log"
timeout /t 5 /nobreak >nul
goto :loop

REM ---------------------------------------------------------------------------
REM :stop - end the WRAPPER (not just the child), then the scheduled task.
REM Killing serve alone is what doubled the fleet in the #899 incident: the
REM wrapper's :loop relaunched it while the task started a second wrapper.
REM ---------------------------------------------------------------------------
:stop
schtasks /End /TN "%TASK_NAME%" >nul 2>&1
REM kill any wrapper + child trees left behind (this task's own python only)
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match 'hermes_cluster\.serve' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
exit /b 0

REM ---------------------------------------------------------------------------
REM :restart - the ONE documented restart path (brief #899 item 4):
REM stop -> wait for zero serve processes -> start. Never the other way round.
REM ---------------------------------------------------------------------------
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

REM ---------------------------------------------------------------------------
REM :status - count live serve processes (0 = stopped; 2+ = duplicate, #899).
REM ---------------------------------------------------------------------------
:status
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "@(Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match 'hermes_cluster\.serve' }).Count"
exit /b 0

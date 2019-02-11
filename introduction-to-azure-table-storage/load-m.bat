REM
REM Run process in parallel
REM

REM Set path
cd "C:\Community Work\BCC29\TS"


REM Execute the powershell script
start "cmd" powershell.exe -file step3-load-stocks-table-storage.ps1 M
exit /b
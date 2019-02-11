REM
REM Run process in parallel
REM

REM Set path
cd "C:\Community Work\BCC29\TS"

REM Each file that starts with %%i
for %%i in (A B C D E F G H I J K L M N O P Q R S T U V W X Y Z) do call :for_body %%i
exit /b

REM Execute the powershell script
:for_body
    start "cmd" powershell.exe -file step2-nagle-n-load-timings.ps1 %1
exit /b
@echo off
cls

for /f "tokens=1" %%i in ('date /t') do set thedate=%%i

set mm=%thedate:~3,2%
set dd=%thedate:~0,2%
set yyyy=%thedate:~6,4%

REM Change the number below as needed:

set /A dd=%dd% - 1
set /A mm=%mm% + 0

if /I %dd% GTR 0 goto DONE
set /A mm=%mm% - 1
if /I %mm% GTR 0 goto ADJUSTDAY
set /A mm=12
set /A yyyy=%yyyy% - 1

:ADJUSTDAY
if %mm%==1 goto SET31
if %mm%==2 goto LEAPCHK
if %mm%==3 goto SET31
if %mm%==4 goto SET30
if %mm%==5 goto SET31
if %mm%==6 goto SET30
if %mm%==7 goto SET31
if %mm%==8 goto SET31
if %mm%==9 goto SET30
if %mm%==10 goto SET31
if %mm%==11 goto SET30
if %mm%==12 goto SET31

goto ERROR

:SET31
set /A dd=31 + %dd%
goto DONE

:SET30
set /A dd=30 + %dd%
goto DONE

:LEAPCHK
set /A tt=%yyyy% %% 4
if not %tt%==0 goto SET28
set /A tt=%yyyy% %% 100
if not %tt%==0 goto SET29
set /A tt=%yyyy% %% 400
if %tt%==0 goto SET29

:SET28
set /A dd=28 + %dd%
goto DONE

:SET29
set /A dd=29 + %dd%

:DONE
if /i %dd% LSS 10 set dd=0%dd%
if /I %mm% LSS 10 set mm=0%mm%
set fromdate=%yyyy%-%mm%-%dd% 00:00:00
set todate=%yyyy%-%mm%-%dd% 23:59:59

echo fromdate: %fromdate%
echo todate: %todate%
python fb_insight.py '%fromdate%' '%todate%'
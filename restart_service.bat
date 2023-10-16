@echo off

REM Get the current time in HH24:MM format
for /f "tokens=1 delims= " %%t in ('time /t') do set "current_time=%%t"

REM Extract the hour part from the current time
set "hour=%current_time:~0,2%"

echo Stopped at : %date% %time% > "D:\Mausam\Learn\batch\TEMP.txt"
net stop AudioMatchingService

REM Check if the current hour is within the desired range (5 AM to 10 PM)
if %hour% geq 05 if %hour% leq 22 (

    @REM Any command can be use out of `net` and `nssm`
    @REM Better to use net as it is inbuilt command `net` as third party `nssm` might get deleted
    @REM But when any changes occurs nssm is best way to restart
    
    @REM timeout 1
    net start AudioMatchingService
    echo Restarted at : %date% %time% > "D:\Mausam\Learn\batch\TEMP.txt"

    @REM nssm restart AudioMatchingService
) 

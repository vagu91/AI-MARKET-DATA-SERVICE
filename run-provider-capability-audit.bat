@echo off
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run-provider-capability-audit.ps1" %*
exit /b %ERRORLEVEL%

@echo off
REM Windows wrapper for the Energy Domain reports (analog of the Linux run.sh).
REM Runs a script under the venv sitting next to this file, with start/end/exit
REM logging, so Task Scheduler runs leave a readable trail.
REM
REM Usage (from anywhere):
REM   run.bat rigs_ed.py --email
REM   run.bat permits_ed.py --email
REM   run.bat rigs_ed.py            (file only, no email)

setlocal
cd /d "%~dp0"
echo ===== START %DATE% %TIME% : %* =====
".\venv\Scripts\python.exe" %*
set rc=%ERRORLEVEL%
echo ===== END   %DATE% %TIME% : exit %rc% =====
endlocal & exit /b %rc%

@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

if exist build\classes rmdir /s /q build\classes
mkdir build\classes

set "sources="
for /f "tokens=*" %%f in ('dir /s /b src\main\java\*.java') do (
    set "sources=!sources! "%%f""
)

javac -Xlint:all -d build\classes !sources!

if %ERRORLEVEL% equ 0 (
    rem Resources (the document corpus) are loaded from the classpath by every node.
    if exist src\main\resources xcopy /e /i /y /q src\main\resources build\classes >nul
    echo Compiled successfully to %CD%\build\classes
) else (
    echo Compilation failed.
    exit /b %ERRORLEVEL%
)

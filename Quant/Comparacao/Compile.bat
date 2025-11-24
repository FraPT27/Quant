@echo off
echo Compiling with SQLite library...
g++ -std=c++17 main.cpp -lsqlite3 -o app.exe

if %errorlevel% equ 0 (
    echo Compilation successful!
    echo Running application...
    app.exe
) else (
    echo Compilation failed!
    pause
)
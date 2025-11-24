#!/bin/bash
echo "Compiling Finance Analysis Tool..."
g++ -std=c++17 -o app main.cpp ../sqlite3.c -I..
if [ $? -eq 0 ]; then
    echo "Compilation successful!"
    echo "Running application..."
    ./app
else
    echo "Compilation failed!"
    read -p "Press enter to continue..."
fi
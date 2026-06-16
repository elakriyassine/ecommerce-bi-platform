@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo   E-Commerce BI Platform - Kafka 4.3.0 KRaft
echo ============================================================

echo [1/6] Nettoyage des logs...
if exist "C:\kafka\controller-logs" rd /s /q "C:\kafka\controller-logs"
if exist "C:\kafka\broker-logs" rd /s /q "C:\kafka\broker-logs"
mkdir "C:\kafka\controller-logs"
mkdir "C:\kafka\broker-logs"

echo [2/6] Generation UUID...
for /f "tokens=*" %%i in ('C:\kafka\bin\windows\kafka-storage.bat random-uuid 2^>nul') do set UUID=%%i
echo UUID : !UUID!

echo [3/6] Formatage controller...
C:\kafka\bin\windows\kafka-storage.bat format -t !UUID! -c C:\kafka\config\controller.properties --standalone --ignore-formatted 2>nul
echo Formatage controller OK

echo [4/6] Formatage broker...
C:\kafka\bin\windows\kafka-storage.bat format -t !UUID! -c C:\kafka\config\broker.properties --standalone --ignore-formatted 2>nul
echo Formatage broker OK

echo [5/6] Demarrage controller...
start "Kafka-Controller" cmd /c "C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\controller.properties"
echo Attente 25 secondes...
timeout /t 25 /nobreak

echo [6/6] Demarrage broker...
start "Kafka-Broker" cmd /c "C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\broker.properties"
echo Attente 25 secondes...
timeout /t 25 /nobreak

echo Creation des topics...
C:\kafka\bin\windows\kafka-topics.bat --create --topic orders-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic payments-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
C:\kafka\bin\windows\kafka-topics.bat --create --topic user-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo Verification...
C:\kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

echo ============================================================
echo   Kafka pret ! 3 topics crees.
echo ============================================================
pause

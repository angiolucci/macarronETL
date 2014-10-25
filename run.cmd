@echo off
set CLASSPATH=..\..\lib\*
set CLASSPATH=%CLASSPATH%;.
set cmd_type=Main
cd build\classes
java -classpath %CLASSPATH% %cmd_type% %*
cd ..\..

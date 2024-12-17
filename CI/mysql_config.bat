@echo off
rem mysql_config replacement script 
rem based on https://github.com/StrawberryPerl/build-extlibs/blob/master/mysql.special/mysql_config.bat

set ROOT=c:\Program Files\MySQL\MySQL Server 8.0

set XCFLAGS="-I%ROOT%\include"
set XLIBS="-L%ROOT%\lib" -lmysql
set XVERSION=8.0.35
set XPREFIX=%ROOT%..\

for %%p in (%*) do (
  if x%%p == x--cflags     echo %XCFLAGS%
  if x%%p == x--libs       echo %XLIBS%
  if x%%p == x--version    echo %XVERSION%
  if x%%p == x--prefix     echo %XPREFIX%
)

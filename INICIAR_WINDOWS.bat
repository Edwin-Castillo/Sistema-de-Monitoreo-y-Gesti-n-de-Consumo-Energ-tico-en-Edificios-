@echo off
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
 echo Instale Node.js 24 LTS antes de ejecutar este programa.
 pause
 exit /b 1
)
if not exist node_modules (
 call npm ci
 if errorlevel 1 (
  echo No se pudieron instalar las dependencias. Revise la conexion a Internet.
  pause
  exit /b 1
 )
)
echo Abra http://localhost:3000 en su navegador.
echo Mantenga esta ventana abierta durante la demostracion.
call npm start
pause

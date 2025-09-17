@echo off
REM Script de instalación para Windows - Sistema de Conciliación Queirolo

echo 🚀 Instalando Sistema de Conciliación - Santiago Queirolo

REM Verificar si Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python no está instalado. Por favor instálalo primero.
    pause
    exit /b 1
)

REM Verificar si pip está instalado
pip --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ pip no está instalado. Por favor instálalo primero.
    pause
    exit /b 1
)

REM Crear entorno virtual
echo 📦 Creando entorno virtual...
python -m venv venv

REM Activar entorno virtual
echo 🔧 Activando entorno virtual...
call venv\Scripts\activate.bat

REM Instalar dependencias
echo 📚 Instalando dependencias...
pip install -r requirements.txt

REM Crear directorios necesarios
echo 📁 Creando directorios...
if not exist "temp" mkdir temp
if not exist "outputs" mkdir outputs

REM Mensaje de éxito
echo.
echo ✅ ¡Instalación completada!
echo.
echo Para ejecutar la aplicación:
echo 1. Activar entorno virtual: venv\Scripts\activate.bat
echo 2. Ejecutar aplicación: python conciliador.py
echo 3. Abrir navegador: http://localhost:8000
echo.
echo Para usar Docker:
echo docker-compose up --build
echo.
pause

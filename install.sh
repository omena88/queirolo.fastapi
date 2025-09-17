#!/bin/bash

# Script de instalación para Sistema de Conciliación Queirolo
echo "🚀 Instalando Sistema de Conciliación - Santiago Queirolo"

# Verificar si Python está instalado
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 no está instalado. Por favor instálalo primero."
    exit 1
fi

# Verificar si pip está instalado
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 no está instalado. Por favor instálalo primero."
    exit 1
fi

# Crear entorno virtual
echo "📦 Creando entorno virtual..."
python3 -m venv venv

# Activar entorno virtual
echo "🔧 Activando entorno virtual..."
source venv/bin/activate

# Instalar dependencias
echo "📚 Instalando dependencias..."
pip install -r requirements.txt

# Crear directorios necesarios
echo "📁 Creando directorios..."
mkdir -p temp outputs

# Mensaje de éxito
echo ""
echo "✅ ¡Instalación completada!"
echo ""
echo "Para ejecutar la aplicación:"
echo "1. Activar entorno virtual: source venv/bin/activate"
echo "2. Ejecutar aplicación: python conciliador.py"
echo "3. Abrir navegador: http://localhost:8000"
echo ""
echo "Para usar Docker:"
echo "docker-compose up --build"

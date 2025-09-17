# 📊 Sistema de Conciliación - Santiago Queirolo

Sistema completo de conciliación bancaria con interfaz web moderna, desarrollado con **FastAPI** y **HTML/Tailwind CSS**.

## 🚀 Características

- ✅ **Conciliación Multi-Paso** (6 fases como el sistema original)
- ✅ **Soporte para 5 tipos de archivos**: AMEX, DINERS, MC, VISA, PAYU
- ✅ **Detección automática de formato mes-año** (ENE25, FEB26, etc.)
- ✅ **Interfaz web intuitiva** con asistente virtual
- ✅ **Drag & Drop** para carga de archivos
- ✅ **Generación de Excel** con resultados completos
- ✅ **Arquitectura moderna** FastAPI + pandas + xlsxwriter

## 📁 Estructura del Proyecto

```
queirolo.fastapi/
├── conciliador.py          # Backend FastAPI
├── conciliador.html        # Frontend web
├── requirements.txt        # Dependencias Python
├── Dockerfile             # Para despliegue
├── docker-compose.yml     # Para desarrollo local
├── temp/                  # Archivos temporales (auto-creado)
└── outputs/               # Archivos Excel generados (auto-creado)
```

## 🛠️ Instalación Local

### Opción 1: Con Python directamente

```bash
# Clonar el repositorio
git clone https://github.com/omena88/queirolo.fastapi.git
cd queirolo.fastapi

# Crear entorno virtual (recomendado)
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar la aplicación
python conciliador.py
```

### Opción 2: Con Docker Compose (recomendado)

```bash
# Clonar el repositorio
git clone https://github.com/omena88/queirolo.fastapi.git
cd queirolo.fastapi

# Ejecutar con Docker Compose
docker-compose up --build
```

## 🌐 Uso

1. **Abrir navegador**: http://localhost:8000
2. **Seleccionar moneda**: PEN o USD
3. **Cargar extracto principal**: Archivo Excel del banco
4. **Cargar archivos de conciliación**: AMEX, DINERS, MC, VISA, PAYU
5. **Procesar conciliación**: El sistema aplica todas las reglas automáticamente
6. **Descargar Excel**: Archivo con resultados completos

## 🐳 Despliegue en EasyPanel

### 1. Configurar en EasyPanel

```yaml
# Configuración de la aplicación
Name: queirolo-conciliador
Repository: https://github.com/omena88/queirolo.fastapi.git
Branch: main
Build Command: docker build -t queirolo-conciliador .
Port: 8000
```

### 2. Variables de Entorno (Opcional)

```env
PORT=8000
PYTHONPATH=/app
```

### 3. Configuración de Volúmenes (Opcional)

```yaml
Volumes:
  - /app/temp:/tmp/temp
  - /app/outputs:/tmp/outputs
```

## 📋 Dependencias

- **FastAPI**: Framework web moderno
- **Uvicorn**: Servidor ASGI
- **Pandas**: Procesamiento de datos
- **NumPy**: Cálculos numéricos
- **OpenPyXL**: Lectura de archivos Excel
- **XlsxWriter**: Generación de archivos Excel
- **Python-multipart**: Manejo de archivos

## 🔧 Desarrollo

### Estructura de Archivos

- **`conciliador.py`**: Backend con toda la lógica de conciliación
- **`conciliador.html`**: Frontend con interfaz de usuario
- **API Endpoints**:
  - `GET /`: Página principal
  - `POST /api/set-currency`: Configurar moneda
  - `POST /api/upload/extracto`: Subir extracto principal
  - `POST /api/upload/{tipo}`: Subir archivos por tipo
  - `POST /api/reconcile`: Procesar conciliación
  - `GET /api/download/{archivo}`: Descargar resultado

### Lógica de Conciliación

1. **AMEX**: Fase 2 (fecha+monto) y Fase 3 (solo monto)
2. **DINERS**: Fase 1 (fecha+monto), Fase 2 (+2.07), Fase 3 (-5.90)
3. **MC**: Fase 1 (CODCOM+monto), Fase 2 (solo monto), Fase 3 (agrupación)
4. **VISA**: Fase 1 (CODCOM+monto), Fase 2 (agrupación por fecha)
5. **PAYU**: Conciliación directa por monto

## 🏷️ Etiquetado MA-

El sistema detecta automáticamente archivos con formato mes-año (ENE25, FEB26, etc.) y aplica la etiqueta `MA-` en:
- Estados de conciliación
- Referencias cruzadas
- Resultados finales

## 🚨 Solución de Problemas

### Error: "Faltan columnas"
- Verificar que el archivo Excel tenga las columnas requeridas
- Asegurar que el extracto use la fila 5 como encabezado

### Error: "No hay extracto cargado"
- Cargar primero el archivo de extracto principal
- Verificar que el archivo sea .xlsx o .xls

### Error de memoria
- Reducir el tamaño de los archivos
- Reiniciar la aplicación

## 📞 Soporte

Para reportar problemas o solicitar nuevas características, crear un issue en:
https://github.com/omena88/queirolo.fastapi/issues

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

**Desarrollado con ❤️ para Santiago Queirolo**

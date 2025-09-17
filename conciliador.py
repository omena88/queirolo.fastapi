#!/usr/bin/env python3
"""
Sistema de Conciliación Completo
Basado en 5. Conciliador sin validacion.html
Implementa todas las fases de conciliación multi-paso
"""
import pandas as pd
import numpy as np
import os
import re
from datetime import datetime
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from typing import List, Dict, Any, Optional, Tuple
import uuid
import json
import xlsxwriter
from io import BytesIO
from itertools import combinations
import math

app = FastAPI()

# Crear directorios necesarios
os.makedirs("temp", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# Variables globales para almacenar datos
currency = None
extracto_data = None
amex_data = []
diners_data = []
mc_data = []
visa_data = []
payu_data = []
files_info = {
    'amex': [],
    'diners': [],
    'mc': [],
    'visa': [],
    'payu': []
}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("conciliador.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/api/set-currency")
async def set_currency(data: dict):
    global currency
    currency = data["currency"]
    return {"message": f"Moneda {currency} configurada"}

@app.post("/api/upload/extracto")
async def upload_extracto(files: List[UploadFile] = File(...)):
    global extracto_data
    
    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls')):
            continue
            
        # Guardar archivo temporalmente
        temp_file = f"temp/{uuid.uuid4()}_{file.filename}"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            # Leer archivo Excel (fila 5 como header)
            df = pd.read_excel(temp_file, header=4)
            
            # Limpiar nombres de columnas
            df.columns = df.columns.astype(str).str.strip()
            
            # Mapear columnas con nombres flexibles
            column_mapping = {}
            required_patterns = {
                'FECHA': ['FECHA'],
                'DESCRIPCIÓN OPERACIÓN': ['DESCRIPCIÓN OPERACIÓN', 'DESCRIPCION OPERACION', 'DESCRIPCIÓN', 'DESCRIPCION'],
                'MONTO': ['MONTO', 'IMPORTE', 'VALOR'],
                'OPERACIÓN - NÚMERO': ['OPERACIÓN - NÚMERO', 'OPERACION - NUMERO', 'OPERACIÓN NÚMERO', 'OPERACION NUMERO', 'OP NUMERO', 'OP - NUMERO'],
                'REFERENCIA2': ['REFERENCIA2', 'REFERENCIA 2', 'REF2', 'REFERENCIA']
            }
            
            # Buscar columnas por patrones
            for standard_name, patterns in required_patterns.items():
                found = False
                for pattern in patterns:
                    for col in df.columns:
                        if pattern.upper() in str(col).upper():
                            column_mapping[standard_name] = col
                            found = True
                            break
                    if found:
                        break
                
                if not found:
                    # Buscar por similitud parcial
                    for col in df.columns:
                        col_upper = str(col).upper()
                        if any(word in col_upper for word in standard_name.split()):
                            column_mapping[standard_name] = col
                            found = True
                            break
            
            # Verificar que se encontraron todas las columnas
            missing_cols = [col for col in required_patterns.keys() if col not in column_mapping]
            if missing_cols:
                available_cols = list(df.columns)
                raise Exception(f"Faltan columnas: {', '.join(missing_cols)}. Columnas disponibles: {', '.join(available_cols)}")
            
            # Renombrar columnas al estándar
            df = df.rename(columns={v: k for k, v in column_mapping.items()})
            
            # Filtrar por descripción operación
            desc_col = 'DESCRIPCIÓN OPERACIÓN'
            extracto_data = df[
                df[desc_col].astype(str).str.upper().str.contains(
                    'DINERS CLUB|CIA DE SERV|DE PROCESOS DE MEDIOS|DINERS CLUB PERU S|DE PAYU PERU S.A.C|COMPAN', 
                    na=False, regex=True
                )
            ].copy()
            
            # Agregar columnas de control
            extracto_data['ESTADO'] = 'Pendiente'
            extracto_data['#REF'] = ''
            
            print(f"✅ Extracto cargado: {len(extracto_data)} filas")
                
        except Exception as e:
            print(f"❌ Error procesando extracto: {e}")
            raise HTTPException(status_code=400, detail=f"Error: {e}")
        finally:
            os.remove(temp_file)
    
    return {"message": f"Extracto cargado: {len(extracto_data) if extracto_data is not None else 0} registros"}

@app.post("/api/upload/{file_type}")
async def upload_files(file_type: str, files: List[UploadFile] = File(...)):
    global amex_data, diners_data, mc_data, visa_data, payu_data, files_info
    
    processed_count = 0
    
    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls')):
            continue
            
        temp_file = f"temp/{uuid.uuid4()}_{file.filename}"
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        try:
            # Detectar formato mes-año en nombre de archivo
            formato_mes_anio = detectar_formato_mes_anio(file.filename)
            
            # Leer según tipo
            if file_type in ['extracto', 'payu']:
                df = pd.read_excel(temp_file, header=4)
            else:
                df = pd.read_excel(temp_file, header=0)
            
            # Limpiar nombres de columnas
            df.columns = df.columns.astype(str).str.strip()
            
            # Procesar según tipo
            if file_type == 'amex':
                required_cols = ['CODIGO', 'NETO_TOTAL', 'FECHA_ABONO']
                print(f"📄 AMEX - Archivo: {file.filename}")
                print(f"📄 AMEX - Columnas disponibles: {list(df.columns)}")
                print(f"📄 AMEX - Formato MA detectado: {formato_mes_anio}")
                
                # Mapear columnas usando índices como en el original
                header_map = []
                missing_cols = []
                for col in required_cols:
                    found_index = -1
                    for i, df_col in enumerate(df.columns):
                        if col.upper() == str(df_col).upper():
                            found_index = i
                            break
                    if found_index == -1:
                        missing_cols.append(col)
                    else:
                        header_map.append(found_index)
                
                if not missing_cols:
                    # Mapear datos usando índices y filtrar por NETO_TOTAL != 0
                    raw_data = df.values
                    neto_total_idx = header_map[required_cols.index('NETO_TOTAL')]
                    filtered_data = []
                    
                    for row in raw_data:
                        neto_total = convert_to_number(row[neto_total_idx])
                        if not pd.isna(neto_total) and neto_total != 0:
                            # Mapear solo las columnas requeridas
                            mapped_row = [row[idx] for idx in header_map]
                            filtered_data.append(mapped_row)
                    
                    if filtered_data:
                        # Crear DataFrame con solo las columnas requeridas + ESTADO + #REF
                        estado_inicial = 'Pendiente MA' if formato_mes_anio['encontrado'] else 'Pendiente'
                        final_data = []
                        for row in filtered_data:
                            final_row = row + [estado_inicial, '']
                            final_data.append(final_row)
                        
                        # Crear DataFrame final con headers correctos
                        final_headers = required_cols + ['ESTADO', '#REF']
                        df_final = pd.DataFrame(final_data, columns=final_headers)
                        amex_data.append(df_final)
                        
                        # Guardar info del archivo
                        file_info = {
                            'name': file.filename,
                            'formato_mes_anio': formato_mes_anio['encontrado'],
                            'mes': formato_mes_anio.get('mes'),
                            'anio': formato_mes_anio.get('anio'),
                            'rows': len(final_data)
                        }
                        files_info['amex'].extend([file_info] * len(final_data))
                        processed_count += len(final_data)
                        print(f"✅ AMEX procesado: {len(final_data)} registros, Estado: {estado_inicial}")
                    else:
                        print(f"⚠️ AMEX - No hay registros válidos en {file.filename}")
                else:
                    print(f"❌ AMEX - Faltan columnas: {missing_cols}")
                    
            elif file_type == 'diners':
                required_cols = ['CÓDIGO DE COMERCIO', 'ORDEN DE PAGO', 'FECHA DE PAGO', 'IMPORTE NETO DE PAGO']
                print(f"📄 DINERS - Archivo: {file.filename}")
                print(f"📄 DINERS - Columnas disponibles: {list(df.columns)}")
                print(f"📄 DINERS - Formato MA detectado: {formato_mes_anio}")
                
                # Mapear columnas usando índices como en el original (búsqueda flexible para acentos)
                header_map = []
                missing_cols = []
                for col in required_cols:
                    found_index = -1
                    for i, df_col in enumerate(df.columns):
                        # Normalizar ambos strings: quitar acentos, espacios y convertir a mayúsculas
                        col_normalized = col.upper().replace('Ó', 'O').replace('É', 'E').replace('Í', 'I').replace('Á', 'A').replace('Ú', 'U').strip()
                        df_col_normalized = str(df_col).upper().replace('Ó', 'O').replace('É', 'E').replace('Í', 'I').replace('Á', 'A').replace('Ú', 'U').strip()
                        if col_normalized == df_col_normalized:
                            found_index = i
                            break
                    if found_index == -1:
                        missing_cols.append(col)
                    else:
                        header_map.append(found_index)
                
                print(f"📄 DINERS - Header map: {header_map}")
                print(f"📄 DINERS - Missing cols: {missing_cols}")
                
                if not missing_cols:
                    # Filtrar filas que tienen datos en al menos una columna requerida (como en el original)
                    raw_data = df.values
                    filtered_data = []
                    
                    for row in raw_data:
                        if any(row[idx] is not None and row[idx] != '' and str(row[idx]).strip() != '' for idx in header_map):
                            # Mapear solo las columnas requeridas
                            mapped_row = [row[idx] for idx in header_map]
                            filtered_data.append(mapped_row)
                    
                    if filtered_data:
                        # Crear DataFrame con solo las columnas requeridas + ESTADO + #REF
                        estado_inicial = 'Pendiente MA' if formato_mes_anio['encontrado'] else 'Pendiente'
                        final_data = []
                        for row in filtered_data:
                            final_row = row + [estado_inicial, '']
                            final_data.append(final_row)
                        
                        # Crear DataFrame final con headers correctos
                        final_headers = required_cols + ['ESTADO', '#REF']
                        df_final = pd.DataFrame(final_data, columns=final_headers)
                        diners_data.append(df_final)
                        
                        # Guardar info del archivo
                        file_info = {
                            'name': file.filename,
                            'formato_mes_anio': formato_mes_anio['encontrado'],
                            'mes': formato_mes_anio.get('mes'),
                            'anio': formato_mes_anio.get('anio'),
                            'rows': len(final_data)
                        }
                        files_info['diners'].extend([file_info] * len(final_data))
                        processed_count += len(final_data)
                        print(f"✅ DINERS procesado: {len(final_data)} registros, Estado: {estado_inicial}")
                    else:
                        print(f"⚠️ DINERS - No hay filas válidas en {file.filename}")
                else:
                    print(f"❌ DINERS - Faltan columnas: {missing_cols}")
                    
            elif file_type == 'mc':
                required_cols = ['NETO_TOTAL', 'FECHA_ABONO']
                # MC necesita CODCOM que se extrae del nombre del archivo
                print(f"📄 MC - Archivo: {file.filename}")
                print(f"📄 MC - Columnas disponibles: {list(df.columns)}")
                print(f"📄 MC - Formato MA detectado: {formato_mes_anio}")
                
                # Mapear columnas usando índices como en el original
                header_map = []
                missing_cols = []
                for col in required_cols:
                    found_index = -1
                    for i, df_col in enumerate(df.columns):
                        if col.upper() == str(df_col).upper():
                            found_index = i
                            break
                    if found_index == -1:
                        missing_cols.append(col)
                    else:
                        header_map.append(found_index)
                
                if not missing_cols:
                    # Extraer CODCOM del nombre del archivo
                    codcom = file.filename.split('-')[0] if '-' in file.filename else file.filename.split('.')[0]
                    
                    # Mapear datos usando índices y filtrar por NETO_TOTAL != 0
                    raw_data = df.values
                    neto_total_idx = header_map[required_cols.index('NETO_TOTAL')]
                    fecha_abono_idx = header_map[required_cols.index('FECHA_ABONO')]
                    filtered_data = []
                    
                    for row in raw_data:
                        neto_total = convert_to_number(row[neto_total_idx])
                        if not pd.isna(neto_total) and neto_total != 0:
                            # Crear fila con CODCOM + columnas requeridas
                            mapped_row = [codcom, row[neto_total_idx], row[fecha_abono_idx]]
                            filtered_data.append(mapped_row)
                    
                    if filtered_data:
                        # Crear DataFrame con CODCOM + columnas requeridas + ESTADO + #REF
                        estado_inicial = 'Pendiente MA' if formato_mes_anio['encontrado'] else 'Pendiente'
                        final_data = []
                        for row in filtered_data:
                            final_row = row + [estado_inicial, '']
                            final_data.append(final_row)
                        
                        # Headers finales: CODCOM + columnas originales + ESTADO + #REF
                        final_headers = ['CODCOM'] + required_cols + ['ESTADO', '#REF']
                        df_final = pd.DataFrame(final_data, columns=final_headers)
                        mc_data.append(df_final)
                        
                        # Guardar info del archivo
                        file_info = {
                            'name': file.filename,
                            'formato_mes_anio': formato_mes_anio['encontrado'],
                            'mes': formato_mes_anio.get('mes'),
                            'anio': formato_mes_anio.get('anio'),
                            'rows': len(final_data)
                        }
                        files_info['mc'].extend([file_info] * len(final_data))
                        processed_count += len(final_data)
                        print(f"✅ MC procesado: {len(final_data)} registros, Estado: {estado_inicial}")
                    else:
                        print(f"⚠️ MC - No hay registros válidos en {file.filename}")
                else:
                    print(f"❌ MC - Faltan columnas: {missing_cols}")
                    
            elif file_type == 'visa':
                required_cols = ['COMERCIO/CADENA', 'FECHA PROCESO', 'IMPORTE NETO']
                print(f"📄 VISA - Archivo: {file.filename}")
                print(f"📄 VISA - Columnas disponibles: {list(df.columns)}")
                print(f"📄 VISA - Formato MA detectado: {formato_mes_anio}")
                
                # Mapear columnas usando índices como en el original
                header_map = []
                missing_cols = []
                for col in required_cols:
                    found_index = -1
                    for i, df_col in enumerate(df.columns):
                        if col.upper() == str(df_col).upper():
                            found_index = i
                            break
                    if found_index == -1:
                        missing_cols.append(col)
                    else:
                        header_map.append(found_index)
                
                if not missing_cols:
                    # Mapear datos usando índices y filtrar por IMPORTE NETO != 0
                    raw_data = df.values
                    importe_neto_idx = header_map[required_cols.index('IMPORTE NETO')]
                    filtered_data = []
                    
                    for row in raw_data:
                        importe_neto = convert_to_number(row[importe_neto_idx])
                        if not pd.isna(importe_neto) and importe_neto != 0:
                            # Mapear solo las columnas requeridas
                            mapped_row = [row[idx] for idx in header_map]
                            filtered_data.append(mapped_row)
                    
                    if filtered_data:
                        # Crear DataFrame con solo las columnas requeridas + ESTADO + #REF
                        estado_inicial = 'Pendiente MA' if formato_mes_anio['encontrado'] else 'Pendiente'
                        final_data = []
                        for row in filtered_data:
                            final_row = row + [estado_inicial, '']
                            final_data.append(final_row)
                        
                        # Crear DataFrame final con headers correctos
                        final_headers = required_cols + ['ESTADO', '#REF']
                        df_final = pd.DataFrame(final_data, columns=final_headers)
                        visa_data.append(df_final)
                        
                        # Guardar info del archivo
                        file_info = {
                            'name': file.filename,
                            'formato_mes_anio': formato_mes_anio['encontrado'],
                            'mes': formato_mes_anio.get('mes'),
                            'anio': formato_mes_anio.get('anio'),
                            'rows': len(final_data)
                        }
                        files_info['visa'].extend([file_info] * len(final_data))
                        processed_count += len(final_data)
                        print(f"✅ VISA procesado: {len(final_data)} registros, Estado: {estado_inicial}")
                    else:
                        print(f"⚠️ VISA - No hay registros válidos en {file.filename}")
                else:
                    print(f"❌ VISA - Faltan columnas: {missing_cols}")
                    
            elif file_type == 'payu':
                required_cols = ['FECHA', 'DOCUMENTO', 'DESCRIPCION', 'CREDITOS', 'DEBITOS', 'NUEVO SALDO', 'SALDO CONGELADO ANTERIOR', 'SALDO RESERVA', 'SALDO DISPONIBLE']
                print(f"📄 PAYU - Archivo: {file.filename}")
                print(f"📄 PAYU - Columnas disponibles: {list(df.columns)}")
                print(f"📄 PAYU - Formato MA detectado: {formato_mes_anio}")
                
                # Mapear columnas usando índices como en el original
                header_map = []
                missing_cols = []
                for col in required_cols:
                    found_index = -1
                    for i, df_col in enumerate(df.columns):
                        if col.upper() == str(df_col).upper():
                            found_index = i
                            break
                    if found_index == -1:
                        missing_cols.append(col)
                    else:
                        header_map.append(found_index)
                
                if not missing_cols:
                    # Filtrar datos usando índices
                    raw_data = df.values
                    descripcion_idx = header_map[required_cols.index('DESCRIPCION')]
                    debitos_idx = header_map[required_cols.index('DEBITOS')]
                    documento_idx = header_map[required_cols.index('DOCUMENTO')]
                    
                    # Filtrar solo PAYMENT_ORDER con débitos válidos
                    filtered_data = []
                    seen_combinations = set()
                    
                    for row in raw_data:
                        descripcion = str(row[descripcion_idx]).upper() if row[descripcion_idx] else ''
                        debitos = convert_to_number(row[debitos_idx])
                        documento = str(row[documento_idx]) if row[documento_idx] else ''
                        
                        if (descripcion == 'PAYMENT_ORDER [PAYMENT_ORDER]' and 
                            not pd.isna(debitos) and debitos != 0):
                            
                            # Eliminar duplicados por DOCUMENTO + DEBITOS
                            combination_key = f"{documento}_{debitos:.2f}"
                            if combination_key not in seen_combinations:
                                seen_combinations.add(combination_key)
                                # Mapear solo las columnas requeridas
                                mapped_row = [row[idx] for idx in header_map]
                                filtered_data.append(mapped_row)
                    
                    if filtered_data:
                        # Crear DataFrame con solo las columnas requeridas + ESTADO + #REF
                        estado_inicial = 'Pendiente MA' if formato_mes_anio['encontrado'] else 'Pendiente'
                        final_data = []
                        for row in filtered_data:
                            final_row = row + [estado_inicial, '']
                            final_data.append(final_row)
                        
                        # Crear DataFrame final con headers correctos
                        final_headers = required_cols + ['ESTADO', '#REF']
                        df_final = pd.DataFrame(final_data, columns=final_headers)
                        payu_data.append(df_final)
                        
                        # Guardar info del archivo
                        file_info = {
                            'name': file.filename,
                            'formato_mes_anio': formato_mes_anio['encontrado'],
                            'mes': formato_mes_anio.get('mes'),
                            'anio': formato_mes_anio.get('anio'),
                            'rows': len(final_data)
                        }
                        files_info['payu'].extend([file_info] * len(final_data))
                        processed_count += len(final_data)
                        print(f"✅ PAYU procesado: {len(final_data)} registros, Estado: {estado_inicial}")
                    else:
                        print(f"⚠️ PAYU - No hay registros PAYMENT_ORDER válidos en {file.filename}")
                else:
                    print(f"❌ PAYU - Faltan columnas: {missing_cols}")
                    
        except Exception as e:
            print(f"❌ Error procesando {file_type}: {e}")
            raise HTTPException(status_code=400, detail=f"Error procesando {file_type}: {e}")
        finally:
            os.remove(temp_file)
    
    return {"message": f"{file_type.upper()} cargado: {processed_count} registros"}

def detectar_formato_mes_anio(filename: str) -> Dict[str, Any]:
    """Detecta si un nombre de archivo contiene formato mes-año (ENE25, FEB26, etc.)"""
    meses = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SET', 'OCT', 'NOV', 'DIC']
    filename_upper = filename.upper()
    
    for mes in meses:
        pattern = f"{mes}\\d{{2}}"
        matches = re.findall(pattern, filename_upper)
        if matches:
            match = matches[0]
            anio = match[3:]
            return {
                'encontrado': True,
                'mes': mes,
                'anio': anio
            }
    
    return {'encontrado': False}

def convert_to_number(value) -> float:
    """Convierte un valor a número manejando diferentes formatos"""
    if pd.isna(value) or value is None:
        return np.nan
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        if value.strip() == '':
            return np.nan
        # Manejar formatos como "1.190,07" o "1190.07"
        clean_value = value.replace('.', '').replace(',', '.')
        try:
            return float(clean_value)
        except ValueError:
            return np.nan
    
    return np.nan

def parse_date(value):
    """Parsea fechas en diferentes formatos"""
    if pd.isna(value) or value is None:
        return None
    
    if isinstance(value, datetime):
        return value
    
    # Si es número (fecha de Excel)
    if isinstance(value, (int, float)):
        if 40000 < value < 100000:  # Rango típico de fechas Excel
            try:
                return pd.to_datetime('1900-01-01') + pd.Timedelta(days=value-2)
            except:
                return None
    
    # Si es string
    if isinstance(value, str):
        value = value.strip()
        
        # Formato DD/MM/YYYY
        if '/' in value:
            try:
                return pd.to_datetime(value, format='%d/%m/%Y')
            except:
                pass
        
        # Formato YYYYMMDD
        if len(value) == 8 and value.isdigit():
            try:
                return pd.to_datetime(value, format='%Y%m%d')
            except:
                pass
        
        # Formato DDMMYYYY
        if len(value) == 8 and value.isdigit():
            try:
                day = value[:2]
                month = value[2:4]
                year = value[4:8]
                return pd.to_datetime(f"{year}-{month}-{day}")
            except:
                pass
    
    return None

def create_date_key(date_value) -> Optional[str]:
    """Crea una clave de fecha normalizada"""
    parsed = parse_date(date_value)
    if parsed:
        return parsed.strftime('%Y-%m-%d')
    return None

def find_combination_by_sum(records: List[Dict], target_sum: float, max_combinations: int = 3) -> List[Dict]:
    """Encuentra combinaciones de registros que sumen un total específico"""
    # Limitar registros para evitar complejidad
    limited_records = records[:50]
    
    # Buscar coincidencia exacta
    for record in limited_records:
        if abs(record['monto'] - target_sum) < 0.01:
            return [record]
    
    # Buscar combinaciones de 2
    for i in range(min(20, len(limited_records))):
        for j in range(i + 1, min(20, len(limited_records))):
            sum_val = limited_records[i]['monto'] + limited_records[j]['monto']
            if abs(sum_val - target_sum) < 0.01:
                return [limited_records[i], limited_records[j]]
    
    # Buscar combinaciones de 3
    for i in range(min(10, len(limited_records))):
        for j in range(i + 1, min(10, len(limited_records))):
            for k in range(j + 1, min(10, len(limited_records))):
                sum_val = limited_records[i]['monto'] + limited_records[j]['monto'] + limited_records[k]['monto']
                if abs(sum_val - target_sum) < 0.01:
                    return [limited_records[i], limited_records[j], limited_records[k]]
    
    return []

@app.post("/api/reconcile")
async def reconcile():
    global extracto_data, amex_data, diners_data, mc_data, visa_data, payu_data, files_info
    
    if extracto_data is None or len(extracto_data) == 0:
        raise HTTPException(status_code=400, detail="No hay extracto cargado")
    
    try:
        print("🔄 INICIANDO CONCILIACIÓN MULTI-PASO")
        
        # Consolidar archivos
        all_amex = pd.concat(amex_data) if amex_data else pd.DataFrame()
        all_diners = pd.concat(diners_data) if diners_data else pd.DataFrame()
        all_mc = pd.concat(mc_data) if mc_data else pd.DataFrame()
        all_visa = pd.concat(visa_data) if visa_data else pd.DataFrame()
        all_payu = pd.concat(payu_data) if payu_data else pd.DataFrame()
        
        # Realizar conciliación multi-paso
        result = perform_reconciliation_multi_step(
            extracto_data.copy(),
            all_amex.copy() if not all_amex.empty else pd.DataFrame(),
            all_diners.copy() if not all_diners.empty else pd.DataFrame(),
            all_mc.copy() if not all_mc.empty else pd.DataFrame(),
            all_visa.copy() if not all_visa.empty else pd.DataFrame(),
            all_payu.copy() if not all_payu.empty else pd.DataFrame()
        )
        
        # Generar Excel con resultados
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"CONCILIACION_{currency}_{timestamp}.xlsx"
        output_path = f"outputs/{output_filename}"
        
        # Crear Excel con formato
        workbook = xlsxwriter.Workbook(output_path)
        
        # Formatos mejorados
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D7E4BC',
            'border': 1,
            'text_wrap': True,
            'valign': 'top'
        })
        
        pending_format = workbook.add_format({
            'bg_color': '#FFE6E6',
            'border': 1
        })
        
        conciliated_format = workbook.add_format({
            'bg_color': '#E6FFE6',
            'border': 1
        })
        
        ma_format = workbook.add_format({
            'bg_color': '#FFF2CC',
            'border': 1
        })
        
        # Escribir hojas
        sheets_data = [
            ('EXTRACTO', result['extracto']),
            ('AMEX', result['amex']),
            ('DINERS', result['diners']),
            ('MC', result['mc']),
            ('VISA', result['visa']),
            ('PAYU', result['payu'])
        ]
        
        for sheet_name, data in sheets_data:
            if data is not None and not data.empty:
                ws = workbook.add_worksheet(sheet_name)
                
                # Escribir headers
                for col, header in enumerate(data.columns):
                    ws.write(0, col, header, header_format)
                
                # Escribir datos con formato condicional
                for row_idx, (_, data_row) in enumerate(data.iterrows(), 1):
                    # Determinar formato de fila basado en ESTADO
                    row_format = None
                    if 'ESTADO' in data.columns:
                        estado_value = str(data_row['ESTADO'])
                        if 'Pendiente' in estado_value and 'MA' not in estado_value:
                            row_format = pending_format
                        elif 'Conciliado' in estado_value or 'CONCILIADO' in estado_value:
                            row_format = conciliated_format
                        elif 'MA' in estado_value:
                            row_format = ma_format
                    
                    # Escribir datos de la fila
                    for col_idx, value in enumerate(data_row):
                        formatted_value = str(value) if pd.notna(value) else ''
                        if row_format:
                            ws.write(row_idx, col_idx, formatted_value, row_format)
                        else:
                            ws.write(row_idx, col_idx, formatted_value)
                
                # Agregar filtro automático
                if len(data) > 0:
                    ws.autofilter(0, 0, len(data), len(data.columns) - 1)
                
                # Fijar primera fila (headers) - CRÍTICO: debe ir después de escribir datos
                ws.freeze_panes(1, 0)
                
                # Ajustar ancho de columnas
                for col_idx, header in enumerate(data.columns):
                    max_length = max(len(str(header)), 
                                   data.iloc[:, col_idx].astype(str).str.len().max() if len(data) > 0 else 0)
                    ws.set_column(col_idx, col_idx, min(max_length + 2, 50))
        
        workbook.close()
        
        # Calcular estadísticas
        total_extracto = len(result['extracto'])
        conciliados = len(result['extracto'][~result['extracto']['ESTADO'].str.startswith('Pendiente')])
        
        print(f"✅ CONCILIACIÓN COMPLETADA: {conciliados}/{total_extracto} registros conciliados")
        
        return {
            "message": "Conciliación completada",
            "stats": {
                "extracto_total": total_extracto,
                "conciliados": conciliados,
                "pendientes": total_extracto - conciliados
            },
            "download_url": f"/api/download/{output_filename}"
        }
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def perform_reconciliation_multi_step(extracto_df, amex_df, diners_df, mc_df, visa_df, payu_df):
    """Realiza la conciliación multi-paso siguiendo EXACTAMENTE la lógica del archivo original"""
    print("🔄 INICIANDO CONCILIACIÓN MULTI-PASO")
    
    # Contadores
    stats = {
        'amex_f2': 0, 'amex_f3': 0,
        'diners_f1': 0, 'diners_f2': 0, 'diners_f3': 0,
        'mc_f1': 0, 'mc_f2': 0, 'mc_f3': 0,
        'visa_f1': 0, 'visa_f2': 0,
        'payu': 0
    }
    
    # PASO 1: Conciliación AMEX (2 fases)
    if not amex_df.empty:
        print("💳 PASO 1: Conciliando AMEX")
        
        # Fase 2: Por fecha y monto
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            fecha_ext = parse_date(ext_row['FECHA'])
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if fecha_ext and not np.isnan(monto_ext):
                fecha_key = fecha_ext.strftime('%Y-%m-%d')
                
                # Buscar en AMEX
                for amex_idx, amex_row in amex_df.iterrows():
                    if not amex_row['ESTADO'].startswith('Pendiente'):
                        continue
                        
                    fecha_amex = parse_date(amex_row['FECHA_ABONO'])
                    monto_amex = convert_to_number(amex_row['NETO_TOTAL'])
                    
                    if fecha_amex and not np.isnan(monto_amex):
                        fecha_amex_key = fecha_amex.strftime('%Y-%m-%d')
                        
                        if fecha_key == fecha_amex_key and abs(monto_ext - monto_amex) < 0.01:
                            # Conciliar
                            etiqueta = 'MA-' if amex_row['ESTADO'] == 'Pendiente MA' else ''
                            extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P2-F2-Conciliado'
                            extracto_df.at[idx, '#REF'] = f'{etiqueta}{amex_row["CODIGO"]} - {fecha_key}'
                            amex_df.at[amex_idx, 'ESTADO'] = f'{etiqueta}P2-F2-Conciliado'
                            amex_df.at[amex_idx, '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - {fecha_key}'
                            stats['amex_f2'] += 1
                            break
        
        # Fase 3: Solo por monto (fechas diferentes)
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if not np.isnan(monto_ext):
                # Buscar en AMEX pendientes
                for amex_idx, amex_row in amex_df.iterrows():
                    if not amex_row['ESTADO'].startswith('Pendiente'):
                        continue
                        
                    monto_amex = convert_to_number(amex_row['NETO_TOTAL'])
                    
                    if not np.isnan(monto_amex) and abs(monto_ext - monto_amex) < 0.01:
                        # Conciliar
                        etiqueta = 'MA-' if amex_row['ESTADO'] == 'Pendiente MA' else ''
                        extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P2-F3-Conciliado'
                        extracto_df.at[idx, '#REF'] = f'{etiqueta}{amex_row["CODIGO"]} - Monto: {monto_ext:.2f}'
                        amex_df.at[amex_idx, 'ESTADO'] = f'{etiqueta}P2-F3-Conciliado'
                        amex_df.at[amex_idx, '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - Monto: {monto_ext:.2f}'
                        stats['amex_f3'] += 1
                        break
    
    # PASO 2: Conciliación DINERS (3 fases)
    if not diners_df.empty:
        print("🏦 PASO 2: Conciliando DINERS")
        
        # Agrupar DINERS por orden de pago y fecha
        diners_groups = {}
        for idx, diners_row in diners_df.iterrows():
            if not diners_row['ESTADO'].startswith('Pendiente'):
                continue
                
            orden_pago = str(diners_row['ORDEN DE PAGO']).strip()
            if len(orden_pago) >= 10:
                last10 = orden_pago[-10:]
                fecha_pago = parse_date(diners_row['FECHA DE PAGO'])
                
                if fecha_pago:
                    fecha_key = fecha_pago.strftime('%Y-%m-%d')
                    group_key = f"{last10}_{fecha_key}"
                    
                    if group_key not in diners_groups:
                        diners_groups[group_key] = []
                    
                    diners_groups[group_key].append({
                        'idx': idx,
                        'row': diners_row,
                        'monto': convert_to_number(diners_row['IMPORTE NETO DE PAGO'])
                    })
        
        # Fase 1: Por fecha y monto exacto
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            fecha_ext = parse_date(ext_row['FECHA'])
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if fecha_ext and not np.isnan(monto_ext):
                fecha_ext_key = fecha_ext.strftime('%Y-%m-%d')
                
                # Buscar grupo DINERS que coincida
                for group_key, group_items in list(diners_groups.items()):
                    if fecha_ext_key in group_key:
                        total_grupo = sum(item['monto'] for item in group_items if not np.isnan(item['monto']))
                        
                        if abs(monto_ext - total_grupo) < 0.01:
                            # Conciliar
                            es_ma = any(item['row']['ESTADO'] == 'Pendiente MA' for item in group_items)
                            etiqueta = 'MA-' if es_ma else ''
                            
                            extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P3-F1-Conciliado'
                            extracto_df.at[idx, '#REF'] = f'{etiqueta}DINERS - {fecha_ext_key}'
                            
                            for item in group_items:
                                diners_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P3-F1-Conciliado'
                                diners_df.at[item['idx'], '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - {fecha_ext_key}'
                            
                            del diners_groups[group_key]
                            stats['diners_f1'] += 1
                            break
        
        # Fase 2: Monto + 2.07
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if not np.isnan(monto_ext):
                monto_ajustado = monto_ext + 2.07
                
                # Buscar grupo DINERS que coincida con monto ajustado
                for group_key, group_items in list(diners_groups.items()):
                    total_grupo = sum(item['monto'] for item in group_items if not np.isnan(item['monto']))
                    
                    if abs(monto_ajustado - total_grupo) < 0.01:
                        # Conciliar
                        es_ma = any(item['row']['ESTADO'] == 'Pendiente MA' for item in group_items)
                        etiqueta = 'MA-' if es_ma else ''
                        
                        extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P3-F2-Conciliado'
                        extracto_df.at[idx, '#REF'] = f'{etiqueta}DINERS - Monto: {monto_ext:.2f} + 2.07'
                        
                        for item in group_items:
                            diners_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P3-F2-Conciliado'
                            diners_df.at[item['idx'], '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - Ajustado'
                        
                        del diners_groups[group_key]
                        stats['diners_f2'] += 1
                        break
        
        # Fase 3: Restar 5.90 a DINERS pendientes
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if not np.isnan(monto_ext):
                # Buscar grupo DINERS que coincida restando 5.90 al total DINERS
                for group_key, group_items in list(diners_groups.items()):
                    total_grupo = sum(item['monto'] for item in group_items if not np.isnan(item['monto']))
                    monto_ajustado_diners = total_grupo - 5.90
                    
                    if abs(monto_ext - monto_ajustado_diners) < 0.01:
                        # Conciliar
                        es_ma = any(item['row']['ESTADO'] == 'Pendiente MA' for item in group_items)
                        etiqueta = 'MA-' if es_ma else ''
                        
                        extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P3-F3-Conciliado'
                        extracto_df.at[idx, '#REF'] = f'{etiqueta}DINERS - Extracto: {monto_ext:.2f} = DINERS: {total_grupo:.2f} - 5.90'
                        
                        for item in group_items:
                            diners_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P3-F3-Conciliado'
                            diners_df.at[item['idx'], '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - Ajustado: {total_grupo:.2f} - 5.90'
                        
                        del diners_groups[group_key]
                        stats['diners_f3'] += 1
                        break
    
    # PASO 3: Conciliación MC (3 fases)
    if not mc_df.empty:
        print("💳 PASO 3: Conciliando MC")
        
        # Fase 1: Por CODCOM + MONTO
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            referencia2 = str(ext_row.get('REFERENCIA2', '')).strip()
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if referencia2 and not np.isnan(monto_ext):
                # Extraer código de comercio (9 dígitos, tomar últimos 7)
                match = re.search(r'(\d{9})', referencia2)
                if match:
                    full_code = match.group(1)
                    codcom_key = full_code[-7:]
                    
                    # Buscar en MC
                    for mc_idx, mc_row in mc_df.iterrows():
                        if (not mc_row['ESTADO'].startswith('Pendiente') or 
                            mc_row['CODCOM'] != codcom_key):
                            continue
                            
                        monto_mc = convert_to_number(mc_row['NETO_TOTAL'])
                        
                        if not np.isnan(monto_mc) and abs(monto_ext - monto_mc) < 0.01:
                            # Conciliar
                            etiqueta = 'MA-' if mc_row['ESTADO'] == 'Pendiente MA' else ''
                            extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P4-F1-Conciliado'
                            extracto_df.at[idx, '#REF'] = f'{etiqueta}MC-{codcom_key}'
                            mc_df.at[mc_idx, 'ESTADO'] = f'{etiqueta}P4-F1-Conciliado'
                            mc_df.at[mc_idx, '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]}'
                            stats['mc_f1'] += 1
                            break
        
        # Fase 2: Solo por monto
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if not np.isnan(monto_ext):
                # Buscar en MC pendientes
                for mc_idx, mc_row in mc_df.iterrows():
                    if not mc_row['ESTADO'].startswith('Pendiente'):
                        continue
                        
                    monto_mc = convert_to_number(mc_row['NETO_TOTAL'])
                    
                    if not np.isnan(monto_mc) and abs(monto_ext - monto_mc) < 0.01:
                        # Conciliar
                        etiqueta = 'MA-' if mc_row['ESTADO'] == 'Pendiente MA' else ''
                        extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P4-F2-Conciliado'
                        extracto_df.at[idx, '#REF'] = f'{etiqueta}MC-Monto: {monto_ext:.2f}'
                        mc_df.at[mc_idx, 'ESTADO'] = f'{etiqueta}P4-F2-Conciliado'
                        mc_df.at[mc_idx, '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]}'
                        stats['mc_f2'] += 1
                        break
        
        # Fase 3: Agrupación por fecha del extracto vs MC pendientes
        extracto_fecha_groups = {}
        
        # Agrupar extracto pendiente por fecha
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            fecha_ext = parse_date(ext_row['FECHA'])
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if fecha_ext and not np.isnan(monto_ext):
                fecha_key = fecha_ext.strftime('%Y-%m-%d')
                
                if fecha_key not in extracto_fecha_groups:
                    extracto_fecha_groups[fecha_key] = {'total': 0, 'items': []}
                
                extracto_fecha_groups[fecha_key]['total'] += monto_ext
                extracto_fecha_groups[fecha_key]['items'].append({'idx': idx, 'row': ext_row})
        
        # Crear lista de MC pendientes
        mc_pendientes = []
        for mc_idx, mc_row in mc_df.iterrows():
            if mc_row['ESTADO'].startswith('Pendiente'):
                monto_mc = convert_to_number(mc_row['NETO_TOTAL'])
                if not np.isnan(monto_mc):
                    mc_pendientes.append({
                        'idx': mc_idx,
                        'row': mc_row,
                        'monto': monto_mc
                    })
        
        # Conciliar totales de fecha extracto vs combinaciones MC
        for fecha_key, fecha_group in extracto_fecha_groups.items():
            total_extracto = fecha_group['total']
            
            # Buscar combinaciones de MC que sumen este total
            mc_combination = find_combination_by_sum(mc_pendientes, total_extracto)
            
            if mc_combination:
                # Verificar si algún registro MC tiene estado 'Pendiente MA'
                es_ma = any(mc_record['row']['ESTADO'] == 'Pendiente MA' for mc_record in mc_combination)
                etiqueta = 'MA-' if es_ma else ''
                
                # Marcar extracto como conciliado
                for item in fecha_group['items']:
                    extracto_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P4-F3-Conciliado'
                    extracto_df.at[item['idx'], '#REF'] = f'{etiqueta}MC-Fecha: {fecha_key} - Total: {total_extracto:.2f}'
                
                # Marcar MC como conciliado
                for mc_record in mc_combination:
                    mc_etiqueta = 'MA-' if mc_record['row']['ESTADO'] == 'Pendiente MA' else ''
                    mc_df.at[mc_record['idx'], 'ESTADO'] = f'{mc_etiqueta}P4-F3-Conciliado'
                    mc_df.at[mc_record['idx'], '#REF'] = f'{mc_etiqueta}Extracto-Fecha: {fecha_key} - Total: {total_extracto:.2f}'
                    
                    # Remover de pendientes para evitar reutilización
                    mc_pendientes = [mc for mc in mc_pendientes if mc['idx'] != mc_record['idx']]
                
                stats['mc_f3'] += len(fecha_group['items'])
    
    # PASO 4: Conciliación VISA (2 fases)
    if not visa_df.empty:
        print("🏦 PASO 4: Conciliando VISA")
        
        # Primero, agrupar VISA por comercio y fecha proceso
        visa_groups = {}
        for visa_idx, visa_row in visa_df.iterrows():
            if not visa_row['ESTADO'].startswith('Pendiente'):
                continue
                
            comercio = str(visa_row.get('COMERCIO/CADENA', '')).strip()
            fecha_proceso = parse_date(visa_row['FECHA PROCESO'])
            monto_visa = convert_to_number(visa_row['IMPORTE NETO'])
            
            if comercio and fecha_proceso and not np.isnan(monto_visa):
                fecha_key = fecha_proceso.strftime('%Y-%m-%d')
                group_key = f"{comercio}_{fecha_key}"
                
                if group_key not in visa_groups:
                    visa_groups[group_key] = {'total': 0, 'items': [], 'comercio': comercio, 'fecha': fecha_key}
                
                visa_groups[group_key]['total'] += monto_visa
                visa_groups[group_key]['items'].append({'idx': visa_idx, 'row': visa_row})
        
        # Fase 1: Línea de extracto vs Grupos totalizados de VISA
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            referencia2 = str(ext_row.get('REFERENCIA2', '')).strip()
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if referencia2 and not np.isnan(monto_ext):
                # Extraer código de comercio
                match = re.search(r'(\d{9})', referencia2)
                if match:
                    full_code = match.group(1)
                    codcom_key = full_code[-7:]
                    
                    # Buscar grupo VISA que coincida con el comercio y monto
                    for group_key, visa_group in list(visa_groups.items()):
                        if (codcom_key in visa_group['comercio'] and 
                            abs(monto_ext - visa_group['total']) < 0.01):
                            
                            # Verificar si algún registro VISA del grupo tiene estado 'Pendiente MA'
                            es_ma = any(item['row']['ESTADO'] == 'Pendiente MA' for item in visa_group['items'])
                            etiqueta = 'MA-' if es_ma else ''
                            
                            # Conciliar extracto
                            extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P5-F1-Conciliado'
                            extracto_df.at[idx, '#REF'] = f'{etiqueta}VISA-{codcom_key} - {visa_group["fecha"]}'
                            
                            # Conciliar todos los registros VISA del grupo
                            for item in visa_group['items']:
                                visa_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P5-F1-Conciliado'
                                visa_df.at[item['idx'], '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]} - {visa_group["fecha"]}'
                            
                            del visa_groups[group_key]
                            stats['visa_f1'] += 1
                            break
        
        # Fase 2: Extracto agrupado por fecha y comercio vs grupos VISA
        extracto_visa_groups = {}
        
        # Agrupar extracto pendiente por fecha y comercio
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            fecha_ext = parse_date(ext_row['FECHA'])
            monto_ext = convert_to_number(ext_row['MONTO'])
            referencia2 = str(ext_row.get('REFERENCIA2', '')).strip()
            
            if fecha_ext and not np.isnan(monto_ext) and referencia2:
                # Extraer código de comercio
                match = re.search(r'(\d{9})', referencia2)
                if match:
                    full_code = match.group(1)
                    codcom_key = full_code[-7:]
                    fecha_key = fecha_ext.strftime('%Y-%m-%d')
                    group_key = f"{codcom_key}_{fecha_key}"
                    
                    if group_key not in extracto_visa_groups:
                        extracto_visa_groups[group_key] = {'total': 0, 'items': [], 'codcom': codcom_key, 'fecha': fecha_key}
                    
                    extracto_visa_groups[group_key]['total'] += monto_ext
                    extracto_visa_groups[group_key]['items'].append({'idx': idx, 'row': ext_row})
        
        # Comparar grupos del extracto con grupos VISA restantes
        for ext_group_key, ext_group in extracto_visa_groups.items():
            # Buscar grupo VISA con el mismo comercio y monto total (fechas pueden ser diferentes)
            for visa_group_key, visa_group in list(visa_groups.items()):
                if (ext_group['codcom'] in visa_group['comercio'] and 
                    abs(ext_group['total'] - visa_group['total']) < 0.01):
                    
                    # Verificar si algún registro VISA del grupo tiene estado 'Pendiente MA'
                    es_ma = any(item['row']['ESTADO'] == 'Pendiente MA' for item in visa_group['items'])
                    etiqueta = 'MA-' if es_ma else ''
                    
                    # Conciliar todos los registros del extracto
                    for item in ext_group['items']:
                        extracto_df.at[item['idx'], 'ESTADO'] = f'{etiqueta}P5-F2-Conciliado'
                        extracto_df.at[item['idx'], '#REF'] = f'{etiqueta}VISA-{ext_group["codcom"]} - Monto: {ext_group["total"]:.2f} ({ext_group["fecha"]}→{visa_group["fecha"]})'
                    
                    # Conciliar todos los registros VISA del grupo
                    for item in visa_group['items']:
                        visa_etiqueta = 'MA-' if item['row']['ESTADO'] == 'Pendiente MA' else ''
                        visa_df.at[item['idx'], 'ESTADO'] = f'{visa_etiqueta}P5-F2-Conciliado'
                        visa_df.at[item['idx'], '#REF'] = f'{visa_etiqueta}{ext_group["items"][0]["row"]["OPERACIÓN - NÚMERO"]} - Monto: {ext_group["total"]:.2f} ({ext_group["fecha"]}→{visa_group["fecha"]})'
                    
                    del visa_groups[visa_group_key]
                    stats['visa_f2'] += 1
                    break
    
    # PASO 5: Conciliación PAYU
    if not payu_df.empty:
        print("💰 PASO 5: Conciliando PAYU")
        
        for idx, ext_row in extracto_df.iterrows():
            if not ext_row['ESTADO'].startswith('Pendiente'):
                continue
                
            monto_ext = convert_to_number(ext_row['MONTO'])
            
            if not np.isnan(monto_ext):
                # Buscar en PAYU
                for payu_idx, payu_row in payu_df.iterrows():
                    if not payu_row['ESTADO'].startswith('Pendiente'):
                        continue
                        
                    debitos_payu = abs(convert_to_number(payu_row['DEBITOS']))
                    
                    if not np.isnan(debitos_payu) and abs(monto_ext - debitos_payu) < 0.01:
                        # Conciliar
                        etiqueta = 'MA-' if payu_row['ESTADO'] == 'Pendiente MA' else ''
                        extracto_df.at[idx, 'ESTADO'] = f'{etiqueta}P6-Conciliado'
                        extracto_df.at[idx, '#REF'] = f'{etiqueta}PAYU - Monto: {monto_ext:.2f}'
                        payu_df.at[payu_idx, 'ESTADO'] = f'{etiqueta}P6-Conciliado'
                        payu_df.at[payu_idx, '#REF'] = f'{etiqueta}{ext_row["OPERACIÓN - NÚMERO"]}'
                        stats['payu'] += 1
                        break
    
    print(f"✅ Conciliación completada. Estadísticas: {stats}")
    
    return {
        'extracto': extracto_df,
        'amex': amex_df if not amex_df.empty else None,
        'diners': diners_df if not diners_df.empty else None,
        'mc': mc_df if not mc_df.empty else None,
        'visa': visa_df if not visa_df.empty else None,
        'payu': payu_df if not payu_df.empty else None,
        'stats': stats
    }

@app.get("/api/download/{filename}")
async def download_file(filename: str):
    file_path = f"outputs/{filename}"
    if os.path.exists(file_path):
        # Crear respuesta de descarga
        response = FileResponse(
            path=file_path,
            filename=filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        # Programar eliminación del archivo después de la descarga
        import threading
        import time
        
        def delete_file_after_delay():
            time.sleep(5)  # Esperar 5 segundos para asegurar que la descarga termine
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"🗑️ Archivo eliminado automáticamente: {filename}")
            except Exception as e:
                print(f"⚠️ Error eliminando archivo {filename}: {e}")
        
        # Ejecutar eliminación en hilo separado
        threading.Thread(target=delete_file_after_delay, daemon=True).start()
        
        return response
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

if __name__ == "__main__":
    import uvicorn
    print("🚀 Iniciando Sistema de Conciliación Simple...")
    print("📍 URL: http://localhost:8000")
    uvicorn.run(app, host="127.0.0.1", port=8000)

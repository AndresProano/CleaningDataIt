import pandas as pd
import re
from datetime import datetime
from pathlib import Path

INPUT = Path("datos.csv")
OUTPUT = Path("datos_completos_power_bi.csv")

def convertir_fecha_a_datetime(fecha_texto):
    """Convierte fecha de texto a formato datetime"""
    if not fecha_texto or fecha_texto.strip() == "":
        return None
    
    try:
        # Formato: M/D/YYYY H:MM:SS AM/PM
        fecha_dt = datetime.strptime(fecha_texto.strip(), '%m/%d/%Y %I:%M:%S %p')
        return fecha_dt
    except ValueError:
        try:
            # Formato alternativo: ISO (2025-08-15T15:34:50.1430981)
            if 'T' in fecha_texto:
                fecha_clean = fecha_texto.split('T')[0] + ' ' + fecha_texto.split('T')[1].split('.')[0]
                fecha_dt = datetime.strptime(fecha_clean, '%Y-%m-%d %H:%M:%S')
                return fecha_dt
        except ValueError:
            pass
    
    return None

def extraer_fechas_principales(texto_completo):
    """Extrae las dos fechas principales de cada solicitud"""
    # Patrón para fechas: M/D/YYYY H:MM:SS AM/PM
    fecha_pattern = r'(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM))'
    
    fechas_encontradas = re.findall(fecha_pattern, texto_completo, re.IGNORECASE)
    
    fecha_respuesta = ""
    fecha_creacion = ""
    
    if len(fechas_encontradas) >= 1:
        fecha_creacion = fechas_encontradas[0]  # Primera fecha = respuesta/acción
    
    if len(fechas_encontradas) >= 2:
        fecha_respuesta = fechas_encontradas[1]   # Segunda fecha = creación/ingreso

    return fecha_respuesta, fecha_creacion

def extraer_fecha_ingreso_iso(bloque_texto):
    """Extrae la fecha de ingreso en formato ISO si existe"""
    # Busca patrón: - Fecha de ingreso:................  2025-08-15T15:34:50.1430981
    iso_pattern = r'- Fecha de ingreso:\.*\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)'
    
    match = re.search(iso_pattern, bloque_texto)
    if match:
        return match.group(1)
    
    return ""

def extraer_contenido_comillas(texto):
    """Extrae todo el contenido que está dentro de comillas dobles"""
    contenido = []
    i = 0
    while i < len(texto):
        if texto[i] == '"':
            i += 1  # salta la comilla inicial
            inicio = i
            while i < len(texto):
                if texto[i] == '"':
                    if i + 1 < len(texto) and texto[i + 1] == '"':
                        i += 2  # salta comillas de escape
                        continue
                    else:
                        contenido.append(texto[inicio:i])
                        i += 1
                        break
                else:
                    i += 1
        else:
            i += 1
    return contenido

def extraer_datos_fuera_comillas(bloque_texto):
    """Extrae datos estructurados que están fuera de comillas"""
    datos = {}
    
    # Busca patrones específicos fuera de comillas
    patrones = {
        'realizada_por': r'Realizada por:\s*([^;]+)',
        'area': r'Área:\s*([^;]+)',
        'ticket': r'Ticket de referencia del Service Desk:\s*([^;]+)',
        'ambiente': r'Ambiente:\s*([^;]+)',
        'servicio': r'Servicio:\s*([^;]+)',
        'servidores': r'Servidores:\s*([^;]+)',
        'nombre_solicitante': r'- Nombre:\.*\s*([^;]+)',
        'correo_solicitante': r'- Correo:\.*\s*([^;]+)',
        'cargo_solicitante': r'- Cargo:\.*\s*([^;]+)',
        'fecha_ingreso': r'- Fecha de ingreso:\.*\s*([^;]+)',
        'coordinacion': r'COORDINACIÓN QUIEN SOLICITA:\s*([^;]+)',
        'autoridad': r'AUTORIDAD[^:]*:\s*([^;]+)'
    }
    
    for clave, patron in patrones.items():
        match = re.search(patron, bloque_texto, re.IGNORECASE)
        if match:
            valor = match.group(1).strip()
            # Limpia caracteres problemáticos
            valor = valor.replace('"', '').strip()
            datos[clave] = valor
    
    return datos

def procesar_bloque_solicitud(lineas_bloque):
    """Procesa un bloque completo de solicitud (dentro y fuera de comillas)"""
    # Une todo el bloque en un solo texto
    texto_completo = '\n'.join(lineas_bloque)
    
    # Extrae contenido de comillas
    contenido_comillas = extraer_contenido_comillas(texto_completo)
    
    # Extrae datos fuera de comillas
    datos_externos = extraer_datos_fuera_comillas(texto_completo)
    
    # Identifica componentes principales
    titulo = ""
    details = ""
    file_link = ""
    
    if contenido_comillas:
        # El primer fragmento suele ser título + detalles iniciales
        if any(palabra in contenido_comillas[0] for palabra in ["SOLICITUD", "Solicitud de paso", "PUBLICACIÓN", "ANALISIS", "CDC"]):
            if "," in contenido_comillas[0]:
                partes = contenido_comillas[0].split(",", 1)
                titulo = partes[0].strip()
                details = partes[1].strip() if len(partes) > 1 else ""
            else:
                titulo = contenido_comillas[0].strip()
        
        # Busca links en otros fragmentos
        for fragmento in contenido_comillas:
            if "https://" in fragmento:
                file_link = re.search(r'(https?://[^\s,;"\']+)', fragmento)
                file_link = file_link.group(1) if file_link else ""
                break
    
    # Extrae las DOS fechas principales de la línea
    fecha_respuesta, fecha_creacion = extraer_fechas_principales(texto_completo)
    fecha_ingreso_iso = extraer_fecha_ingreso_iso(texto_completo)
    
    # Status
    status_match = re.search(r'\b(Completed|Approved|Rejected|Requested)\b', texto_completo)
    status = status_match.group(1) if status_match else ""

    # Comentario
    comentario_match = re.search(r'Comment:\s*([^|,]+)', texto_completo)
    comentario = comentario_match.group(1).strip() if comentario_match else ""
    if not comentario:
        if "Aprobado" in texto_completo:
            comentario = "Aprobado"
        elif "Approve" in texto_completo:
            comentario = "Approve"
    
    # Enviado por/a
    enviado_por = datos_externos.get('realizada_por', "")
    enviado_a_match = re.search(r'Cindy Belén Espinoza Aguirre|Leonardo Herrera Gómez|Freddy Guzmán Martínez|Claudia Arcos Obando', texto_completo)
    enviado_a = enviado_a_match.group(0) if enviado_a_match else ""
    
    return {
        'title': titulo,
        'details': details,
        'file': file_link,
        'status': status,
        'stage': "",  # Vacío por defecto
        'source': datos_externos.get('area', ""),
        'create_at': fecha_creacion,
        'sent_by': enviado_por,
        'sent_to': enviado_a,
        'custom_responses': comentario,
        # Campos adicionales extraídos
        'ticket': datos_externos.get('ticket', ""),
        'nombre_solicitante': datos_externos.get('nombre_solicitante', ""),
        'correo_solicitante': datos_externos.get('correo_solicitante', ""),
        'cargo_solicitante': datos_externos.get('cargo_solicitante', ""),
        'fecha_ingreso': datos_externos.get('fecha_ingreso', ""),
        'fecha_respuesta': fecha_respuesta,
        'coordinacion': datos_externos.get('coordinacion', ""),
        'autoridad': datos_externos.get('autoridad', ""),
        'ambiente': datos_externos.get('ambiente', ""),
        'servicio': datos_externos.get('servicio', ""),
        'servidores': datos_externos.get('servidores', "")
    }

def dividir_en_bloques(lineas):
    """Divide el archivo en bloques por solicitud"""
    bloques = []
    bloque_actual = []
    
    for linea in lineas[1:]:  # Omite header
        # Detecta inicio de nueva solicitud
        if re.match(r'^\s*"[^"]*(?:SOLICITUD|Solicitud de paso|PUBLICACIÓN|ANALISIS|CDC)', linea):
            if bloque_actual:
                bloques.append(bloque_actual)
                bloque_actual = []
        bloque_actual.append(linea)
    
    if bloque_actual:
        bloques.append(bloque_actual)
    
    return bloques

def main():
    print("EXTRAYENDO DATOS COMPLETOS (DENTRO Y FUERA DE COMILLAS)...")
    print("=" * 70)
    
    # Lee el archivo
    with INPUT.open("r", encoding="utf-8") as f:
        lineas = f.readlines()
    
    # Divide en bloques por solicitud
    bloques = dividir_en_bloques(lineas)
    print(f"Bloques encontrados: {len(bloques)}")
    
    # Procesa cada bloque
    solicitudes = []
    for i, bloque in enumerate(bloques):
        print(f"Procesando bloque {i+1}...")
        solicitud = procesar_bloque_solicitud(bloque)
        if solicitud['title']:  # Solo agrega si tiene título
            solicitudes.append(solicitud)
    
    # Crea DataFrame
    df = pd.DataFrame(solicitudes)

    df = df.replace("*", "")
    df = df.replace("#", "")
    df = df.replace('"', "")

    fecha_cols = ['create_at', 'fecha_ingreso', 'fecha_respuesta']
    for col in fecha_cols:
        if col in df.columns:
            df[col] = df[col].apply(convertir_fecha_a_datetime)
   
    # Guarda como Excel con fechas válidas
    output_excel = OUTPUT.with_suffix('.xlsx')
    df.to_excel(output_excel, index=False, engine='openpyxl')

    # Guarda archivo con todas las columnas
    df.to_csv(OUTPUT, index=False, encoding='utf-8')
    
    print(f"\nARCHIVO COMPLETO GENERADO: {OUTPUT}")
    print(f"Total solicitudes: {len(df)}")
    
    # Estadísticas de campos poblados
    print(f"\nCAMPOS POBLADOS:")
    for col in df.columns:
        if col in fecha_cols:
            count = df[col].notna().sum()
            porcentaje = (count / len(df)) * 100
            print(f"  {col:20}: {count:3d}/{len(df)} ({porcentaje:5.1f}%) [FECHA]")
        else:
            count = (df[col].notna() & (df[col] != "")).sum()
            porcentaje = (count / len(df)) * 100
            print(f"  {col:20}: {count:3d}/{len(df)} ({porcentaje:5.1f}%)")
    
    # Muestra muestra
    print(f"\nMUESTRA DE PRIMERAS 3 SOLICITUDES:")
    for i in range(min(3, len(df))):
        row = df.iloc[i]
        print(f"\nSolicitud {i+1}:")
        print(f"  Título: {row['title'][:60]}...")
        print(f"  Status: {row['status']}")
        print(f"  Realizada por: {row['sent_by']}")
        print(f"  Área: {row['source']}")
        print(f"  Ticket: {row['ticket']}")
        print(f"  Solicitante: {row['nombre_solicitante']}")
        print(f"  Correo: {row['correo_solicitante']}")

if __name__ == "__main__":
    main()
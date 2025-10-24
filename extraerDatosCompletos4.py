from datetime import datetime
from pathlib import Path
import csv
import re

INPUT = Path("datos.csv")
OUTPUT = Path("datos_completos_power_bi.csv")

def fecha_a_datetime(fecha_texto):
    txt = str(fecha_texto).strip()
    for fmt in ['%m/%d/%Y %I:%M:%S %p']:
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            pass
    return None

def clasificar_por_titulo(title):
    if not title:
        return ""
    up = title.upper()
    if "CDC BDD" in up:
        return "Base de Datos"
    if "SOLICITUD DE PASO A PRODUCCIÓN" in up:
        return "Paso a Producción"
    if "PUBLICACIÓN" in up:
        return "Publicación"
    if "ANALISIS FUNCIONAL" in up:
        return "Análisis Funcional"
    return "Otros"

CLASIFICACION_SOURCE = {
    "Control De Cambios Infraestructura": "Infraestructura",
    "Infraestructura": "Infraestructura",
    "Producción": "Producción",
    "Registro": "Registro",
    "USFQ Path": "USFQ Path",
}

def clasificar_por_source(source):
    candidatos = [source]
    for texto in candidatos:
        if not texto:
            continue
        lower = texto.lower()
        for llave, categoria in CLASIFICACION_SOURCE.items():
            if llave.lower() in lower:
                return categoria
        if "producción" in lower:
            return "Producción"
        if "infraestructura" in lower or "cdc" in lower:
            return "Infraestructura"
        if "registro" in lower:
            return "Registro"
    return "Otro"


def leer_registros_multilinea(filename):
    """
    Lee el archivo CSV que tiene registros multilínea donde:
    - Cada registro empieza con comillas " al inicio
    - Cada registro termina con "; (comillas, punto y coma) al final
    - Los campos internos pueden tener saltos de línea
    
    Identificadores de separadores (de extraerDatosCompletos3.py):
    - Title termina con ,"
    - Details termina con ",
    - File puede estar vacío o termina con ,
    - El último campo termina con ";
    """
    registros = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        # Saltar encabezado
        f.readline()
        
        buffer_lineas = []
        dentro_de_registro = False
        
        for linea in f:
            linea_stripped = linea.rstrip('\n\r')
            
            # Detectar inicio de nuevo registro: línea empieza con comillas
            if linea_stripped.startswith('"') and not dentro_de_registro:
                dentro_de_registro = True
                buffer_lineas = [linea_stripped]
                continue
            
            # Si estamos dentro de un registro, seguir acumulando
            if dentro_de_registro:
                buffer_lineas.append(linea_stripped)
                
                # Detectar fin de registro: línea termina con ";
                # Puede ser "; solo o ",; dependiendo del último campo
                if linea_stripped.endswith(',";'):
                    # Unir todas las líneas del registro
                    # Reemplazar saltos de línea internos por espacios
                    registro_completo = ' '.join(buffer_lineas)
                    registros.append(registro_completo)
                    buffer_lineas = []
                    dentro_de_registro = False
        
        # Procesar último registro si quedó en buffer
        if buffer_lineas:
            registro_completo = ' '.join(buffer_lineas)
            registros.append(registro_completo)
    
    return registros


def parsear_registro_csv(registro_str):
    """
    Parsea un registro CSV completo usando csv.reader de Python que maneja automáticamente:
    - Comillas y comillas escapadas
    - Campos entre comillas que contienen comas
    - Campos vacíos
    
    Estructura del CSV (10 campos):
    1. Title
    2. Details  
    3. File
    4. Status
    5. Stage (siempre vacío)
    6. Source
    7. Create at
    8. Sent by
    9. Sent to
    10. Custom response
    """
    
    try:
        # Quitar el ';' final del registro
        registro_limpio = registro_str.rstrip(';').strip()

        #Obtener Title

        match_title = re.match(r'"([^"]*?),"', registro_limpio)
        if match_title:
            title = match_title.group(1).strip()
        else:
            title = " "
        
        resto_sin_titulo = re.sub(r'^"[^"]*?,', '', registro_limpio, count=1)

        #Obtener details

        match_details = re.search(r'""(.*?)(?=(?:,{1,2}""https?://|,,|"",))', resto_sin_titulo.strip(), re.DOTALL)
        if match_details:
            details = match_details.group(1).strip()
            resto_details = resto_sin_titulo[match_details.end():]
        else:
            details = " "
            resto_details = resto_sin_titulo

        #Obtener file

        match_file_field = re.search(r'^(?:"",""(.*?)""|,""(.*?)""|,""|"",|,,)(?=,|$)', resto_details, re.DOTALL)
        if match_file_field:
            captured= match_file_field.group(1)
            file_field = captured.strip() if captured else ""
            resto_file = resto_details[match_file_field.end():]
            #print('Match_file_field:', resto_file)
        else:
            file_field = ""
            resto_file = resto_details.lstrip()
            #print('Else: ', resto_file)

        #Obtener status

        match_status = re.match(r',{1,2}\s*([^,]+),', resto_file, re.DOTALL)
        if match_status:
            status = match_status.group(1).strip()
            resto_status = resto_file[match_status.end():]
        else:
            status = ""
            resto_status = resto_file

        #Obtener source

        match_source = re.match(r'^(.*?)\s*(?=,,)', resto_status, re.DOTALL)
        if match_source:
            source = match_source.group(1).strip()
            #print(source)
            resto_source = resto_status[match_source.end():]
            #print(resto_source)
        else:
            source = ""
            resto_source = resto_status
            #print(resto_source)

        #Obtener create_at

        match_create_at = re.match(r',,(.*?),', resto_source)
        if match_create_at:
            create_at = match_create_at.group(1).strip()
            resto_create_at = resto_source[match_create_at.end():]
            #print(resto_create_at)
        else:
            create_at = ""
            resto_create_at = resto_source

        #Obtener sent_by

        match_sent_by = re.match(r'^(.*?),', resto_create_at)
        if match_sent_by:
            sent_by = match_sent_by.group(1).strip()
            resto_sent_by = resto_create_at[match_sent_by.end():]
            print(resto_sent_by)
            print('---')
        else:
            sent_by = ""
            resto_sent_by = resto_create_at

        #Obtener sent_to

        match_sent_to = re.match(r'^(?:""(.*?)"")|(.*?),"', resto_sent_by)
        if match_sent_to:
            sent_to = match_sent_to.group(1).strip()
            resto_sent_to = resto_sent_by[match_sent_to.end():]
        else:
            sent_to = ""
            resto_sent_to = resto_sent_by

        #Obtener custom_response
        
        match_custom_response = re.match(r'^(.*)$', resto_sent_to)
        if match_custom_response:
            custom_response = match_custom_response.group(1).strip()
        else:
            custom_response = ""


        # Usar csv.reader que maneja correctamente comillas y escapes
        reader = csv.reader([resto_source], delimiter=',', quotechar='"', 
                           doublequote=True, skipinitialspace=False)
        campos_raw = next(reader)

        # Asegurar que tenemos al menos 10 campos
        while len(campos_raw) < 10:
            campos_raw.append("")
  
        stage = "Empty"  # Siempre vacío según lo indicado
        
        # Limpiar details: reemplazar ; por espacios
        #details = details.replace(';', ' ')
        #details = ' '.join(details.split())  # Normalizar espacios
        #details = details.strip()
        
        return {
            'title': title,
            'details': details,
            'file': file_field,
            'status': status,
            'stage': stage,
            'source': source,
            'create_at': create_at,
            'sent_by': sent_by,
            'sent_to': sent_to,
            'custom_response': custom_response
        }
        
    except Exception as e:
        print(f"⚠️  Error parseando registro: {e}")
        print(f"   Registro: {registro_str[:200]}...")
        import traceback
        traceback.print_exc()
        return None


def extraer_campos(input_file, output_file):
    """
    Extrae y procesa los campos del CSV manteniendo la lógica de separadores
    identificada en extraerDatosCompletos3.py
    """
    
    # Leer todos los registros multilínea
    registros = leer_registros_multilinea(input_file)
    print(f"Total registros encontrados: {len(registros)}")
    
    # Procesar cada registro
    datos_procesados = []
    
    for i, registro in enumerate(registros, 1):
        parsed = parsear_registro_csv(registro)
        
        if parsed:
            # Aplicar clasificaciones
            parsed['clasificacion_titulo'] = clasificar_por_titulo(parsed['title'])
            parsed['clasificacion_source'] = clasificar_por_source(parsed['source'])
            
            # Procesar fecha
            fecha_dt = fecha_a_datetime(parsed['create_at'])
            if fecha_dt:
                parsed['year'] = fecha_dt.year
                parsed['month'] = fecha_dt.month
                parsed['day'] = fecha_dt.day
            else:
                parsed['year'] = ""
                parsed['month'] = ""
                parsed['day'] = ""
            
            datos_procesados.append(parsed)
        else:
            print(f"⚠️  No se pudo parsear el registro {i}")
    
    # Escribir CSV de salida
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        
        # Encabezados
        writer.writerow([
            'Title', 'Details', 'File', 'Status', 'Stage', 'Source', 
            'Create at', 'Year', 'Month', 'Day', 'Sent by', 'Sent to', 
            'Custom response', 'Classification Title', 'Classification Source'
        ])
        
        # Escribir datos
        for datos in datos_procesados:
            writer.writerow([
                datos['title'],
                datos['details'],
                datos['file'],
                datos['status'],
                datos['stage'],
                datos['source'],
                datos['create_at'],
                datos.get('year', ''),
                datos.get('month', ''),
                datos.get('day', ''),
                datos['sent_by'],
                datos['sent_to'],
                datos['custom_response'],
                datos.get('clasificacion_titulo', ''),
                datos.get('clasificacion_source', '')
            ])
    
    print(f"✅ Archivo generado: {output_file}")
    print(f"   Total registros procesados: {len(datos_procesados)}")
    
    # Estadísticas
    print("\n📊 ESTADÍSTICAS:")
    print(f"   Campos Title poblados: {sum(1 for d in datos_procesados if d['title'])}")
    print(f"   Campos Status poblados: {sum(1 for d in datos_procesados if d['status'])}")
    print(f"   Campos Source poblados: {sum(1 for d in datos_procesados if d['source'])}")
    print(f"   Fechas válidas: {sum(1 for d in datos_procesados if d.get('year'))}")


def main():
    input_file = 'datos.csv'
    output_file = 'datos_completos_power_bi.csv'
    
    print("="*70)
    print("EXTRACTOR DE DATOS CSV - Versión 4 (con csv.reader)")
    print("="*70)
    
    extraer_campos(input_file, output_file)
    
    print("\n✅ Proceso completado exitosamente")


if __name__ == "__main__":
    main()

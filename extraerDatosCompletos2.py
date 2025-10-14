import pandas as pd
import re
import csv
from datetime import datetime
from pathlib import Path

INPUT = Path("datos.csv")
OUTPUT = Path("datos_completos_power_bi.csv")

def convertir_fecha_a_datetime(fecha_texto):
    if not fecha_texto or str(fecha_texto).strip() == "":
        return None
    txt = str(fecha_texto).strip()
    # Formato M/D/YYYY H:MM:SS AM/PM
    for fmt in ['%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M:%S']:
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            pass
    return None

def extraer_campos_detalles(details):
    """Extrae campos dentro del bloque Details."""
    datos = {}
    # Fecha ingreso ISO
    m = re.search(r'Fecha de ingreso:\.*\s*([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+)', details, re.IGNORECASE)
    if m: datos['fecha_ingreso_iso'] = m.group(1)
    # Nombre
    m = re.search(r'- Nombre:\.*\s*([^\n;]+)', details, re.IGNORECASE)
    if m: datos['nombre_solicitante'] = m.group(1).strip()
    # Correo
    m = re.search(r'- Correo:\.*\s*([^\n;]+)', details, re.IGNORECASE)
    if m: datos['correo_solicitante'] = m.group(1).strip()
    # Cargo
    m = re.search(r'- Cargo:\.*\s*([^\n;]+)', details, re.IGNORECASE)
    if m: datos['cargo_solicitante'] = m.group(1).strip()
    # Autoridad / coordinación
    m = re.search(r'AUTORIDAD[^:]*:\s*(.+)', details, re.IGNORECASE)
    if m: datos['autoridad_bloque'] = m.group(1).strip()
    return datos

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

# Diccionario editable para clasificar por Source
CLASIFICACION_SOURCE = {
    "Control De Cambios Infraestructura": "Infraestructura",
    "Infraestructura": "Infraestructura",
    "Producción": "Producción",
    "Registro": "Registro",
    "USFQ Path": "USFQ Path",
}

def clasificar_por_source(source, stage="", sent_by="", details=""):
    candidatos = [source, stage, sent_by, details]
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

def agrupar_registros(lineas):
    """Agrupa las líneas crudas en bloques por registro."""
    registros = []
    actual = []
    for raw_line in lineas:
        line = raw_line.rstrip("\n")
        if not actual and not line.strip():
            continue
        actual.append(line)
        if line.endswith(',\";') and len(actual) > 1:
            registros.append(actual)
            actual = []
    if actual:
        registros.append(actual)
    return registros

def limpiar_segmento_detalle(segmento):
    """Normaliza un fragmento del campo Details conservando los saltos relevantes."""
    seg = segmento.replace('""', '"').rstrip(';').strip()
    if seg.startswith('"') and not seg.startswith('"http'):
        seg = seg[1:]
    if seg.endswith('"') and not seg.endswith('""'):
        seg = seg[:-1]
    return seg.strip()

def limpiar_campo(valor):
    """Normaliza campos simples removiendo comillas sobrantes."""
    if valor is None:
        return ""
    cleaned = valor.replace('""', '"').strip()
    cleaned = cleaned.strip('"').strip()
    cleaned = cleaned.strip(',').strip()
    return cleaned

def parsear_registro(lineas):
    if not lineas:
        return None

    primera = lineas[0]
    if ',""' in primera:
        titulo_bruto, detalle_inicial = primera.split(',""', 1)
    elif ',' in primera:
        titulo_bruto, detalle_inicial = primera.split(',', 1)
    else:
        return None
    titulo = titulo_bruto.lstrip('"').strip()

    segmentos_detalle = []
    resto_lineas = []
    resto_encontrado = False

    for linea in ([detalle_inicial] + lineas[1:]):
        if not resto_encontrado:
            http_idx = linea.lower().find("http")
            if http_idx != -1:
                sep_idx = linea.rfind('","', 0, http_idx)
                if sep_idx != -1:
                    detalle_parcial = linea[:sep_idx]
                    if detalle_parcial.strip():
                        segmentos_detalle.append(detalle_parcial)
                    resto_lineas.append(linea[sep_idx + 2 :])
                    resto_encontrado = True
                    continue
            if ",," in linea:
                pos = linea.index(",,")
                detalle_parcial = linea[:pos]
                if detalle_parcial.strip():
                    segmentos_detalle.append(detalle_parcial)
                resto_lineas.append(linea[pos:])
                resto_encontrado = True
            else:
                segmentos_detalle.append(linea)
        elif resto_encontrado:
            resto_lineas.append(linea)

    detalle_limpio = "\n".join(
        line for line in (limpiar_segmento_detalle(s) for s in segmentos_detalle) if line
    )

    def consumir_hasta_coma(texto):
        """Extrae el siguiente campo respetando comillas."""
        if not texto:
            return "", ""
        buffer = []
        i = 0
        en_comillas = False
        longitud = len(texto)
        while i < longitud:
            ch = texto[i]
            if ch == '"':
                if en_comillas and i + 1 < longitud and texto[i + 1] == '"':
                    buffer.append('"')
                    i += 2
                    continue
                en_comillas = not en_comillas
                i += 1
                continue
            if ch == ',' and not en_comillas:
                return "".join(buffer), texto[i + 1 :]
            buffer.append(ch)
            i += 1
        return "".join(buffer), ""

    def dividir_por_doble_coma(texto):
        """Divide el texto en dos partes usando la primera aparición de ',,'."""
        idx = texto.find(",,")
        if idx != -1:
            return texto[:idx], texto[idx + 1 :]
        idx_simple = texto.find(",")
        if idx_simple == -1:
            return texto, ""
        return texto[:idx_simple], texto[idx_simple + 1 :]

    def parsear_resto(lineas_resto):
        if not lineas_resto:
            return [""] * 8

        resto_bruto = "\n".join(ln.rstrip(";") for ln in lineas_resto).strip()
        resto_bruto = resto_bruto.replace('""', '"')

        file_field, restante = consumir_hasta_coma(resto_bruto)
        status, restante = consumir_hasta_coma(restante)
        stage_bruto, restante = dividir_por_doble_coma(restante)

        stage = stage_bruto.strip()

        if not status:
            stage_limpio_inicial = stage.lstrip()
            posibles_status = [
                "Completed",
                "Approved",
                "Rejected",
                "Requested",
                "Cancelled",
                "Canceled",
                "In Progress",
                "Pending",
                "Aprobado",
                "Rechazado",
                "APPROVED",
                "APPROBADO",
                "REJECTED",
            ]
            stage_mayus = stage_limpio_inicial.upper()
            for candidato in posibles_status:
                if stage_mayus.startswith(candidato.upper()):
                    status = stage_limpio_inicial[:len(candidato)].strip()
                    stage = stage_limpio_inicial[len(candidato):].lstrip(" ,|-")
                    break

        source, restante = consumir_hasta_coma(restante)
        create_at_raw, restante = consumir_hasta_coma(restante)
        sent_by, restante = consumir_hasta_coma(restante)
        sent_to, restante = consumir_hasta_coma(restante)
        custom_resp = restante.strip() if restante else ""

        return [file_field, status, stage, source, create_at_raw, sent_by, sent_to, custom_resp]

    file_field, status, stage, source, create_at_raw, sent_by, sent_to, custom_resp = parsear_resto(resto_lineas)
    file_field = limpiar_campo(file_field)
    status = status.strip()
    stage = limpiar_campo(stage)
    source = limpiar_campo(source)
    create_at_raw = create_at_raw.strip()
    sent_by = limpiar_campo(sent_by)
    sent_to = limpiar_campo(sent_to)
    custom_resp = limpiar_campo(custom_resp)

    if not source and sent_by:
        source = sent_by

    internos = extraer_campos_detalles(detalle_limpio)
    fecha_ingreso_iso = internos.get('fecha_ingreso_iso', "")

    create_at_dt = convertir_fecha_a_datetime(create_at_raw)

    return {
        "title": titulo,
        "details": detalle_limpio,
        "file": file_field,
        "status": status,
        "stage": stage,
        "source": source,
        "create_at": create_at_dt,
        "sent_by": sent_by,
        "sent_to": sent_to,
        "custom_responses": custom_resp,
        "create_at_texto": create_at_raw,
        "fecha_ingreso_iso": fecha_ingreso_iso,
        "nombre_solicitante": internos.get('nombre_solicitante', ""),
        "correo_solicitante": internos.get('correo_solicitante', ""),
        "cargo_solicitante": internos.get('cargo_solicitante', ""),
        "autoridad": internos.get('autoridad_bloque', ""),
        "clasificacion_titulo": clasificar_por_titulo(titulo),
        "clasificacion_source": clasificar_por_source(source, stage, sent_by, detalle_limpio),
    }

def main():
    print("PARSEANDO REGISTROS...")
    with INPUT.open("r", encoding="utf-8") as f:
        _header = f.readline()
        registros = agrupar_registros(f)
    print(f"Registros detectados: {len(registros)}")

    filas = []
    for i, lineas in enumerate(registros, 1):
        try:
            parsed = parsear_registro(lineas)
        except Exception as exc:
            print(f"[WARN] Error al parsear registro {i}: {exc}")
            parsed = None
        if parsed is None:
            print(f"[WARN] No se pudo parsear registro {i}")
            continue
        filas.append(parsed)

    df = pd.DataFrame(filas)

    if "create_at" in df.columns:
        df["create_at"] = pd.to_datetime(df["create_at"], errors="coerce")
        df["create_at_year"] = df["create_at"].dt.year
        df["create_at_month"] = df["create_at"].dt.month
        df["create_at_day"] = df["create_at"].dt.day

    # Convertir fecha_ingreso_iso a datetime y desglosar componentes
    if "fecha_ingreso_iso" in df.columns:
        df["fecha_ingreso"] = pd.to_datetime(
            df["fecha_ingreso_iso"].str.split(".", n=1).str[0],
            format="%Y-%m-%dT%H:%M:%S",
            errors="coerce",
        )
        df["fecha_ingreso_year"] = df["fecha_ingreso"].dt.year
        df["fecha_ingreso_month"] = df["fecha_ingreso"].dt.month
        df["fecha_ingreso_day"] = df["fecha_ingreso"].dt.day

    # Guardar
    output_excel = OUTPUT.with_suffix('.xlsx')
    df.to_excel(output_excel, index=False)
    df.to_csv(OUTPUT, index=False, encoding="utf-8")

    print(f"Generado: {OUTPUT} ({len(df)} filas)")
    print("Campos:", ", ".join(df.columns))

if __name__ == "__main__":
    main()

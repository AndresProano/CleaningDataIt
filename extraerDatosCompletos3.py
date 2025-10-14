from pathlib import Path

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

#def extraer_campos_detalles(details):

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
from datetime import datetime
from pathlib import Path
import re, csv, io
from typing import Iterator, TextIO

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


def _char_stream(f: TextIO, chunk_size: int = 8192) -> Iterator[str]:
    """Genera caracteres desde el archivo en bloques (streaming)."""
    prev_char = None
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        for ch in chunk:
            if ch == '\r':
                continue
            if ch == '\n' and prev_char == ';':
                yield '\n'  # Marcador de fin de registro
                prev_char = ch
                continue
            # ✅ Si vemos \n en otros contextos, convertir a espacio
            if ch == '\n':
                yield ' '
                prev_char = ch
                continue
            yield ch
            prev_char = ch


def extraer_campos(input, output):
    with open(input, 'r', encoding='utf-8') as infile, \
         open(output, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)

            writer.writerow(['Title', 'Details', 'File', 'Status', 'Stage', 
                           'Source', 'Create at', 'Sent by', 'Sent to', 'Custom response'])


            state = "title_open"
            buf_title = []
            buf_details = []
            buf_file = []
            buf_status = []
            buf_stage = []
            buf_source = []
            buf_create_at = []
            buf_sent_by = []
            buf_sent_to = []
            buf_custom_response = []

            comilla_started = False
            first_line = True

            stream = _char_stream(infile)

            for ch in stream: 
                if first_line:
                    if ch == '\n':
                        first_line = False
                    continue

                if state == "title_open":
                    if ch == '"':
                        comilla_started = True
                        state = "title"
                    else:
                        comilla_started = False
                        state = "title"
                        buf_title.append(ch)
                    continue

                if state == "title":
                    if ch == ',':
                        try:
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == '"':
                            state = "details"
                            comilla_started = True
                            continue
                        else:
                            buf_title.append(ch)
                            if nxt:
                                buf_title.append(nxt)
                    else:
                        buf_title.append(ch)
                    continue
                
                '''
                if state == "details_open":
                    if not comilla_started and ch == '"':
                        comilla_started = True
                    else:
                        state = "details"
                        buf_details.append(ch)
                    continue
                '''

                if state == "details":
                    if ch == '"':
                        nxt = next(stream, None)
                        if nxt is None:
                            continue
                        if nxt == '"':
                            buf_details.append('"')
                            continue
                        if nxt == ',':
                            state = "file_open"
                            comilla_started = False
                            continue

                        buf_details.append('"')
                        buf_details.append(nxt)
                        continue

                    buf_details.append(ch)
                    continue



                if state == "file_open":
                    if ch == '"':
                        comilla_started = True
                        state = "file"
                    else:
                        comilla_started = False
                        state = "file"
                        buf_file.append(ch)
                    continue

                if state == "file":
                    if comilla_started:
                        if ch == '"':
                            nxt = next(stream)
                            if nxt == ',':
                                state = "status"
                                comilla_started = False
                                continue
                            buf_file.append(ch)
                            buf_file.append(nxt)
                            continue
                    elif ch == ',':
                        try: 
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == ',':
                            state = "status"
                            buf_file.append("Empty")
                            comilla_started = False
                            continue
                    buf_file.append(ch)
                    continue

                
                if state == 'status':
                    if ch == ',':
                        state = "stage"
                        continue
                    else:
                        buf_status.append(ch)
                    continue
                
                if state == 'stage':
                    buf_stage.append('Empty')
                    state = "source"
                    continue

                if state == 'source':
                    if ch == ',':
                        try: 
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == ',':
                            state = "create_at"
                            continue
                    else:
                        buf_source.append(ch)
                    continue
            
                if state == 'create_at':
                    if ch == ',':
                        state = "sent_by"
                    else:
                        buf_create_at.append(ch)
                    continue

                if state == 'sent_by':
                    if ch == ',':
                        state = "sent_to_open"
                    else:
                        buf_sent_by.append(ch)
                    continue

                if state == 'sent_to_open':
                    if ch == '"':
                        comilla_started = True
                        state = "sent_to"
                    else:
                        comilla_started = False
                        state = "sent_to"
                        buf_sent_to.append(ch)
                    continue
                
                if state == 'sent_to':
                    if ch == '"':
                        try:
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == ',':
                            state = "custom_responses_open"
                            comilla_started = False
                            continue
                        else:
                            buf_sent_to.append(ch)
                            ch = nxt
                    elif ch == ',':
                        try: 
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == ',':
                            state = "custom_responses_open"
                            comilla_started = False
                            continue
                    else:
                        buf_sent_to.append(ch)
                        continue
                    
                if state == 'custom_responses_open':
                    if ch == '"':
                        comilla_started = True
                        state = "custom_response"
                    elif ch == ';':  # Campo vacío y fin de registro
                        # Escribir fila con custom_response vacío
                        writer.writerow([
                            "".join(buf_title).strip(),
                            "".join(buf_details),
                            "".join(buf_file).strip(),
                            "".join(buf_status).strip(),
                            "".join(buf_stage).strip(),
                            "".join(buf_source).strip(),
                            "".join(buf_create_at).strip(),
                            "".join(buf_sent_by).strip(),
                            "".join(buf_sent_to).strip(),
                            ""
                        ])
                        # Limpiar buffers
                        buf_title.clear()
                        buf_details.clear()
                        buf_file.clear()
                        buf_status.clear()
                        buf_stage.clear()
                        buf_source.clear()
                        buf_create_at.clear()
                        buf_sent_by.clear()
                        buf_sent_to.clear()
                        buf_custom_response.clear()
                        state = "title_open"
                        comilla_started = False
                    else:
                        comilla_started = False
                        state = "custom_response"
                        buf_custom_response.append(ch)
                    continue

                if state == 'custom_response':
                    if ch == '"':
                        try:
                            nxt = next(stream)
                        except StopIteration:
                            break
                        if nxt == '"':
                            buf_custom_response.append('"')
                            continue
                        if nxt == ';':
                            comilla_started = False
                            writer.writerow([
                                    "".join(buf_title).strip(),
                                    "".join(buf_details),
                                    "".join(buf_file).strip(),
                                    "".join(buf_status).strip(),
                                    "".join(buf_stage).strip(),
                                    "".join(buf_source).strip(),
                                    "".join(buf_create_at).strip(),
                                    "".join(buf_sent_by).strip(),
                                    "".join(buf_sent_to).strip(),
                                    "".join(buf_custom_response).strip()
                            ])

                            buf_title.clear()
                            buf_details.clear()
                            buf_file.clear()
                            buf_status.clear()
                            buf_stage.clear()
                            buf_source.clear()
                            buf_create_at.clear()
                            buf_sent_by.clear()
                            buf_sent_to.clear()
                            buf_custom_response.clear()

                            state = "title_open"
                            comilla_started = False
                            continue
                        buf_custom_response.append('"')
                        if nxt:
                            buf_custom_response.append(nxt)
                        else:
                            buf_custom_response.append(ch)
                    else:
                        if ch == ';':  # Fin sin comillas
                            writer.writerow([
                                "".join(buf_title).strip(),
                                "".join(buf_details),
                                "".join(buf_file).strip(),
                                "".join(buf_status).strip(),
                                "".join(buf_stage).strip(),
                                "".join(buf_source).strip(),
                                "".join(buf_create_at).strip(),
                                "".join(buf_sent_by).strip(),
                                "".join(buf_sent_to).strip(),
                                "".join(buf_custom_response).strip()
                            ])
                            buf_title.clear()
                            buf_details.clear()
                            buf_file.clear()
                            buf_status.clear()
                            buf_stage.clear()
                            buf_source.clear()
                            buf_create_at.clear()
                            buf_sent_by.clear()
                            buf_sent_to.clear()
                            buf_custom_response.clear()
                            state = "title_open"
                        else:
                            buf_custom_response.append(ch)
                    continue

                '''
                if state in ("custom_response",) and (buf_title or buf_details or buf_file or buf_status or buf_stage or buf_source or buf_create_at
                                                      or buf_sent_by or buf_sent_to or buf_custom_response):
                    title = "".join(buf_title).strip()
                    details = "".join(buf_details)
                    file = "".join(buf_file).strip()
                    status = "".join(buf_status).strip()
                    stage = "".join(buf_stage).strip()
                    source = "".join(buf_source).strip()
                    create_at = "".join(buf_create_at).strip()
                    sent_by = "".join(buf_sent_by).strip()
                    sent_to = "".join(buf_sent_to).strip()
                    custom_response = "".join(buf_custom_response).strip()
                    writer.writerow([title, details, file, status, stage, source, create_at, sent_by, sent_to, custom_response])
                '''

def main():
    input = 'datos.csv'
    output = 'datos_completos_power_bi.csv'

    extraer_campos(input, output)

if __name__ == "__main__":
    main()
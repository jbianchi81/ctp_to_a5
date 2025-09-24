import os
import pandas
import logging
from zoneinfo import ZoneInfo
from a5client import client
import json
from datetime import timedelta 

data_dir = "series_corregidas"
series_file = "series_p_corregida.csv"
imported_dir = "imported"

# lee series_p_corregida.csv
series = pandas.read_csv(open(series_file, encoding="utf-8"))
# lista archivos csv en dir series_corregidas
archivos_datos = os.listdir(data_dir)
unmatched_ids = []
# itera
for archivo in archivos_datos:
    logging.info("Leyendo archivo %s" % archivo)
    # busca id serie usando id_externo en nombre de archivo
    id_externo = archivo.replace("_corregida.csv","")
    series_match = series[series["id_externo"] == id_externo]
    # si no encuentra imprime error
    if not len(series_match):
        unmatched_ids.append(id_externo)
        logging.warning("No se encontro serie para id_externo: %s" % id_externo)
        continue
    series_id = int(series_match.iloc[0,0])
    # lee datos
    data = pandas.read_csv(open("%s/%s" % (data_dir, archivo), encoding="utf-8"))
    # elimina nulos
    observaciones = data[["fecha","ppt_corregida"]].rename(columns={"fecha":"timestart", "ppt_corregida": "valor"}).dropna()
    # parsea fecha
    observaciones["timestart"] = pandas.to_datetime(observaciones["timestart"], format="%Y-%m-%d")
    # 2. Set time to 09:00
    observaciones["timestart"] = observaciones["timestart"].dt.floor("D") + pandas.Timedelta(hours=9)
    # 3. Localize to Argentina timezone
    observaciones["timestart"] = observaciones["timestart"].dt.tz_localize(ZoneInfo("America/Argentina/Buenos_Aires"))
    # check hora
    check_hora = len(observaciones[observaciones["timestart"].dt.hour != 9])
    if check_hora > 0:
        logging.error("No se asignó correctamente la hora en %i registros" % check_hora)
        continue
    # setea index
    observaciones = observaciones.set_index("timestart")
    # importa a a5
    try:
        created = client.createObservaciones(observaciones, series_id, tipo = "puntual", timeSupport= timedelta(days=1))
    except Exception as e:
        logging.error(e)
        continue
    logging.info("Se crearon %i registros de la serie %i, id_externo %s" % (len(created), series_id, id_externo))
    json.dump(created, open("%s/%s.json" % (imported_dir, id_externo), "w"), indent=2)

    # data[pandas.isna(data["ppt_original"])]["ppt_corregida"].min()
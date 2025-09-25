import os
import pandas
import logging
from zoneinfo import ZoneInfo
from a5client import client
import json
from datetime import timedelta 

data_dir = "series_corregidas"
series_file = "series_analisis.csv"
series_interpolada_file = "series_interpolada.csv"
imported_dir = "imported"
estaciones_list = "estaciones_faltantes.csv"

# lee estaciones faltantes
estaciones_faltantes = pandas.read_csv(estaciones_list)
estaciones_faltantes = estaciones_faltantes[estaciones_faltantes["falta"] == 1]
nombre_faltantes = [ e for e in estaciones_faltantes["nombre"]]

# lee series_p_corregida.csv
series = pandas.read_csv(open(series_file, encoding="utf-8"))

#lee series_p_interpolada.csv
series_interpolada = pandas.read_csv(open(series_interpolada_file, encoding="utf-8"))

# lista archivos csv en dir series_corregidas
archivos_datos = os.listdir(data_dir)
unmatched_ids = []
# itera
for archivo in archivos_datos:
    print("Leyendo archivo %s" % archivo)
    # busca id serie usando id_externo en nombre de archivo
    id_externo = archivo.replace("_corregida.csv","")
    if id_externo not in nombre_faltantes:
        print("Salteando estacion %s" % id_externo)
        continue
    series_match = series[series["id_externo"] == id_externo]
    # si no encuentra imprime error
    if not len(series_match):
        unmatched_ids.append(id_externo)
        print("No se encontro serie para id_externo: %s" % id_externo)
        continue
    series_id = int(series_match.iloc[0,0])
    # lee datos
    data = pandas.read_csv(open("%s/%s" % (data_dir, archivo), encoding="utf-8"))
    # parsea fecha
    data["fecha"] = pandas.to_datetime(data["fecha"], format="%Y-%m-%d")
    # 2. Set time to 09:00
    data["fecha"] = data["fecha"].dt.floor("D") + pandas.Timedelta(hours=9)
    # 3. Localize to Argentina timezone
    data["fecha"] = data["fecha"].dt.tz_localize(ZoneInfo("America/Argentina/Buenos_Aires"))
    # check hora
    check_hora = len(data[data["fecha"].dt.hour != 9])
    if check_hora > 0:
        print("No se asignó correctamente la hora en %i registros" % check_hora)
        continue
    # setea index
    data = data.set_index("fecha")
    data.index.name = "timestart"
    # elimina falsos ceros y elimina nulos
    sin_falsos_ceros = data[data["zero_flag"] == 0][["ppt_original"]].dropna().rename(columns={"ppt_original": "valor"})
    # importa a a5
    try:
        created = client.createObservaciones(sin_falsos_ceros, series_id, tipo = "puntual", timeSupport= timedelta(days=1))
    except Exception as e:
        print(e)
        continue
    print("Se crearon %i registros de la serie %i, id_externo %s" % (len(created), series_id, id_externo))
    json.dump(created, open("%s/serie_analisis_%i_%s.json" % (imported_dir,series_id, id_externo), "w"), indent=2)

    # busca id serie interpolada
    series_match_interpolada = series_interpolada[series_interpolada["id_externo"] == id_externo]
    # si no encuentra imprime error
    if not len(series_match):
        unmatched_ids.append(id_externo)
        print("No se encontro serie interpolada para id_externo: %s sin falsos ceros" % id_externo)
        continue
    interpolada_series_id = int(series_match_interpolada.iloc[0,0])
    # importa serie interpolada
    interpolada = data[["ppt_corregida"]].dropna().rename(columns={"ppt_corregida": "valor"})
    # importa a a5
    try:
        created_i = client.createObservaciones(interpolada, interpolada_series_id, tipo = "puntual", timeSupport= timedelta(days=1))
    except Exception as e:
        print(e)
        continue
    print("Se crearon %i registros de la serie %i, id_externo %s interpolada" % (len(created), interpolada_series_id, id_externo))
    json.dump(created, open("%s/serie_interpolada_%i_%s.json" % (imported_dir, interpolada_series_id, id_externo), "w"), indent=2)

    # data[pandas.isna(data["ppt_original"])]["ppt_corregida"].min()
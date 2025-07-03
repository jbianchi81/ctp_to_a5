import json
import os
import pandas
from datetime import datetime, timedelta
import pytz
from zoneinfo import ZoneInfo

tz = pytz.timezone("America/Argentina/Buenos_Aires")

estaciones = json.load(open("/home/jbianchi/git_repos/alerta5DBIO/tmp/estaciones_ctp.geojson"))

series = json.load(open("/home/jbianchi/git_repos/alerta5DBIO/tmp/series_ctp.json"))

series_id = 42155

for serie in series:
    if series_id is not None and serie["id"] != series_id:
        continue
    data_filepath = '/home/jbianchi/Downloads/Armado_PMA/Precipitación diaria/todas/PPT - %s.xlsx' % (serie["estacion"]["id_externo"])
    if not os.path.exists(data_filepath):
        print("file %s not found" % data_filepath)
        continue
    print("parsing file: %s" % data_filepath)
    try:
        data = pandas.read_excel(data_filepath)
        data["timestart"] = [datetime(d.year, d.month,d.day,9,  tzinfo=ZoneInfo("America/Argentina/Buenos_Aires")) for d in data["Fecha"]]
        # data["timestart"] = data["timestart"].dt.tz_localize('America/Argentina/Buenos_Aires')
        data["timeend"] = [dt + timedelta(days=1) for dt in data["timestart"] ]
        data["timeend"] = [datetime(dt.year,dt.month,dt.day,9,tzinfo=ZoneInfo("America/Argentina/Buenos_Aires")) for dt in data["timeend"] ]
        data["series_id"] = serie["id"]
        data["valor"] = data["Precipitaciones"]
        data = data.drop_duplicates(subset='timestart', keep='first')
        data[["timestart", "timeend", "series_id", "valor"]].to_json(open("/home/jbianchi/Downloads/Armado_PMA/Precipitación diaria/json/obs_%d.json" % serie["id"], "w"), orient="records", force_ascii=False, date_format="iso", indent=2)
    except Exception as e:
        print(str(e))


    


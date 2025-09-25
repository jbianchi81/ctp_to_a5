import json
import pandas
from  a5client import client

estaciones_file = "estaciones.geojson"
estaciones_list = "estaciones_faltantes.csv"

estaciones_geojson = json.load(open(estaciones_file, encoding="utf-8"))

estaciones_faltantes = pandas.read_csv(estaciones_list)
estaciones_faltantes = estaciones_faltantes[estaciones_faltantes["falta"] == 1]
nombre_faltantes = [ e for e in estaciones_faltantes["nombre"]]

para_importar = []
missing=[]
for nombre in nombre_faltantes:
    found=False
    for estacion in estaciones_geojson["features"]:
        if estacion["properties"]["Name"] == nombre:
            print("Se encontró la estacion %s" % nombre)
            para_importar.append({
                "nombre":nombre,
                "id_externo":nombre,
                "geom": {
                    "type": "Point", 
                    "coordinates": [
                        estacion["geometry"]["coordinates"][0],
                        estacion["geometry"]["coordinates"][1]
                    ]
                },
                "tabla":"ctp",
                "tipo":"P",
                "habilitar":True,
                "propietario":"Comisión Trinacional para el Desarrollo de la Cuenca del Río Pilcomayo",
                "real":True,
                "public":False,
            })
            found=True
            break
    if not found:
        print("NO SE ENCONTRÓ: %s" % nombre)
        missing.append(nombre)

json.dump(para_importar, open("estaciones_para_importar.json","w",encoding="utf-8"),indent=2)
json.dump(missing, open("estaciones_faltantes.json","w",encoding="utf-8"),indent=2)

created = client.createSites(para_importar, "estaciones", "json")
json.dump(created, open("estaciones_importadas.json","w"),indent=2)

# series

series_p_obs = [
    {
        "estacion_id": e["id"],
        "var_id": 1,
        "proc_id": 1,
        "unit_id": 22,
        "tipo": "puntual"
    } for e in created
]

series_p_obs_created = client.createSeries(series_p_obs, tipo="puntual")

# p analisis

series_p_a = [
    {
        "estacion_id": e["id"],
        "var_id": 1,
        "proc_id": 6,
        "unit_id": 22,
        "tipo": "puntual"
    } for e in created
]

series_p_a_created = client.createSeries(series_p_a, tipo="puntual")

# p interpolada

series_p_i = [
    {
        "estacion_id": e["id"],
        "var_id": 1,
        "proc_id": 3,
        "unit_id": 22,
        "tipo": "puntual"
    } for e in created
]

series_p_i_created = client.createSeries(series_p_i, tipo="puntual")

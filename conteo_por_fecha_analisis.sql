\copy (
    with o as ( 
        select observaciones.timestart::date AS t
        from series
        join estaciones on estaciones.unid=series.estacion_id
        join observaciones on series.id=observaciones.series_id
        where 
            estaciones.tabla = 'ctp'
        and var_id = 1
        and proc_id = 6
        and series.unit_id = 22
    ), d as (
        select generate_series(DATE '2000-01-01', DATE '2025-01-01', INTERVAL '1 day')::date AS date
    )
    select 
        d.date,
        count(o.t) count
    from d
    left join o on d.date=o.t
    group by d.date
    order by d.date
)
to 'conteo_por_fecha_analisis.csv' 
with csv
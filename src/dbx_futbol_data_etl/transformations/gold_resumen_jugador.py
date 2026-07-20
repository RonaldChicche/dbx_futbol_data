from pyspark import pipelines as dp
from pyspark.sql.functions import count, avg

@dp.materialized_view(comment="Eventos y ubicacion promedio por jugador")
def gold_resumen_jugador():
    return (
        dp.read("silver_eventos")
        .filter("jugador IS NOT NULL")
        .groupBy("jugador", "equipo")
        .agg(
            count("*").alias("total_eventos"),
            avg("ubicacion_x").alias("posicion_x_promedio"),
            avg("ubicacion_y").alias("posicion_y_promedio"),
        )
    )
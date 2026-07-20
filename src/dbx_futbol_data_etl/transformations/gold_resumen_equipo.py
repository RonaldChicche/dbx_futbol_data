from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, avg

@dp.materialized_view(comment="Resumen de eventos por equipo y tipo")
def gold_resumen_equipo():
    return (
        dp.read("silver_eventos")
        .groupBy("equipo", "tipo_evento")
        .agg(count("*").alias("total_eventos"))
    )
    
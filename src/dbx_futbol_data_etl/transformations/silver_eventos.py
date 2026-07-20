from pyspark import pipelines as dp 
from pyspark.sql.functions import col

@dp.table(comment="Eventos de StatsBomb aplanados, listos para analisis")
def silver_eventos():
    return (
        spark.readStream.table("bronze_eventos")
        .select(
            col("id").alias("evento_id"),
            col("index").alias("orden"),
            col("period").alias("periodo"),
            col("timestamp").alias("marca_tiempo"),
            col("minute").alias("minuto"),
            col("second").alias("segundo"),
            col("duration").alias("duracion_segundos"),
            col("possession").alias("posesion_numero"),
            col("type.name").alias("tipo_evento"),
            col("play_pattern.name").alias("patron_juego"),
            col("team.name").alias("equipo"),
            col("possession_team.name").alias("equipo_en_posesion"),
            col("player.name").alias("jugador"),
            col("position.name").alias("posicion"),
            col("location")[0].alias("ubicacion_x"),
            col("location")[1].alias("ubicacion_y"),
        )
        .filter(col("tipo_evento").isNotNull())
    )
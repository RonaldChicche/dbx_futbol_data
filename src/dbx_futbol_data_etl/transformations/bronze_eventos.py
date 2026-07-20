from pyspark import pipelines as dp


@dp.table(comment="Eventos crudos de StataBomb, sin procesar")
def bronze_eventos():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/futbol/bronze/landing/_schema")
        .load("/Volumes/futbol/bronze/landing/eventos")
    )

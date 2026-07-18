from pyspark import pipelines as dp


@dp.table(comment="Eventos crudos de StataBomb, sin procesar")
def bronze_eventos():
    return (
        spark.readStrea
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.scjemaLocation", "/Volumes/futbol/bronze/landing/_schema")
        .load("/Volumes/futbol/bronze/landing/eventos")
    )

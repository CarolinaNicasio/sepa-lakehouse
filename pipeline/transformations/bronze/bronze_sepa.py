from pyspark import pipelines as dp
from pyspark.sql import functions as F
from collections import defaultdict
import re

RUTA_CSV = "/Volumes/sepa/landing/raw/csv/"


def limpiar_nombre(nombre):
    nombre = nombre.strip().lower()
    return re.sub(r"[ ,;{}()\n\t=]+", "_", nombre)


def unificar_columnas(df):
    grupos = defaultdict(list)
    for c in df.columns:
        grupos[limpiar_nombre(c)].append(F.col(f"`{c}`"))
    return df.select([
        (F.coalesce(*cols) if len(cols) > 1 else cols[0]).alias(nombre)
        for nombre, cols in grupos.items()
    ])


def leer_sepa(nombre_archivo):
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "|")
        .option("header", "true")
        .option("escape", '"')
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.partitionColumns", "fecha,comercio")
        .option("pathGlobFilter", nombre_archivo)
        .load(RUTA_CSV)
        .withColumn("archivo_origen", F.col("_metadata.file_path"))
    )
    df = unificar_columnas(df)
    return (
        df.filter(~F.col("id_comercio").startswith("Ultima actualizacion"))
        .withColumn("fecha_ingesta", F.current_timestamp())
    )


@dp.table(name="sepa.bronze.comercio", comment="Datos crudos de comercios SEPA")
def comercio():
    return leer_sepa("comercio.csv")


@dp.table(name="sepa.bronze.sucursales", comment="Datos crudos de sucursales SEPA")
def sucursales():
    return leer_sepa("sucursales.csv")


@dp.table(name="sepa.bronze.productos", comment="Datos crudos de precios SEPA")
def productos():
    return leer_sepa("productos.csv")
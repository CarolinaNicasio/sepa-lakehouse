from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# Funciones de limpieza reutilizables
# ============================================================

def texto(columna):
    """Unifica espacios (incluido el \\r), recorta y convierte vacios en null."""
    limpio = F.trim(F.regexp_replace(F.col(columna), r"\s+", " "))
    return F.when(limpio == "", None).otherwise(limpio)


def numero(columna, tipo="decimal(12,2)"):
    """Convierte texto a numero. Si no se puede convertir, devuelve null."""
    return F.expr(f"try_cast(trim({columna}) AS {tipo})")


# ============================================================
# PRECIOS: vista de limpieza + Auto CDC SCD2
# ============================================================

reglas_criticas_precios = {
    "clave_completa": "id_comercio IS NOT NULL AND id_bandera IS NOT NULL AND id_sucursal IS NOT NULL AND id_producto IS NOT NULL",
    "precio_lista_valido": "precio_lista IS NOT NULL AND precio_lista > 0",
}

reglas_advertencia_precios = {
    "descripcion_informada": "descripcion IS NOT NULL",
    "marca_informada": "marca IS NOT NULL",
    "precio_referencia_valido": "precio_referencia IS NULL OR precio_referencia > 0",
    "promo1_no_supera_lista": "precio_promo1 IS NULL OR precio_promo1 <= precio_lista",
}


@dp.temporary_view(name="productos_limpios")
@dp.expect_all_or_drop(reglas_criticas_precios)
@dp.expect_all(reglas_advertencia_precios)
def productos_limpios():
    return spark.readStream.table("sepa.bronze.productos").select(
        texto("id_comercio").alias("id_comercio"),
        texto("id_bandera").alias("id_bandera"),
        texto("id_sucursal").alias("id_sucursal"),
        texto("id_producto").alias("id_producto"),
        F.to_date("fecha").alias("fecha"),
        (texto("productos_ean") == "1").alias("es_ean"),
        texto("productos_descripcion").alias("descripcion"),
        texto("productos_marca").alias("marca"),
        numero("productos_cantidad_presentacion", "double").alias("cantidad_presentacion"),
        F.lower(texto("productos_unidad_medida_presentacion")).alias("unidad_presentacion"),
        numero("productos_precio_lista").alias("precio_lista"),
        numero("productos_precio_referencia").alias("precio_referencia"),
        numero("productos_cantidad_referencia", "double").alias("cantidad_referencia"),
        F.lower(texto("productos_unidad_medida_referencia")).alias("unidad_referencia"),
        numero("productos_precio_unitario_promo1").alias("precio_promo1"),
        texto("productos_leyenda_promo1").alias("leyenda_promo1"),
        numero("productos_precio_unitario_promo2").alias("precio_promo2"),
        texto("productos_leyenda_promo2").alias("leyenda_promo2"),
        F.col("archivo_origen"),
    )


dp.create_streaming_table(
    name="sepa.silver.precios_historial",
    comment="Historial de precios (SCD2): una fila por cada cambio de precio de un producto en una sucursal",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="sepa.silver.precios_historial",
    source="productos_limpios",
    keys=["id_comercio", "id_bandera", "id_sucursal", "id_producto"],
    sequence_by="fecha",
    stored_as_scd_type=2,
    track_history_column_list=["precio_lista", "precio_promo1", "precio_promo2"],
)


# ============================================================
# SUCURSALES: vista de limpieza + Auto CDC SCD1
# ============================================================

reglas_advertencia_sucursales = {
    "latitud_en_argentina": "latitud IS NULL OR latitud BETWEEN -56 AND -21",
    "longitud_en_argentina": "longitud IS NULL OR longitud BETWEEN -74 AND -53",
    "provincia_formato_iso": "provincia RLIKE '^AR-[A-Z]$'",
    "codigo_postal_informado": "codigo_postal IS NOT NULL",
    "provincia_coincide_con_coordenadas": "ubicacion_consistente",
}


@dp.temporary_view(name="sucursales_limpias")
@dp.expect_all_or_drop({"clave_completa": "id_comercio IS NOT NULL AND id_bandera IS NOT NULL AND id_sucursal IS NOT NULL"})
@dp.expect_all(reglas_advertencia_sucursales)
def sucursales_limpias():
    cp = F.regexp_extract(F.col("sucursales_codigo_postal"), r"(\d{4})", 1)
    return spark.readStream.table("sepa.bronze.sucursales").select(
        texto("id_comercio").alias("id_comercio"),
        texto("id_bandera").alias("id_bandera"),
        texto("id_sucursal").alias("id_sucursal"),
        F.to_date("fecha").alias("fecha"),
        texto("sucursales_nombre").alias("nombre"),
        texto("sucursales_tipo").alias("tipo"),
        texto("sucursales_calle").alias("calle"),
        texto("sucursales_numero").alias("numero"),
        numero("sucursales_latitud", "double").alias("latitud"),
        numero("sucursales_longitud", "double").alias("longitud"),
        texto("sucursales_barrio").alias("barrio"),
        F.when(cp == "", None).otherwise(cp).alias("codigo_postal"),
        texto("sucursales_localidad").alias("localidad"),
        F.upper(texto("sucursales_provincia")).alias("provincia"),
        texto("sucursales_lunes_horario_atencion").alias("horario_lunes"),
        texto("sucursales_martes_horario_atencion").alias("horario_martes"),
        texto("sucursales_miercoles_horario_atencion").alias("horario_miercoles"),
        texto("sucursales_jueves_horario_atencion").alias("horario_jueves"),
        texto("sucursales_viernes_horario_atencion").alias("horario_viernes"),
        texto("sucursales_sabado_horario_atencion").alias("horario_sabado"),
        texto("sucursales_domingo_horario_atencion").alias("horario_domingo"),
        F.col("archivo_origen"),
    ).withColumn(
        # Falso si la sucursal dice estar en CABA pero sus coordenadas caen fuera de la Ciudad
        "ubicacion_consistente",
        F.expr("""
            coalesce(
                NOT (provincia = 'AR-C'
                     AND latitud IS NOT NULL AND longitud IS NOT NULL
                     AND NOT (latitud BETWEEN -34.71 AND -34.52
                              AND longitud BETWEEN -58.54 AND -58.33)),
                true)
        """),
    )


dp.create_streaming_table(
    name="sepa.silver.sucursales",
    comment="Sucursales con sus datos actuales (SCD1)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="sepa.silver.sucursales",
    source="sucursales_limpias",
    keys=["id_comercio", "id_bandera", "id_sucursal"],
    sequence_by="fecha",
    stored_as_scd_type=1,
)


# ============================================================
# COMERCIOS: vista de limpieza + Auto CDC SCD1
# ============================================================

@dp.temporary_view(name="comercios_limpios")
@dp.expect_all_or_drop({"clave_completa": "id_comercio IS NOT NULL AND id_bandera IS NOT NULL"})
@dp.expect_all({"cuit_11_digitos": "cuit RLIKE '^[0-9]{11}$'"})
def comercios_limpios():
    return spark.readStream.table("sepa.bronze.comercio").select(
        texto("id_comercio").alias("id_comercio"),
        texto("id_bandera").alias("id_bandera"),
        F.to_date("fecha").alias("fecha"),
        texto("comercio_cuit").alias("cuit"),
        texto("comercio_razon_social").alias("razon_social"),
        texto("comercio_bandera_nombre").alias("bandera_nombre"),
        texto("comercio_bandera_url").alias("bandera_url"),
        F.expr("try_cast(trim(comercio_ultima_actualizacion) AS timestamp)").alias("ultima_actualizacion"),
        texto("comercio_version_sepa").alias("version_sepa"),
        F.col("archivo_origen"),
    )


dp.create_streaming_table(
    name="sepa.silver.comercio",
    comment="Comercios y banderas con sus datos actuales (SCD1)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="sepa.silver.comercio",
    source="comercios_limpios",
    keys=["id_comercio", "id_bandera"],
    sequence_by="fecha",
    stored_as_scd_type=1,
)
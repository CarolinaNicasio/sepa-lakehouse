# sepa-lakehouse

Pipeline de datos de **precios de supermercados de Argentina** construido en **Databricks** con **Lakeflow Spark Declarative Pipelines (SDP)**, arquitectura medallón y Unity Catalog.

Procesa la base pública **SEPA / Precios Claros** (Secretaría de Comercio): unos **14 millones de precios por día**, informados por las grandes cadenas para cada producto en cada sucursal del país. El pipeline construye un **historial de precios (SCD tipo 2)** y responde preguntas de negocio: qué cadena es más barata, cuánto subieron los precios en la semana y en qué productos conviene comparar antes de comprar.

> Datos: 7 días (15/10/2026 al 21/10/2026), **~99,9 millones de filas** de entrada.

---

## Resultados principales

| Pregunta | Resultado |
|---|---|
| ¿Cuánto se comprime el historial? | **99,9 M filas** de entrada → **17,7 M versiones** en el historial SCD2 (**82% menos**) sin perder información |
| ¿Cuántos precios cambian por día? | Entre **108 mil** (domingo) y **1,07 millones** (miércoles): la actualización de precios se concentra en días hábiles |
| ¿Cuánto subieron los precios en la semana? | Las cadenas grandes subieron entre **+0,2% y +0,4%**; **La Anónima (+1,49%)** y **Vea (+1,23%)** fueron las que más ajustaron |
| ¿Qué cadena es más barata? | Entre las cadenas con datos confiables: **Maxi** y **La Agrícola** (índice 93-94); las más caras: **Jumbo, Disco y La Anónima** (104-105) |
| ¿Se pueden creer todos los datos? | No. El historial reveló que **Unicoop** y **California Supermercados** informan **precios congelados**: con un solo día parecían las cadenas más baratas del país |

El detalle de los problemas de calidad encontrados está en **[docs/hallazgos_calidad.md](docs/hallazgos_calidad.md)**.

---

## Arquitectura

```
Portal SEPA (zip diario)
        │
        ▼
┌──────────────────────────────────────────────────────────────┐
│ LANDING  (Volume sepa.landing.raw)                           │
│  zips/sepa_AAAA-MM-DD.zip                                    │
│  csv/fecha=AAAA-MM-DD/comercio=.../{comercio,sucursales,     │
│                                    productos}.csv            │
│  Notebook 01_descomprimir_sepa: descomprime, repara filas    │
│  partidas y marca cada día procesado con _OK                 │
└──────────────────────────────────────────────────────────────┘
        │  Auto Loader (solo archivos nuevos)
        ▼
┌──────────────────────────────────────────────────────────────┐
│ BRONZE  (streaming tables, todo como texto)                  │
│  comercio · sucursales · productos                           │
└──────────────────────────────────────────────────────────────┘
        │  vistas de limpieza + expectations
        ▼
┌──────────────────────────────────────────────────────────────┐
│ SILVER  (Auto CDC)                                           │
│  precios_historial  → SCD2 (historial de cada cambio)        │
│  sucursales, comercio → SCD1 (dato actual)                   │
└──────────────────────────────────────────────────────────────┘
        │  materialized views
        ▼
┌──────────────────────────────────────────────────────────────┐
│ GOLD                                                         │
│  Modelo:   dim_comercio · dim_sucursal · fct_precios_vigentes│
│  Análisis: precio_producto_cadena · precio_mercado           │
│            indice_precios_cadena · dispersion_productos      │
│            variacion_precios_cadena · precios_atipicos       │
└──────────────────────────────────────────────────────────────┘
```

### Tablas

| Capa | Tabla | Tipo | Contenido |
|---|---|---|---|
| Bronze | `comercio`, `sucursales`, `productos` | Streaming table (Auto Loader, append) | Datos crudos, sin tipar, con columnas de auditoría (`archivo_origen`, `fecha_ingesta`, `fecha`, `comercio`) |
| Silver | `precios_historial` | Streaming table (Auto CDC, **SCD2**) | Una versión por cada cambio de precio de un producto en una sucursal (`__START_AT` / `__END_AT`) |
| Silver | `sucursales`, `comercio` | Streaming table (Auto CDC, SCD1) | Dato actual, limpio y tipado |
| Gold | `dim_comercio`, `dim_sucursal` | Materialized view | Dimensiones; la sucursal incluye provincia y región |
| Gold | `fct_precios_vigentes` | Materialized view | Precio vigente de cada producto en cada sucursal |
| Gold | `precio_producto_cadena` | Materialized view | Precio mediano de cada producto (EAN) en cada cadena |
| Gold | `precio_mercado` | Materialized view | Precio de referencia de cada producto vendido por 3 o más cadenas |
| Gold | `indice_precios_cadena` | Materialized view | Índice de precios por cadena (100 = mercado) |
| Gold | `dispersion_productos` | Materialized view | Diferencia de precio de un mismo producto entre cadenas |
| Gold | `variacion_precios_cadena` | Materialized view | Variación de precios entre el primer y el último día (misma canasta) |
| Gold | `precios_atipicos` | Materialized view | Precios 5 veces por encima o por debajo del mercado (probables errores de carga) |

---

## Decisiones de diseño

- **Las correcciones de formato se hacen en landing, no en el pipeline.** Un archivo mal formado (filas partidas en varias líneas) se repara como texto antes de que lo lea Spark. El pipeline recibe siempre archivos sanos.
- **Bronze guarda todo como texto.** Los códigos de producto tienen ceros a la izquierda (`0000000023897`) que se perderían al leerlos como número. Los tipos se asignan en silver.
- **SCD2 solo sobre los precios.** `track_history_column_list` limita las versiones nuevas a cambios en `precio_lista` y las promociones; un cambio de descripción no genera historial.
- **Calidad en dos niveles.** Las reglas críticas (clave completa, precio mayor a 0) descartan la fila (`expect_all_or_drop`); las de advertencia (marca informada, coordenadas dentro de Argentina, formato de provincia) solo se registran (`expect_all`).
- **Comparaciones entre cadenas solo con código EAN.** El EAN identifica el mismo producto en cualquier cadena; los códigos internos no son comparables.
- **Mediana en lugar de promedio y filtro de atípicos.** Un solo precio mal cargado ($0,01) distorsiona un promedio o un mínimo. Los precios que se alejan más de 5 veces del mercado se excluyen de los cálculos y se guardan en `precios_atipicos` para no ocultarlos.
- **Ingesta idempotente.** El notebook detecta qué días faltan procesar (marca `_OK`); se puede ejecutar varias veces sin duplicar nada y retoma bien si se corta.

---

## Estructura del repositorio

```
sepa-lakehouse/
├── README.md
├── docs/
│   └── hallazgos_calidad.md        # problemas de calidad encontrados en los datos
├── setup/
│   └── 00_crear_estructura.sql     # catálogo, esquemas y volume
├── ingesta/
│   └── 01_descomprimir_sepa        # notebook: descomprime y repara los archivos
├── pipeline/
│   └── transformations/
│       ├── bronze/bronze_sepa.py
│       ├── silver/silver_sepa.py
│       └── gold/
│           ├── gold_modelo.sql
│           └── gold_analisis.sql
└── analisis/
    ├── verificacion_bronze.sql
    ├── verificacion_silver.sql
    ├── verificacion_gold.sql
    └── verificacion_historial.sql
```

---

## Cómo ejecutarlo

Requisitos: una cuenta de **Databricks Free Edition** (sin costo).

1. **Estructura:** ejecutar `setup/00_crear_estructura.sql` en el SQL Editor.
2. **Datos:** descargar los zips diarios de [Precios Claros - Base SEPA](https://datos.produccion.gob.ar/dataset/sepa-precios), renombrarlos como `sepa_AAAA-MM-DD.zip` y subirlos a `/Volumes/sepa/landing/raw/zips/`.
   El portal publica solo los últimos 7 días (cada archivo se reemplaza a la semana), así que el historial se construye descargando periódicamente.
3. **Ingesta:** ejecutar el notebook `ingesta/01_descomprimir_sepa` (Run all). Procesa solo los días pendientes.
4. **Pipeline:** crear un ETL pipeline con raíz en `pipeline/` (catálogo `sepa`) y ejecutarlo con **Run pipeline**.
5. **Verificación:** ejecutar las consultas de `analisis/`.

---

## Limitaciones conocidas

- **Productos discontinuados.** Auto CDC procesa cambios, no ausencias: si un comercio deja de informar un producto (o no informa un día), su último precio queda como vigente. La mejora prevista es usar `AUTO CDC FROM SNAPSHOT`, que compara fotos completas día a día.
- **Sesgo de cobertura.** El 71% de las sucursales están en CABA y provincia de Buenos Aires. Los promedios "nacionales" reflejan sobre todo al AMBA.
- **Canastas distintas.** Cada cadena se compara con los productos que vende; el índice de cadenas con pocos productos (menos de ~500) es menos confiable.
- **Sin categorías de producto.** SEPA no informa rubro, así que no es posible medir inflación por categoría sin clasificar las descripciones.
- **Período corto.** Una semana de datos permite ver tendencias, no conclusiones de inflación mensual.

---

## Próximos pasos

- Dashboard en Databricks (AI/BI) con el ranking de cadenas, la variación semanal y un panel de calidad de datos.
- Orquestación con Lakeflow Jobs: ingesta → pipeline → alerta, con ejecución diaria.
- `AUTO CDC FROM SNAPSHOT` para detectar productos discontinuados.
- Conversión de precios a dólares con la API pública del BCRA.

---

## Tecnologías

Databricks Free Edition · Lakeflow Spark Declarative Pipelines · Auto Loader · Auto CDC (SCD1/SCD2) · Expectations · Unity Catalog (Volumes) · Delta Lake · PySpark · SQL · Git folders

## Fuente de datos

Precios Claros – Base SEPA, Secretaría de Comercio de la Nación (datos.produccion.gob.ar), licencia **Creative Commons Attribution 4.0**. Los datos fueron procesados y transformados para este proyecto.

## Autora

Carolina Nicasio · [GitHub](https://github.com/CarolinaNicasio)
# Hallazgos de calidad de datos

Problemas encontrados en la base SEPA durante la construcción del pipeline, cómo se detectaron, su impacto y cómo se resolvieron.

Período analizado: 15/10/2026 al 21/10/2026 (7 días, ~99,9 millones de filas).

## Resumen

| # | Hallazgo | Impacto | Estado |
|---|---|---|---|
| 1 | Filas de sucursales partidas en varias líneas (Comodín) | Se perdían dirección y provincia de 25 sucursales | ✅ Resuelto en ingesta |
| 2 | Precios congelados (Unicoop) | Aparecía como la cadena más barata del país | ⚠️ Detectado, se excluye del ranking |
| 3 | Precios desactualizados (California Supermercados) | Índice de precios artificialmente bajo | ⚠️ Detectado, se excluye del ranking |
| 4 | Precios de relleno y errores de carga | Distorsionaban mínimos, máximos y dispersión | ✅ Separados en `precios_atipicos` |
| 5 | Mismo código EAN para productos distintos | Comparaciones de precio incorrectas | ⚠️ Detectado |
| 6 | Productos duplicados (comercio 5) | 49 claves repetidas | ✅ Resuelto con Auto CDC |
| 7 | Provincia faltante o mal escrita | 37 sucursales sin región | ⚠️ Parcial |
| 8 | Formato de archivo distinto a la especificación | Nombres de columna inválidos y columnas duplicadas | ✅ Resuelto en bronze |
| 9 | Comercios que no informan todos los días | Precios que quedan como vigentes sin datos nuevos | ⚠️ Limitación documentada |
| 10 | Campos incompletos o mal cargados | Marca vacía, presentación incorrecta, código postal con letras | ✅ Normalizado en silver |

---

## 1. Filas partidas en varias líneas (Comodín, comercio 6)

**Cómo se detectó.** En gold, 62 sucursales aparecían con provincia "Sin dato". Al filtrar por comercio, 25 eran de Comodín, con todos los campos vacíos después de la latitud. Leyendo el archivo crudo con Python (`repr`) se vio que cada sucursal venía partida en varias líneas:

```
6|1|1|Comodin 1|Hipermercado|Necochea|445|-24.183838
|-65.304235
| |Centro|4600|San Salvador de Jujuy|AR-Y|08:00 a 22:00|...
```

Las sucursales con horario cortado ocupaban hasta 8 líneas (un horario por línea). En total, **111 líneas sueltas por día** en 25 sucursales.

**Impacto.** Spark lee una línea como una fila. La primera línea quedaba como una sucursal incompleta y las demás se **descartaban sin aviso**: empezaban con `|`, por lo que su `id_comercio` quedaba vacío y el filtro de bronze las eliminaba. Se perdían longitud, código postal, localidad y provincia de todas las sucursales de la cadena.

**Solución.** Función `reparar_csv` en el notebook de ingesta: une líneas hasta que la fila tiene la misma cantidad de separadores que el encabezado. Se aplica a todos los archivos de todos los días. El problema se repite **los 7 días** (25 filas reparadas por día); los archivos de productos no estaban afectados.

**Resultado.**

| Provincia | Antes | Después |
|---|---|---|
| Sin dato | 62 | **37** |
| Jujuy | 6 | **24** |
| Salta | 46 | **50** |
| Tucumán | 23 | **26** |

La cobertura de la región NOA pasó de 93 a 118 sucursales (+27%).

**Lección.** Un filtro aparentemente inofensivo (descartar filas sin `id_comercio`) ocultaba una pérdida de datos.

---

## 2. Precios congelados (Sup. Unicoop)

**Cómo se detectó.** Con un solo día de datos, Unicoop tenía un índice de precios de **78,4**: un 22% más barata que el mercado y por lejos la más barata del país. Su porcentaje de precios atípicos era bajo (0,1%), así que no se explicaba por errores de carga.

Con 7 días de historial, la variación semanal mostró:

| Cadena | Precios comparados | Variación | Subieron | Bajaron |
|---|---|---|---|---|
| Sup. Unicoop | 19.272 | **0,00%** | **0%** | **0%** |
| Promedio de cadenas grandes | — | +0,2% a +0,4% | 3% a 6% | ~1% |

**Conclusión.** En una semana en la que todas las cadenas movieron precios, Unicoop no cambió ninguno. Su índice bajo no refleja precios baratos sino **precios desactualizados**. Este hallazgo solo fue posible gracias al historial SCD2.

---

## 3. Precios desactualizados (California Supermercados)

**Cómo se detectó.** 14 de sus 556 precios comparables (**2,5%**) aparecen como atípicos, contra 0% a 0,2% en las cadenas grandes. Todos con un patrón similar: entre el 3% y el 4% del precio de mercado.

| Producto | Precio California | Precio de mercado |
|---|---|---|
| Mostaza Brennan 200 g | $219,00 | $7.000,00 |
| Leche en polvo Sancor 800 g | $461,50 | $13.725,99 |
| Coca-Cola Light 1,5 L | $182,90 | $4.190,00 |

En la semana, solo el **0,7%** de sus productos cambió de precio (variación +0,03%).

**Conclusión.** Precios compatibles con listas viejas que no se actualizan. Su índice (91,3, segunda "más barata") no es confiable.

---

## 4. Precios de relleno y errores de carga

**Cómo se detectó.** La tabla de dispersión (precio mínimo y máximo de un producto entre cadenas) mostraba diferencias de hasta **65 millones por ciento**.

| Cadena | Producto | Precio | Precio de mercado |
|---|---|---|---|
| Estación Lima | Queso Finlandia cheddar 150 g | **$0,01** | $6.109 |
| HiperChangomas / Changomas | Comidas preparadas (empanadas, ravioles, pollo) | **$3,50** | $2.700 a $10.900 |
| Sup. Unicoop | Whisky Chivas 700 ml | **$373** | $113.665 |

**Solución.** Se define el precio de mercado de cada producto (mediana entre cadenas) y se separan los precios que se alejan más de 5 veces (`relacion` fuera de 0,2 a 5) en la tabla `precios_atipicos`: **114 precios** (menos del 0,05% del total). Se excluyen del índice, la dispersión y la variación, pero **no se borran**.

---

## 5. Mismo código EAN, distinto producto

**Ejemplo.** "Sugus caramelo masticable suelto x 3,3 g" a **$50** en Estación Lima y Deheza, contra un precio de mercado de $12.050.

**Interpretación.** Probablemente el precio correcto es el bajo (un caramelo suelto), y otras cadenas usan el mismo código EAN para la bolsa. Un precio "atípico" no siempre es un error de quien parece raro: puede ser un problema de identificación del producto.

---

## 6. Productos duplicados (comercio 5)

**Cómo se detectó.** Consulta de claves repetidas (comercio + bandera + sucursal + producto) en bronze.

**Detalle.** 7 productos importados (sopas Maruchan, Old Spice, Rexona, Oral-B) informados dos veces en sus 7 sucursales: **49 claves duplicadas**, todas con el mismo precio.

**Solución.** Auto CDC conserva una fila por clave. En silver: **0 duplicados**.

---

## 7. Provincia faltante o mal escrita

| Comercio | Valor informado | Sucursales |
|---|---|---|
| 10 | vacío | 36 |
| 15 | `BUENOS AIRES` (en lugar de `AR-B`) | 1 |

El comercio 10 no tiene filas partidas: directamente no informa la provincia. Quedan **37 sucursales (1,4%)** sin región.

**Pendiente.** Traducir nombres de provincia escritos en texto; para el comercio 10, evaluar deducir la provincia por coordenadas o localidad.

---

## 8. Formato de archivo distinto a la especificación

| Problema | Detalle | Solución |
|---|---|---|
| Fin de línea Windows (`\r\n`) | El `\r` queda pegado a la última columna (`productos_leyenda_promo2\r`), un nombre inválido para Delta | Limpieza de nombres de columna en bronze |
| Comercios con distinto formato | Unos usan `\r\n` y otros `\n`: Spark ve dos columnas distintas que, al limpiarse, quedan con el mismo nombre | Fusión de columnas con `coalesce` en bronze |
| Línea de cierre | Cada archivo termina con una línea en blanco y "Ultima actualizacion: ..." (la especificación dice "Última actualización", con tildes) | Filtro en bronze |
| Marca BOM | Los archivos empiezan con `﻿` (UTF-8 con BOM) | Spark la maneja; sin impacto |

---

## 9. Comercios que no informan todos los días

| Comercio | Días informados (de 7) |
|---|---|
| comercio-sepa-24 | 3 |
| comercio-sepa-20 | 5 |
| comercio-sepa-8 | 6 |
| comercio-sepa-36 | 0 (zip vacío todos los días) |

**Impacto.** Cuando un comercio no informa un día, su último precio queda como vigente en el historial. Sus precios pueden aparecer "sin cambios" cuando en realidad no hay datos.

**Mejora prevista.** `AUTO CDC FROM SNAPSHOT`, que compara fotos completas y detecta ausencias.

---

## 10. Campos incompletos o mal cargados

| Problema | Ejemplo | Tratamiento |
|---|---|---|
| Marca vacía | El comercio 4 no informa marca en ningún producto (`" "`) | Se convierte a `null`; expectation `marca_informada` lo mide |
| Presentación incorrecta | "7UP Free PET x 1.5L" informado como "1.00 unidad": el precio de referencia (por litro) no sirve | Las comparaciones usan precio de lista y código EAN |
| Código postal con letras | `C1073AAQ` (la especificación pide solo números) | Se extraen los 4 dígitos: `1073` |
| Ceros a la izquierda | Códigos internos como `0000000023897` | Se leen como texto en todas las capas |
| Mayúsculas inconsistentes | "Wrigleys Spear 12.5g" junto a descripciones en mayúsculas | Se normaliza con `upper` en gold |
| Bandera repetida | `comercio` tiene 38 filas en bronze para 27 banderas distintas | Auto CDC conserva una por clave |

---

## Índice de precios por cadena y confiabilidad

Índice al 15/10/2026 (100 = precio de mercado). La columna de confiabilidad combina el porcentaje de precios atípicos y la variación semanal.

| Cadena | Productos comparados | Índice | Confiabilidad |
|---|---|---|---|
| Sup. Unicoop | 3.966 | 78,4 | ❌ Precios congelados |
| California Supermercados | 542 | 91,3 | ❌ Precios desactualizados |
| La Agrícola Regional Coop. | 1.026 | 93,3 | ✅ |
| Maxi | 12.680 | 93,9 | ✅ |
| Supermercados Comodín | 623 | 97,1 | ✅ |
| SuperChangomas | 24.968 | 98,2 | ✅ |
| Vea | 9.619 | 98,7 | ✅ |
| Supermercados DIA | 3.058 | 99,6 | ✅ |
| Hipermercado Carrefour | 17.918 | 100,0 | ✅ |
| COTO CICSA | 10.179 | 100,8 | ✅ |
| Changomas | 24.979 | 101,0 | ✅ |
| Jumbo | 9.809 | 104,0 | ✅ |
| La Anónima | 7.071 | 104,8 | ✅ |
| Disco | 9.542 | 104,9 | ✅ |
| Estación Lima | 121 | 123,6 | ⚠️ Canasta chica, sin cambios en la semana |
| Deheza | 121 | 131,2 | ⚠️ Canasta chica, sin cambios en la semana |
| Axion Energy | 123 | 136,1 | ⚠️ Canasta chica (tienda de estación de servicio) |

*Selección de cadenas; el ranking completo está en `sepa.gold.indice_precios_cadena`.*
-- =============================================================
-- 00_crear_estructura.sql
-- Proyecto: sepa-lakehouse
-- Objetivo: crear el catalogo, los esquemas de la arquitectura
--           medallon y el volume donde aterrizan los archivos.
-- Se ejecuta UNA SOLA VEZ, al iniciar el proyecto.
-- =============================================================

-- Catalogo principal del proyecto
CREATE CATALOG IF NOT EXISTS sepa;

-- landing: archivos crudos tal como llegan (zips y CSV)
CREATE SCHEMA IF NOT EXISTS sepa.landing;

-- bronze: tablas con los datos crudos, sin transformar
CREATE SCHEMA IF NOT EXISTS sepa.bronze;

-- silver: datos limpios, tipados y sin duplicados
CREATE SCHEMA IF NOT EXISTS sepa.silver;

-- gold: tablas listas para analisis y dashboards
CREATE SCHEMA IF NOT EXISTS sepa.gold;

-- Volume donde se suben los zips y se guardan los CSV descomprimidos
CREATE VOLUME IF NOT EXISTS sepa.landing.raw;
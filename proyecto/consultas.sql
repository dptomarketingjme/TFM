-- Creación de Modelo1 DVS

CREATE OR REPLACE MODEL `wide-oasis-135923.iamarketingdvs.modelodefinitivo1` -- Se usa la clausula create or replace model para crear el primer modelo sobre el proyecto wide-oasis
OPTIONS(model_type='logistic_reg') AS -- Se selecciona regresion logistica
SELECT
IFNULL(SUM(( SELECT value FROM UNNEST(hits.customMetrics) WHERE index = 2)),0) AS label, -- Se establece la metrica personalizada2 como label to predict
date, case when hits.hour > 7 and hits.hour < 13 then 'manana' when hits.hour > 13 and hits.hour < 20 then 'tarde' else 'noche' end as momentodia, --creacion de dimension para el momento del dia basandonos en la hora del hit
 hits.hour as hora, clientId, geoNetwork.city as ciudad, device.deviceCategory as categoriaDispositivo, device.mobileDeviceBranding as marca, device.browser as navegador, trafficSource.source as fuente, trafficSource.campaign as campana, (SELECT value FROM UNNEST(session.customDimensions) WHERE index = 14 GROUP BY 1) AS producto, device.operatingSystem as OS, sum(totals.visits) as visitas, CASE WHEN sum(totals.transactions) < 1 THEN 0 ELSE totals.transactions END as subs
 --seleccion de otras dimensiones y métricas que conformarán la tabla para realizar el modelo 
FROM `wide-oasis-135923.140393857.ga_sessions_*` as session,-- seleccion de origen de los datos. La cuenta de RollUp de todos los productos de España

UNNEST(hits) AS hits-- Se desanida la dimension hit para poder acceder a sus campos como hora, o metricas personalizadas

where _table_suffix BETWEEN '20190101' AND '20191001' -- Se selecciona la fecha del train, hacemos 9 meses

and trafficSource.campaign like '%range%' -- Se filtran las campañas por nombre para aplicar el modelo solo a campañas dirigidas a usuarios Orange
and trafficSource.source like '%google%' -- Se selecciona como fuente de tráfico google
and totals.transactions > 0 -- IMPORTANTE, se trabaja sólo sobre usuarios suscritos, éstá explicado en la memoria, pero el ratio de conversión de visita a transacción es menor a un 1%, lo que desbalancea el dataset de train, por eso vamos a trabajar con usuarios suscritos que son o no cobrados.
GROUP BY date, clientId, ciudad, marca, categoriaDispositivo, totals.transactions, navegador, campana, fuente, producto, OS, hora --agrupaciones

-- creacion del modelo 2 DVS

CREATE OR REPLACE MODEL `wide-oasis-135923.iamarketingdvs.modelodefinitivo2` 
OPTIONS(model_type='logistic_reg') AS
SELECT

IFNULL(SUM(( SELECT value FROM UNNEST(hits.customMetrics) WHERE index = 2)),0) AS label,
date, case when hits.hour > 7 and hits.hour < 13 then 'manana' when hits.hour > 13 and hits.hour < 20 then 'tarde' else 'noche' end as momentodia, clientId, geoNetwork.city as ciudad, device.mobileDeviceBranding as marca, device.browser as navegador, trafficSource.campaign as campana, (SELECT value FROM UNNEST(session.customDimensions) WHERE index = 14 GROUP BY 1) AS producto, device.operatingSystem as OS, sum(totals.visits) as visitas
-- La principal diferencia con el anterior es el numero de dimensiones y métricas que se usan para crear el modelo.
FROM `wide-oasis-135923.140393857.ga_sessions_*` as session,
UNNEST(hits) AS hits
where _table_suffix BETWEEN '20190101' AND '20191001'
and trafficSource.campaign like '%range%'
and trafficSource.source like '%google%'
and totals.transactions > 0
GROUP BY date, clientId, ciudad, marca, navegador, campana, producto, OS, momentodia

-- Creacion del modelo de la merchandise store

CREATE OR REPLACE MODEL `ml-small.MLsmalldataset.modeloGoogleTiendaGogle1` -- Se usa la clausula create or replace model para crear el primer modelo sobre el proyecto ml-small, es la consulta base del tutorial de la web, no tiene misterio alguno, se seleccionan las transaccciones como label to predict y se usan las visitas, el sistema operativo y el pais como columnas para la creacion del modelo
OPTIONS(model_type='logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170701' -- Entrenamos con datos de 11 meses

-- primer intento de mejora del modelo de googleCREATE OR REPLACE MODEL `ml-small.MLsmalldataset.upgradeTiendaGoogle1`
OPTIONS(model_type='logistic_reg') AS
SELECT
   
  IF(totals.transactions IS NULL, 0, 1) AS label,
  fullvisitorId as cliente,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(geoNetwork.city, "") AS ciudad,
  IFNULL(device.deviceCategory, "") AS categoriaDispositivo,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(totals.visits, 0) AS sesiones,
  IFNULL(totals.UniqueScreenViews, 0) AS visitasUnicas,
  IFNULL(totals.timeOnSite, 0) AS TiempoEnWeb,
  IFNULL(totals.sessionQualityDim, 0) AS calidadSesion
  -- se seleccionan más metricas y dimensiones
  
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'

-- Omitimos todos los intentos modificando las consultas, ya que son iguales

-- Las consultas para evaluar quedan así


SELECT -- se seleccionan todos los campos del modelo para hacer un subselect después
  * 
FROM ML.EVALUATE(MODEL `ml-small.MLsmalldataset.upgradeTiendaGoogle1`, (
  SELECT
  
  IF(totals.transactions IS NULL, 0, 1) AS label,
  fullvisitorId as cliente,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(geoNetwork.city, "") AS ciudad,
  IFNULL(device.deviceCategory, "") AS categoriaDispositivo,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(totals.visits, 0) AS sesiones,
  IFNULL(totals.UniqueScreenViews, 0) AS visitasUnicas,
  IFNULL(totals.timeOnSite, 0) AS TiempoEnWeb,
  IFNULL(totals.sessionQualityDim, 0) AS calidadSesion
	
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'))

-- consulta para comprobaer disponibilidad de campos, ante el extraño resultado de la evaluación, no estoy seguro de tener disponibles estos campos en el dataset de ejemplo de google
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.browser, "") AS navegador,
  IFNULL(geoNetwork.city, "") AS city,
  IFNULL(totals.visits, 0) AS sesiones
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170801' AND '20170801'

-- las consultas para predecir son iguales que las de evaluación, con la seleccion de campos a predecir y el subselect de los campos del modelo después, el resultado de esta consulta se guarda en una tabla llamada all_predicted_GMS

SELECT
  os, country, fullvisitorId,
  SUM(predicted_label) as totalTransactionsPredicted -- campos a predecir
FROM ML.PREDICT(MODEL `ml-small.MLsmalldataset.modeloGoogleTiendaGogle1`, (
  SELECT 
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(geoNetwork.country, "") AS country,
    fullvisitorId
	subselect de campos del modelo
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'))
  GROUP BY os, country, fullvisitorId -- agrupación de campos en la tabla de predicción
  ORDER BY totalTransactionsPredicted DESC


-- consulta para obtener resultados reales, el resultado será explorado desde Tableau y se guarda como tabla de BQ llamada all_reales_GMS

SELECT
    sum(totals.transactions) as realTransactions,
    IFNULL(device.operatingSystem, "") AS os,
    sum(totals.pageviews) AS pageviews,
    IFNULL(geoNetwork.country, "") AS country,
    fullvisitorId
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20170701' AND '20170731' -- se extraen resultados de julio, el mes que no entró en el train
  GROUP BY os, country, fullvisitorId
  ORDER BY realTransactions DESC

-- consulta para el predict del modelo de billing, del caso 1

SELECT
  *
FROM ML.PREDICT(MODEL `wide-oasis-135923.iamarketingdvs.modelodefinitivo2`, (
SELECT
IFNULL(SUM(( SELECT value FROM UNNEST(hits.customMetrics) WHERE index = 2)),0) AS label,
date, case when hits.hour > 7 and hits.hour < 13 then 'manana' when hits.hour > 13 and hits.hour < 20 then 'tarde' else 'noche' end as momentodia, hits.hour as hora, clientId, geoNetwork.city as ciudad, device.deviceCategory as categoriaDispositivo, device.mobileDeviceBranding as marca, device.browser as navegador, trafficSource.source as fuente, trafficSource.campaign as campana, (SELECT value FROM UNNEST(session.customDimensions) WHERE index = 14 GROUP BY 1) AS producto, device.operatingSystem as OS, sum(totals.visits) as visitas, CASE WHEN sum(totals.transactions) < 1 THEN 0 ELSE totals.transactions END as subs, IFNULL(SUM(( SELECT value FROM UNNEST(hits.customMetrics) WHERE index = 2)),0) AS Fbilled
FROM `wide-oasis-135923.140393857.ga_sessions_*` as session,
UNNEST(hits) AS hits
where _table_suffix BETWEEN '20191002' AND '20191030' -- se predicen resultados de octubre de este año
and trafficSource.campaign like '%range%'
and trafficSource.source like '%google%'
and totals.transactions > 0
GROUP BY date, clientId, ciudad, marca, categoriaDispositivo, totals.transactions, navegador, campana, fuente, producto, OS, hora))

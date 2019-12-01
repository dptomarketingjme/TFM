En el repositorio hay 2 carpetas, una llamada "recursosAuxiliares" y otra "proyecto".

En la primera se pueden encontrar todo tipo de tablas y resultados intermedios que han conducido a ninguna parte.

En la segunda carpeta, se encuentran 4 archivos

1.- La memoria en word y pdf (MemoriaTFMLucasDiaz)
2.- Las consultas SQL más relevantes en (consultas.sql)
3.- El dashboard construido en Tableau con los resultados finales (prediccion_vs_reales_summary.twbx)

Como no utilicé notebooks, he "recreado" una estructura similar, poniendo todo el desarrollo del proyecto y el código en un documento autoexplicativo.

IMPORTANTE - ACCESOS

En el proyecto se usan 3 proyectos de BigQuery y 4 datasets:

wide-oasis-135923
	140393857
	iamarketingdvs
ml-small
	MLsmalldataset
bigquery-public-data
	google_analytics_sample

Necesitaréis solicitar acceso a la cuenta de prueba de Google Analytics aquí:
https://analytics.google.com/analytics/web/demoAccount

He concedido accesos a israel.herraiz@gmail.com y dani@mateos.io a los otros 2 proyectos de BigQuery. Sin embargo, el dataset 140393857 son datos más sensibles y voy a tener que solicitar acceso el lunes desde el trabajo (siento la falta de previsión, el lunes 2 a las 10:00h estará solucionado).

Una vez se tengan los accesos en orden, sólo habría que ir leyendo el pdf o el word y ejecutando las consultas en la consola web de BigQuery:
https://console.cloud.google.com/bigquery

Aunque hay alguna preview en la memoria, se puede abrir el documento de Tableau para ver los datos más en detalle con el software versión gratuita, descargar aquí:
https://www.tableau.com/es-es/products/desktop/download

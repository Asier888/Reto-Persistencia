# Almacenamiento de datos de Turbina Eólica en InfluxDB

Este proyecto implementa un sistema de ingesta y análisis de datos para una turbina eólica, utilizando un dataset SCADA real y almacenándolo en una base de datos de series temporales (InfluxDB) de forma segura y optimizada.

## Miembros del Equipo
- Asier Sánchez
- Alaia Yeregui

## Pasos Seguidos
1.  **Análisis y Limpieza:** Se procesó el dataset original para eliminar valores nulos e inconsistencias físicas (potencia negativa o ráfagas fuera de rango). 
2.  **Filtrado por Curva Teórica:** Se implementó un filtro de seguridad para descartar registros donde la potencia real se desviaba más de un 25% de la curva teórica del fabricante. Para ello también se analizaron gráficas.
3.  **Modelado en InfluxDB:** Se definió una estructura de datos basada en *Tags* (`turbine_id`) y *Fields* (`ActivePower`, `WindSpeed`) para optimizar las consultas temporales. 
4.  **Sincronización Temporal:** Se ajustaron las marcas de tiempo del dataset (originalmente de 2018) al año **2026** para permitir una monitorización simulada "en tiempo real".
5.  **Ingesta Masiva:** Se desarrolló un script en Python que utiliza la API de InfluxDB para cargar los datos en ráfagas (*batches*), minimizando la latencia de red.

## Instrucciones de Uso
1.  **Requisitos:** Instalar las librerías necesarias:
    ```
    pip install pandas influxdb-client python-dotenv matplotlib
    ```
2.  **Configuración de Seguridad:**
    * Crea un archivo `.env` basado en la plantilla del proyecto.
    * Introduce tu `INFLUX_TOKEN`, `ORG`, `BUCKET` y `URL`.
3.  **Ejecución:**
    * Hay que ejecutar `almacenamiento.py` para limpiar los datos y visualizar el cambio. Luego se hará la carga de datos en 
    la nube y la consulta de las agregaciones.

## Agregaciones y Visualización (Demostración)
Para verificar la inserción, se pueden ejecutar las siguientes consultas **Flux** en el Data Explorer de InfluxDB:

* **Promedio Horario de Potencia:**
    ```flux
    from(bucket: "turbina_eolica")
      |> range(start: 2026-01-01T00:00:00Z)
      |> filter(fn: (r) => r["_measurement"] == "mediciones_turbina")
      |> filter(fn: (r) => r["_field"] == "ActivePower")
      |> aggregateWindow(every: 1h, fn: mean)
    ```

* **Velocidad máxima del viento por día:**
    ```flux
    from (bucket: "turbina_eolica")
      |> range(start: -7d) '
      |> filter(fn: (r) => r["_measurement"] == "mediciones_turbina") '
      |> filter(fn: (r) => r["_field"] == "Wind Speed (m/s)") '
      |> aggregateWindow(every: 1d, fn: max, createEmpty: false)'
    ```


## Problemas / Retos Encontrados
* **Latencia en la Ingesta:** La inserción fila por fila mediante bucles era ineficiente para 50,000 registros. Se resolvió mediante el uso de `write_api` con soporte para DataFrames.
* **Gestión de Zonas Horarias:** Sincronizar objetos `datetime` de Pandas con los requisitos de precisión de nanosegundos de InfluxDB requirió una normalización estricta a UTC.
* **Seguridad de Credenciales:** Evitar el filtrado del Token de acceso en el código fuente mediante el uso de variables de entorno.
* **Plan gratuito:** Tiene una política de mantenimiento de datos de 30 días, por lo que al principio nos rechazaba el dataset porque los datos estaban cogidos durante un mes, e incluso llegabamos a intentar meter datos de 2025. 
Tras solucionar esto, ocurría una saturación al intentar todos los datos en una consulta, por eso nos basamos en el tipo batch, y creamos lotes de 500 que se tratan de meter cada 10segundos. 

## Alternativas Posibles
* **TimescaleDB:** Hubiera sido una opción válida si el proyecto requiriera cruzar datos de sensores con tablas relacionales complejas (ej. datos de empleados o facturación) usando SQL estándar.
* **QuestDB:** Una alternativa si la frecuencia de muestreo fuera de milisegundos, debido a su alto rendimiento de escritura en disco.

## Vías de Mejora
1.  **Mantenimiento Predictivo:** Implementar un modelo de Machine Learning que detecte anomalías cuando la potencia baje del umbral del 25% de forma sostenida.
2.  **Fuentes Externas:** Integrar una API meteorológica externa (OpenWeather) para validar si los anemómetros de la turbina requieren calibración.
3.  **Dashboard en Grafana:** Conectar InfluxDB a Grafana para crear alertas visuales y dashboards de control de planta profesionales.
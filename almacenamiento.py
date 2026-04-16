import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from cogerDatos import df  # Importamos el DataFrame ya procesado y limpio

load_dotenv()  # Cargar variables de entorno desde .env

# --- CONFIGURACIÓN DE SEGURIDAD ---
token = os.getenv("INFLUX_TOKEN")
org = os.getenv("INFLUX_ORG")
bucket = os.getenv("INFLUX_BUCKET")
url = os.getenv("INFLUX_URL")

# Verificación de seguridad rápida
if not token:
    raise ValueError("¡ERROR: No se encontró el TOKEN de InfluxDB!")

client = InfluxDBClient(url=url, token=token, org=org,timeout=30_000)
#synchronous es solo para el cliente
write_api = client.write_api(write_options=SYNCHRONOUS)

# --- PROCESAMIENTO DEL DATASET ---¡
# (Usamos el df que ya limpiamos antes)
# 2. ACTUALIZAR TIEMPO AL PRESENTE (2026)
# se recrea el rango antiguo para actualizar al presente
ahora = datetime.now(timezone.utc)
total_filas = len(df)
# Creamos un rango de fechas que termina EXACTAMENTE ahora
# Usamos periodos de 10 minutos (10T) hacia atrás
nuevas_fechas = pd.date_range(
    end=ahora, 
    periods=total_filas, 
    freq='15s', 
    tz=timezone.utc
)

df['Date/Time'] = nuevas_fechas
print(f"Nueva fecha inicial: {df['Date/Time'].min()}")
print(f"Nueva fecha final (ahora): {df['Date/Time'].max()}")


# Creamos una copia para no alterar el original y lo preparamos
df_envio = df.copy()
df_envio['turbine_id'] = "T1" # Añadimos el tag
df_envio.set_index('Date/Time', inplace=True)

# Definimos el tamaño del lote (batch)
batch_size = 500 
pausa = 10
intentos = 0 # Variable para controlar el número de intentos
exito = False # Variable para controlar el éxito del envío
total_enviado = 0 # Variable para contar el total de filas enviadas
enviar_datos = False # Cambiar a True para enviar datos

if enviar_datos:
    print(f"Total restantes: {df_envio.shape[0]}")

    try:
        # Dividimos el dataframe en trozos
        for i in range(0, len(df_envio), batch_size):
            batch = df_envio.iloc[i : i + batch_size]
            lote_n = i // batch_size + 1
            
            intentos = 0
            enviado = False
            
            # Bucle de reintento para el lote actual
            while not enviado and intentos < 3:
                try:
                    print(f"Enviando lote {lote_n}, tamaño del lote: {batch_size}...")
                    write_api.write(
                        bucket=bucket,
                        record=batch,
                        data_frame_measurement_name="mediciones_turbina",
                        data_frame_tag_columns=['turbine_id']
                    )
                    print(f"Lote {lote_n} completado.")
                    total_enviado += batch_size
                    print(f"Total filas enviadas hasta ahora: {total_enviado}")
                    print(f"Total restantes: {df_envio.shape[0] - total_enviado}\n\n")

                    enviado = True
                    time.sleep(pausa) # Pausa obligatoria tras éxito
                    
                except Exception as e:
                    intentos += 1
                    print(f"Error en lote {lote_n} (Intento {intentos}/3): {e}")
                    if intentos < 3:
                        print("Esperando 30 segundos antes de reintentar...")
                        time.sleep(30) # Pausa más larga tras un error
                    else:
                        print(f"Lote {lote_n} falló definitivamente tras 3 intentos.")

        print("Datos enviados con éxito")

    except Exception as e:
        print(f"Error en el envío: {e}")

# --- DEMOSTRACIÓN DE AGREGACIONES ---
print("\n--- Ejecutando agregaciones de prueba ---")
query_api = client.query_api()

flux_query = (
    f'from(bucket: "{bucket}") '
    f'|> range(start: -7d) '
    f'|> filter(fn: (r) => r["_measurement"] == "mediciones_turbina") '
    f'|> filter(fn: (r) => r["_field"] == "LV ActivePower (kW)") '
    f'|> aggregateWindow(every: 1h, fn: mean, createEmpty: false)'
)

try:
    result = query_api.query(org=org, query=flux_query)
    
    # Variable para verificar si hay datos
    hay_datos = False

    for table in result:
        for record in table.records:
            hay_datos = True
            # Formateamos la hora para que sea legible: YYYY-MM-DD HH:MM:SS
            hora_legible = record.get_time().strftime('%Y-%m-%d %H:%M:%S')
            valor = record.get_value()
            
            print(f"Hora: {hora_legible} | Potencia Media: {valor:>8.2f} kW")

    if not hay_datos:
        print("No se encontraron datos. Verifica que el nombre del campo sea 'ActivePower' y que el bucket tenga datos.")

except Exception as e:
    print(f"Error al consultar: {e}")

print("Cerrando conexión...")

# --- AGREGACIÓN 2: Máxima velocidad de viento por día ---
print("\n--- Máxima velocidad de viento por día (últimos 7 días) ---")

flux_max_viento = (
    f'from(bucket: "{bucket}") '
    f'|> range(start: -7d) '
    f'|> filter(fn: (r) => r["_measurement"] == "mediciones_turbina") '
    f'|> filter(fn: (r) => r["_field"] == "Wind Speed (m/s)") '
    f'|> aggregateWindow(every: 1d, fn: max, createEmpty: false)'
)

try:
    result2 = query_api.query(org=org, query=flux_max_viento)
    for table in result2:
        for record in table.records:
            dia = record.get_time().strftime('%Y-%m-%d')
            valor = record.get_value()
            print(f"Día: {dia} | Velocidad máx: {valor:.2f} m/s")
except Exception as e:
    print(f"Error en agregación 2: {e}")

client.close()
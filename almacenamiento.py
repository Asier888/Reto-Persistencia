import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timezone
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

client = InfluxDBClient(url=url, token=token, org=org)
#synchronous es solo para el cliente
write_api = client.write_api(write_options=SYNCHRONOUS)

# --- PROCESAMIENTO DEL DATASET ---
# (Usamos el df que ya limpiamos antes)
# 2. ACTUALIZAR TIEMPO AL PRESENTE (2026)
ahora = datetime.now(timezone.utc)
# Aseguramos que la columna tenga zona horaria para que Influx no de error
if df['Date/Time'].dt.tz is None:
    df['Date/Time'] = df['Date/Time'].dt.tz_localize(timezone.utc)

diff = ahora - df['Date/Time'].max()
df['Date/Time'] = df['Date/Time'] + diff

print(f"Preparando envío de {len(df)} registros...")

# Creamos una copia para no alterar el original y lo preparamos
df_envio = df.copy()
df_envio['turbine_id'] = "T1" # Añadimos el tag
df_envio.set_index('Date/Time', inplace=True)

try:
    # Este método es 100 veces más rápido que el bucle for
    write_api.write(
        bucket=bucket,
        record=df_envio,
        data_frame_measurement_name="mediciones_turbina",
        data_frame_tag_columns=['turbine_id']
    )
    print("✅ ¡Datos enviados con éxito! Ya puedes revisar tu InfluxDB.")
except Exception as e:
    print(f"❌ Error en el envío: {e}")
finally:
    client.close()
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

df = pd.read_csv('T1.csv')

print(df.describe())

# eliminar nulos
df.dropna(inplace=True)

print(df.describe())


# filtrar valores erroneos
# -> kW, velocidad del viento inferiores a 0
# -> valores demasiado alejados de la curva
df = df[df['LV ActivePower (kW)'] >= 0]
df = df[df['Wind Speed (m/s)'] >= 0]

print(df.describe())

# convertir la columna de tiempo a datetime porque sino se interpreta como string
fecha_col = 'Date/Time'
df[fecha_col] = pd.to_datetime(df[fecha_col], dayfirst=True, format='%d %m %Y %H:%M')
df.sort_values(by=fecha_col, inplace=True)

wind_dir_col = [c for c in df.columns if 'Wind Direction' in c][0]


#GRÁFICOS PRE LIMPIEZA
# gráfico 1: potencia real vs curva teórica en el tiempo
plt.figure(figsize=(14, 6))
plt.plot(df[fecha_col], df['LV ActivePower (kW)'], label='Potencia activa (kW)', color='tab:blue', alpha=0.8)
plt.plot(df[fecha_col], df['Theoretical_Power_Curve (KWh)'], label='Curva teórica (KWh)', color='tab:orange', alpha=0.8)
plt.title('Potencia activa vs Curva teórica')
plt.xlabel('Fecha y hora')
plt.ylabel('Potencia')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# gráfico 2: velocidad del viento vs potencia activa (dispersión)
plt.figure(figsize=(10, 6))
plt.scatter(df['Wind Speed (m/s)'], df['LV ActivePower (kW)'], s=10, alpha=0.5, c='tab:green')
plt.title('Potencia activa vs Velocidad del viento')
plt.xlabel('Velocidad del viento (m/s)')
plt.ylabel('Potencia activa (kW)')
plt.grid(True)
plt.tight_layout()
plt.show()

# gráfico 3: histograma de variables clave
plt.figure(figsize=(14, 10))
plt.subplot(3, 1, 1)
plt.hist(df['LV ActivePower (kW)'], bins=40, color='tab:blue', alpha=0.75)
plt.title('Distribución de la potencia activa')
plt.xlabel('Potencia activa (kW)')
plt.ylabel('Frecuencia')

plt.subplot(3, 1, 2)
plt.hist(df['Wind Speed (m/s)'], bins=40, color='tab:orange', alpha=0.75)
plt.title('Distribución de la velocidad del viento')
plt.xlabel('Velocidad del viento (m/s)')
plt.ylabel('Frecuencia')

plt.subplot(3, 1, 3)
plt.hist(df[wind_dir_col], bins=40, color='tab:green', alpha=0.75)
plt.title('Distribución de la dirección del viento')
plt.xlabel('Dirección del viento (°)')
plt.ylabel('Frecuencia')

plt.tight_layout()
plt.show()


# Eliminar registros con potencia cero cuando el viento es alto (> 3.5 m/s es el arranque común)
df = df[~((df['Wind Speed (m/s)'] > 3.5) & (df['LV ActivePower (kW)'] <= 0))]

# Quedarse solo con datos que estén cerca de la realidad teórica (ej. margen del 25%)
margin = 0.25
df = df[df['LV ActivePower (kW)'] >= df['Theoretical_Power_Curve (KWh)'] * (1 - margin)]

#GRÁFICOS POST LIMPIEZA
# gráfico 1: potencia real vs curva teórica en el tiempo
plt.figure(figsize=(14, 6))
plt.plot(df[fecha_col], df['LV ActivePower (kW)'], label='Potencia activa (kW)', color='tab:blue', alpha=0.8)
plt.plot(df[fecha_col], df['Theoretical_Power_Curve (KWh)'], label='Curva teórica (KWh)', color='tab:orange', alpha=0.8)
plt.title('Potencia activa vs Curva teórica')
plt.xlabel('Fecha y hora')
plt.ylabel('Potencia')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# gráfico 2: velocidad del viento vs potencia activa (dispersión)
plt.figure(figsize=(10, 6))
plt.scatter(df['Wind Speed (m/s)'], df['LV ActivePower (kW)'], s=10, alpha=0.5, c='tab:green')
plt.title('Potencia activa vs Velocidad del viento')
plt.xlabel('Velocidad del viento (m/s)')
plt.ylabel('Potencia activa (kW)')
plt.grid(True)
plt.tight_layout()
plt.show()

# gráfico 3: histograma de variables clave
plt.figure(figsize=(14, 10))
plt.subplot(3, 1, 1)
plt.hist(df['LV ActivePower (kW)'], bins=40, color='tab:blue', alpha=0.75)
plt.title('Distribución de la potencia activa')
plt.xlabel('Potencia activa (kW)')
plt.ylabel('Frecuencia')

plt.subplot(3, 1, 2)
plt.hist(df['Wind Speed (m/s)'], bins=40, color='tab:orange', alpha=0.75)
plt.title('Distribución de la velocidad del viento')
plt.xlabel('Velocidad del viento (m/s)')
plt.ylabel('Frecuencia')

plt.subplot(3, 1, 3)
plt.hist(df[wind_dir_col], bins=40, color='tab:green', alpha=0.75)
plt.title('Distribución de la dirección del viento')
plt.xlabel('Dirección del viento (°)')
plt.ylabel('Frecuencia')

plt.tight_layout()
plt.show()



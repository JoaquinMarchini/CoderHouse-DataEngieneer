import requests
import pandas as pd
import psycopg2
from datetime import datetime

# Variables de configuración
API_KEY = '1b6b6c2490902ad25d44d4f4eab18fc6'
BASE_URL = 'http://data.fixer.io/api/latest'
REDSHIFT_CREDENTIALS = {
    'dbname': 'data-engineer-database',
    'user': 'joaquinmarchini0214_coderhouse',
    'password': 'KPv7qEQ6M2',
    'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    'port': '5439'
}

# 1. Extraer datos de la API
response = requests.get(BASE_URL, params={'access_key': API_KEY})
data = response.json()

# Verificar que la solicitud fue exitosa
if data.get('success'):
    # Convertir los datos en un DataFrame de pandas para su manipulación
    rates = data['rates']
    df = pd.DataFrame(rates.items(), columns=['currency', 'rate'])
    df['date'] = datetime.now()
else:
    raise Exception(f"Error en la solicitud a la API: {data.get('error')}")

# 2. Insertar datos en la tabla de Redshift
conn = psycopg2.connect(**REDSHIFT_CREDENTIALS)
cur = conn.cursor()

# Consulta para insertar datos
insert_query = '''
INSERT INTO FixedCoderHouse1 (currency, rate, date)
VALUES (%s, %s, %s)
'''

# Insertar cada fila en la base de datos
for index, row in df.iterrows():
    cur.execute(insert_query, (row['currency'], row['rate'], row['date']))

# Confirmar los cambios
conn.commit()

# Cerrar la conexión
cur.close()
conn.close()

print("Datos cargados exitosamente en Redshift.")

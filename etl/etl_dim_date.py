import boto3
import pandas as pd
import holidays
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa
import sys

# --- CONFIGURACIÓN ---
s3 = boto3.client('s3')

bucket_output = 'cmjm-dl'
prefix_output = 'dim_date/'

def verify_bucket_exists(bucket_name):
    """Verifica si el bucket existe, lanza error si no."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket verificado: {bucket_name}")
    except s3.exceptions.ClientError as e:
        print(f"❌ Error: El bucket '{bucket_name}' no existe o no es accesible.")
        raise RuntimeError(f"Bucket inexistente o sin permisos: {bucket_name}") from e

def create_prefix_if_needed(bucket, prefix):
    """
    Crea un marcador vacío para el prefijo (solo informativo).
    S3 no requiere carpetas reales, pero esto ayuda a evitar errores en ciertos clientes.
    """
    try:
        s3.put_object(Bucket=bucket, Key=(prefix.rstrip('/') + '/'))
        print(f"📁 Prefijo verificado/creado: s3://{bucket}/{prefix}")
    except Exception as e:
        raise RuntimeError(f"Error al verificar/crear el prefijo {prefix}: {e}")

def main():
    # --- VERIFICACIÓN DE BUCKET ---
    verify_bucket_exists(bucket_output)
    create_prefix_if_needed(bucket_output, prefix_output)

    # --- GENERAR RANGO DE FECHAS ---
    start_date = '2000-01-01'
    end_date = '2030-12-31'
    print(f"📅 Generando calendario desde {start_date} hasta {end_date}")

    df = pd.DataFrame({"rental_date": pd.date_range(start=start_date, end=end_date)})

    # --- GENERAR COLUMNAS DE DIMENSIÓN ---
    df['date_id'] = df['rental_date'].dt.strftime('%Y%m%d').astype(int)
    df['day'] = df['rental_date'].dt.day
    df['month'] = df['rental_date'].dt.month
    df['year'] = df['rental_date'].dt.year
    df['day_of_week'] = df['rental_date'].dt.day_name()
    df['week_of_year'] = df['rental_date'].dt.isocalendar().week.astype(int)
    df['quarter'] = df['rental_date'].dt.quarter
    df['is_weekend'] = df['rental_date'].dt.dayofweek.isin([5, 6])

    # --- FERIADOS EN EE.UU. ---
    us_holidays = holidays.US()
    df['is_holiday'] = df['rental_date'].isin(us_holidays)
    df['holiday_name'] = df['rental_date'].apply(lambda x: us_holidays.get(x) if x in us_holidays else None)

    # --- GUARDAR RESULTADO COMO PARQUET SNAPPY ---
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer_out = BytesIO()
    pq.write_table(table, buffer_out, compression='snappy')

    output_key = f"{prefix_output}dim_date.snappy.parquet"
    print(f"📤 Subiendo archivo a s3://{bucket_output}/{output_key}")

    try:
        s3.put_object(Bucket=bucket_output, Key=output_key, Body=buffer_out.getvalue())
        print(f"✅ ETL completado correctamente.")
        print(f"🗂️ Archivo generado: s3://{bucket_output}/{output_key}")
    except Exception as e:
        print(f"❌ Error al subir archivo a S3: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

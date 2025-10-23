# tests/test_etl_dim_date.py
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
from etl.etl_dim_date import main  # Importa tu script ETL sin modificaciones

@mock_aws
def test_etl_dim_date_creates_output():
    """
    Prueba unitaria para el ETL dim_date:
    - Simula S3 con moto.
    - Ejecuta el script ETL sin modificaciones.
    - Valida las columnas que el script actual genera (incluyendo holiday_name).
    """
    # 1. Configurar entorno simulado de S3
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "cmjm-dl"
    s3.create_bucket(Bucket=bucket_name)

    # 2. Ejecutar el ETL (sin cambios)
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    main()

    # 3. Verificar que el archivo se subió a S3
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="dim_date/")
    assert "Contents" in response, "No se encontró el archivo en S3"

    # Filtrar solo archivos Parquet (ignorar el objeto del prefijo)
    parquet_files = [obj for obj in response["Contents"] if not obj["Key"].endswith("/")]
    assert len(parquet_files) == 1, "Se esperaba un solo archivo Parquet"

    # 4. Descargar y validar el archivo Parquet
    key = parquet_files[0]["Key"]
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df_out = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

    # 5. Validar columnas generadas (incluyendo holiday_name)
    expected_columns = {
        "rental_date",
        "date_id",
        "day",
        "month",
        "year",
        "day_of_week",
        "week_of_year",
        "quarter",
        "is_weekend",
        "is_holiday",
        "holiday_name"
    }
    assert set(df_out.columns) == expected_columns, f"Columnas inesperadas: {set(df_out.columns) - expected_columns}"

    # 6. Validar rango de fechas
    min_date = df_out["rental_date"].min()
    max_date = df_out["rental_date"].max()
    assert min_date == pd.to_datetime("2000-01-01"), f"Fecha mínima incorrecta: {min_date}"
    assert max_date == pd.to_datetime("2030-12-31"), f"Fecha máxima incorrecta: {max_date}"

    # 7. Validar tamaño del DataFrame (11,323 registros)
    assert len(df_out) == 11323, f"Se esperaban 11,323 registros, pero se obtuvieron {len(df_out)}"

    # 8. Validar que is_holiday sea booleano
    assert df_out["is_holiday"].dtype == bool, "La columna is_holiday debe ser booleana"

    # 9. Validar fin de semana (usando == en lugar de is)
    saturday = df_out[df_out["rental_date"] == pd.to_datetime("2025-01-04")].iloc[0]
    assert saturday["is_weekend"] == True, "El 4 de enero de 2025 es sábado (debería ser fin de semana)"

    print("✅ ETL dim_date verificado exitosamente.")

if __name__ == "__main__":
    test_etl_dim_date_creates_output()

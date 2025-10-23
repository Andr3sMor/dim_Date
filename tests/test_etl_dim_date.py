# tests/test_etl_dim_date.py
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
from etl.etl_dim_date import main

@mock_aws
def test_etl_dim_date_creates_output():
    # Configurar credenciales falsas para evitar errores de autenticación
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    # Crear el bucket simulado antes de ejecutar el ETL
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="cmjm-dl")

    # Ejecutar el ETL
    main()

    # Verificar que el archivo se subió al bucket simulado
    response = s3.list_objects_v2(Bucket="cmjm-dl", Prefix="dim_date/")
    assert "Contents" in response, "No se encontró el archivo en S3"

    # Filtrar solo los archivos Parquet (ignorar el objeto del prefijo)
    parquet_files = [obj for obj in response["Contents"] if not obj["Key"].endswith("/")]
    assert len(parquet_files) == 1, "Se esperaba un solo archivo Parquet"

    # Descargar y validar el contenido del archivo Parquet
    key = parquet_files[0]["Key"]
    obj = s3.get_object(Bucket="cmjm-dl", Key=key)
    df_out = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

    # Validar las columnas generadas
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

    # Validar el rango de fechas
    min_date = df_out["rental_date"].min()
    max_date = df_out["rental_date"].max()
    assert min_date == pd.to_datetime("2000-01-01"), f"Fecha mínima incorrecta: {min_date}"
    assert max_date == pd.to_datetime("2030-12-31"), f"Fecha máxima incorrecta: {max_date}"

    # Validar el tamaño del DataFrame
    assert len(df_out) == 11323, f"Se esperaban 11,323 registros, pero se obtuvieron {len(df_out)}"

    # Validar que `is_holiday` sea booleano
    assert pd.api.types.is_bool_dtype(df_out["is_weekend"]), "La columna is_weekend debe ser booleana"
    assert pd.api.types.is_bool_dtype(df_out["is_holiday"]), "La columna is_holiday debe ser booleana"

    print("✅ ETL dim_date verificado exitosamente.")

if __name__ == "__main__":
    test_etl_dim_date_creates_output()

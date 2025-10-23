import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
import os

# Importamos la función principal desde glue_scripts
from glue_scripts.ETL_dim_date import main

@mock_aws
def test_etl_dim_date_creates_output():
    """
    Verifica que el ETL genera un archivo Parquet con las columnas esperadas
    y lo sube al bucket S3 correcto.
    """
    s3 = boto3.client("s3", region_name="us-east-1")

    # Crear bucket simulado
    bucket_name = "cmjm-dl"
    s3.create_bucket(Bucket=bucket_name)

    # Crear un Parquet de entrada simulado
    df_input = pd.DataFrame({
        "rental_date": pd.date_range("2025-01-01", periods=3, freq="D"),
        "customer_id": [1, 2, 3]
    })

    # Guardar en memoria como Parquet
    buffer = BytesIO()
    table = pa.Table.from_pandas(df_input)
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Subir a S3 simulado
    s3.put_object(Bucket=bucket_name, Key="fact_rental/test.parquet", Body=buffer.getvalue())

    # Ejecutar ETL
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    main()

    # Verificar salida
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="dim_date/")
    keys = [obj["Key"] for obj in response.get("Contents", [])]

    assert any(k.endswith(".parquet") for k in keys), "No se generó el archivo Parquet de salida"

    # Leer y validar el contenido del archivo generado
    key = next(k for k in keys if k.endswith(".parquet"))
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df_out = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

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
        "is_holiday"
    }

    assert expected_columns.issubset(df_out.columns), "Faltan columnas en la tabla dim_date"
    assert len(df_out) == 3, "El número de filas no coincide con las fechas únicas"


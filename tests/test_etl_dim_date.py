import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from moto import mock_aws
from etl.etl_dim_date import main  # Asegúrate de que esta función acepte un cliente de S3

@mock_aws
def test_etl_dim_date_creates_output():
    """
    Prueba unitaria del ETL dim_date:
    - Simula un entorno S3 usando moto.
    - Crea un archivo Parquet de entrada (fact_rental).
    - Ejecuta el proceso ETL real.
    - Verifica que el resultado (dim_date) se suba correctamente a S3.
    """
    # 1. Configuración del entorno simulado
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket_name = "cmjm-dl"
    s3.create_bucket(Bucket=bucket_name)

    # 2. Crear un pequeño Parquet de entrada simulado
    df_input = pd.DataFrame({
        "rental_date": pd.date_range("2025-01-01", periods=3, freq="D"),
        "customer_id": [1, 2, 3]
    })

    buffer = BytesIO()
    pq.write_table(pa.Table.from_pandas(df_input), buffer)
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket_name,
        Key="fact_rental/test.parquet",
        Body=buffer.getvalue()
    )

    # 3. Ejecutar el ETL real, inyectando el cliente de S3 "mockeado"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    main(s3_client=s3)  # Pasar el cliente de S3 como argumento

    # 4. Verificar que el ETL generó la salida esperada en el prefijo dim_date/
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix="dim_date/")
    assert "Contents" in response, "No se encontraron archivos en el prefijo dim_date/"

    keys = [obj["Key"] for obj in response["Contents"]]
    parquet_files = [k for k in keys if k.endswith(".parquet")]
    assert parquet_files, "No se generó ningún archivo Parquet en dim_date/"

    # 5. Leer y validar el contenido del archivo generado
    key = parquet_files[0]
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    df_out = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

    # 6. Validar estructura de la tabla
    expected_columns = {
        "date",
        "date_id",
        "day",
        "month",
        "year",
        "quarter",
        "weekday",
        "weekday_name",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "day_of_year",
        "iso_week"
    }
    assert expected_columns.issubset(df_out.columns), \
        f"Faltan columnas en la salida: {expected_columns - set(df_out.columns)}"

    # 7. Validar rango de fechas
    min_date, max_date = df_out["date"].min(), df_out["date"].max()
    print(f"Rango de fechas generado: {min_date} → {max_date}")

    # 8. Validar tamaño general (más de 10k registros es razonable)
    assert len(df_out) > 10000, f"Se esperaban muchas fechas, pero solo se generaron {len(df_out)}"
    print("ETL dim_date verificado exitosamente.")

if __name__ == "__main__":
    test_etl_dim_date_creates_output()

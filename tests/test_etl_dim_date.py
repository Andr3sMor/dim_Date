# tests/test_etl_dim_date.py
import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO, BytesIO
from moto import mock_aws
import sys
from unittest.mock import patch
from etl.etl_dim_date import main, s3  # Importa el cliente s3 global

@mock_aws
def test_etl_dim_date_creates_output():
    """
    Prueba unitaria para el ETL dim_date:
    - Simula S3 con moto.
    - Configura credenciales falsas para evitar errores de autenticación.
    - Usa patch para reemplazar el cliente s3 global.
    - Valida el archivo Parquet generado y su contenido.
    """
    # Configurar credenciales falsas para boto3
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    # Crear el cliente s3 simulado
    s3_mock = boto3.client("s3", region_name="us-east-1")
    s3_mock.create_bucket(Bucket="cmjm-dl")

    # Reemplazar el cliente s3 con el cliente simulado
    with patch('etl.etl_dim_date.s3', s3_mock):
        # Redirigir stdout para evitar mensajes de consola 
        original_stdout = sys.stdout
        sys.stdout = StringIO()

        try:
            # Ejecutar el ETL
            main()
        finally:
            # Restaurar stdout
            sys.stdout = original_stdout

        # Verificar que el archivo se subió al bucket simulado
        response = s3_mock.list_objects_v2(Bucket="cmjm-dl", Prefix="dim_date/")
        assert "Contents" in response, "No se encontró el archivo en S3"

        # Filtrar solo los archivos Parquet (ignorar el objeto del prefijo)
        parquet_files = [obj for obj in response["Contents"] if not obj["Key"].endswith("/")]
        assert len(parquet_files) == 1, "Se esperaba un solo archivo Parquet"
        assert parquet_files[0]["Key"] == "dim_date/dim_date.snappy.parquet", "Nombre de archivo incorrecto"

        # Descargar y validar el contenido del archivo Parquet
        key = parquet_files[0]["Key"]
        obj = s3_mock.get_object(Bucket="cmjm-dl", Key=key)
        df_out = pq.read_table(BytesIO(obj["Body"].read())).to_pandas()

        # Validar las columnas generadas
        expected_columns = {
            "rental_date", "date_id", "day", "month", "year",
            "day_of_week", "week_of_year", "quarter", "is_weekend", "is_holiday", "holiday_name"
        }
        assert set(df_out.columns) == expected_columns, f"Columnas inesperadas: {set(df_out.columns) - expected_columns}"

        # Validar el rango de fechas
        assert df_out["rental_date"].min() == pd.to_datetime("2000-01-01"), "Fecha mínima incorrecta"
        assert df_out["rental_date"].max() == pd.to_datetime("2030-12-31"), "Fecha máxima incorrecta"

        # Validar el tamaño del DataFrame
        assert len(df_out) == 11323, f"Se esperaban 11,323 registros, pero se obtuvieron {len(df_out)}"

        # Validar tipos de datos
        assert pd.api.types.is_integer_dtype(df_out["date_id"]), "date_id debe ser entero"
        assert pd.api.types.is_bool_dtype(df_out["is_weekend"]), "is_weekend debe ser booleano"
        assert pd.api.types.is_bool_dtype(df_out["is_holiday"]), "is_holiday debe ser booleano"

        # Validar algunos valores específicos
        assert df_out.loc[0, "date_id"] == 20000101, "Primer date_id incorrecto"
        assert df_out.loc[0, "day_of_week"] == "Saturday", "Primer día de la semana incorrecto"
        assert df_out.loc[0, "is_weekend"] == True, "Primer día debería ser fin de semana"

        # Validar que no haya valores nulos en el test
        assert not df_out["date_id"].isna().any(), "date_id no debe tener valores nulos"
        assert not df_out["rental_date"].isna().any(), "rental_date no debe tener valores nulos"

        print("ETL dim_date verificado exitosamente.")

if __name__ == "__main__":
    test_etl_dim_date_creates_output()


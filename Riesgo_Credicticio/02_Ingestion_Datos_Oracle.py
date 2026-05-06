import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import math


# Configuramos la variable de entorno para que Spark encuentre winutils
os.environ['HADOOP_HOME'] = r'C:\hadoop' # O la ruta donde lo hayas instalado
sys.path.append(r'C:\hadoop\bin')


# 1. IMPORTANTE: Definir la conexión (esto te faltaba en el último bloque)
conexion = SparkSession.builder \
    .appName("02_Ingestion_Datos_Oracle") \
    .config("spark.jars", r"C:\PySpark\Instaladores\jars\ojdbc11.jar") \
    .getOrCreate()

#2 . Credenciales db
config_db = {
    "user": "BANCO_PRUEBA",
    "password": "spark",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

#3 #usamos la url para la conexion banco prueba
url_oracle = "jdbc:oracle:thin:@//localhost:1521/XE"

#3 Leo los archivos parquet que se encuentran en la carpeta clientes y creditos
df_final_clientes = conexion.read.parquet(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\Clientes")
df_final_creditos = conexion.read.parquet(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\Creditos")


# 4. Carga masiva a las tablas de Oracle
try:
    print("Iniciando carga en Oracle...")
    df_final_clientes.write.jdbc(
        url=url_oracle, 
        table="CLIENTES", 
        mode="overwrite", 
        properties=config_db
    )
    
    # Cargamos Créditos
    df_final_creditos.write.jdbc(
        url=url_oracle, 
        table="HISTORIAL_PAGOS", 
        mode="overwrite", 
        properties=config_db
    )
    
    print("¡Carga finalizada con éxito en BANCO_PRUEBA!")

except Exception as e:
    print(f"Error durante la carga a Oracle: {e}")

finally:
    # Cerramos la sesión para liberar recursos (y evitar errores de limpieza de temporales)
    conexion.stop()
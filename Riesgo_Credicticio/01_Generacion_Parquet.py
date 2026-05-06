import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import math

# Configurar HADOOP_HOME manualmente antes de iniciar la SparkSession
os.environ['HADOOP_HOME'] = r"C:\hadoop"
sys.path.append(r"C:\hadoop\bin")


# 1. Configuración de la Sesión
# Corregimos: .config() lleva dos argumentos y el método es .getOrCreate()
path_jar = r"C:\PySpark\Instaladores\jars\ojdbc11.jar"

# 2. Conexion a DBA Oracle (JDBC)
conexion = SparkSession.builder \
    .appName("Carga_Datos_RiesgoCrediticio_Lauro") \
    .config("spark.jars", path_jar) \
    .getOrCreate()


#Esquemas (Corregido: Agregamos comas faltantes y corregimos nombres de tipos)
schema_clientes = StructType ([
    StructField("ID_CLIENTES", IntegerType(), False), # Clave Primaria
    StructField("NOMBRE_APELLIDO", StringType(), True), 
    StructField("EDAD", IntegerType(), True), 
    StructField("INGRESOS_MENSUALES", DoubleType(), True), # Era DoubleType, no DoubleTypeType
    StructField("CIUDAD", StringType(), True)
])

schema_creditos = StructType([
    StructField ("ID_CREDITO", IntegerType(), False),
    StructField ("ID_CLIENTES", IntegerType(), False),
    StructField ("MONTO_SOLICITADO", DoubleType(), True),
    StructField ("SCORE_VERAZ", IntegerType(), True),
    StructField ("ESTADO_PRESTAMO", StringType(), True)
])


# Lectura con el esquema aplicado
df_clientes_raw  = conexion.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .load("maestro_clientes_simulados.csv")
df_creditos_raw = conexion.read \
    .schema(schema_creditos) \
    .option("multiLine", "true") \
    .json(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\creditos_simulados.json")

#Se que mis archivos tiene 1000 registros voy a particionarlo para crear unos parquet

# 4. Lógica de Particionamiento (Fórmula aplicada)
#quiero que mis archivos tengan 500 registros o lineas
registros_por_archivo = 500
#calculo la cantidad de lineas que tiene cada archivo
total_lineas_cliente = df_clientes_raw.count()
total_lineas_creditos = df_creditos_raw.count()


#max(1, ...): Imaginate que por algún error el archivo viene vacío (0 registros). La división daría 0. Si le pedís a Spark repartition(0), el script explota y tira error.
#Al usar max(1, resultado), te asegurás de que como mínimo siempre se cree 1 archivo, aunque el resultado de la cuenta sea cero o negativo
#en mi caso voy a generar 2 archivos 
num_particiones_cliente = max(1,int (total_lineas_cliente / registros_por_archivo)) 
num_particiones_credito = max(1,int (total_lineas_creditos / registros_por_archivo)) 



# 5. Guardado en Parquet 
# Guardamos Clientes
df_clientes_raw.repartition(num_particiones_cliente) \
    .write.mode("overwrite") \
    .parquet(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\Clientes")


#3 Re-particiono y lo guardo en un parquet
df_creditos_raw.repartition(num_particiones_credito) \
    .write.mode("overwrite") \
    .parquet(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\Creditos")

print(f"Éxito: Se generaron {num_particiones_cliente} archivos para Clientes y {num_particiones_credito} para Créditos.")
    



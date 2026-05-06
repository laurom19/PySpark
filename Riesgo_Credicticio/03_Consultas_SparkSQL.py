from pyspark.sql import SparkSession

# 1. Iniciamos la sesión
spark = SparkSession.builder \
    .appName("03_Consultas_SparkSQL_Lauro") \
    .getOrCreate()



# --- LIMPIEZA DEL CATÁLOGO ---
spark.catalog.dropTempView("V_CLIENTES")
spark.catalog.dropTempView("V_PAGOS")

# --- CARGA DE CLIENTES (CSV) ---
df_clientes = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(r"C:\PySpark\Ejercicios\Riesgo_Credicticio\maestro_clientes_simulados.csv")

df_clientes.createOrReplaceTempView("CLIENTES")

# --- CARGA DE CRÉDITOS/PAGOS (JSON) ---
# Importante: 'multiLine' permite leer el JSON que generamos antes
df_pagos = spark.read.option("multiLine", "true") \
    .json("creditos_simulados.json")

df_pagos.createOrReplaceTempView("HISTORIAL_PAGOS")


# --- VERIFICACIÓN ---
print("ESTRUCTURA DE CLIENTES:")
df_clientes.printSchema() 
df_clientes.show(5)

print("ESTRUCTURA DE PAGOS:")
df_pagos.printSchema()
df_pagos.show(5)


# Consulta para ver los datos integrados
consulta_final = """
SELECT 
    c.ID_CLIENTES, 
    c.APELLIDO_NOMBRE, 
    c.SUCURSAL, 
    p.MONTO_SOLICITADO, 
    p.ESTADO_PRESTAMO
FROM CLIENTES c
INNER JOIN HISTORIAL_PAGOS p ON c.ID_CLIENTES = p.ID_CLIENTES
WHERE p.ESTADO_PRESTAMO = 'APROBADO'
"""

spark.sql(consulta_final).show(10)
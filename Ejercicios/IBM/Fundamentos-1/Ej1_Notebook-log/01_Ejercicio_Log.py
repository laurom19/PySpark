import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Forzar variables de entorno antes de crear la sesión
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYTHONFAULTHANDLER'] = '1'


# Inicialización con indentación corregida
spark = SparkSession.builder \
    .appName("IBM_Stability_Fix") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.master", "local[1]") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("--- Sesión de Spark iniciada correctamente ---")

log_path = r"C:\PySpark\Ejercicios\IBM\Fundamentos-1\LabData\Taxis\notebook.log"

try:
    # Usamos SparkSQL que suele ser más robusto para el inicio
    df = spark.read.text(log_path)
    print(f"Éxito. Líneas contadas: {df.count()}")
    df.show(5)
except Exception as e:
    print(f"Error al procesar: {e}")

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("--- Sesión de Spark creada con configuración de estabilidad ---")

# 3. Prueba de carga simplificada
log_path = r"C:\PySpark\Ejercicios\IBM\Fundamentos-1\LabData\Taxis\notebook.log"

try:
    # Usamos SparkSQL para leer, que a veces es más estable que RDD en Windows
    df = spark.read.text(log_path)
    
    # Intentamos una acción que no mueva muchos datos
    print(f"Conteo de filas: {df.count()}")
    
    # Si llega acá, el problema está resuelto
    df.show(5, truncate=False)

except Exception as e:
    print(f"\n[ERROR]: El worker sigue fallando. Detalle: {e}")


#Filtro errores de contiene la palabra "WARNING", "ERROR", "INFO"
contador_warning = df.filter(df.value.contains("WARNING"))
contador_error = df.filter(df.value.contains("ERROR"))
contador_info = df.filter (df.value.contains("INFO"))

#Cuento con la funcion count() todo lo que filtro
#Muestro los resultados
print("\n" + "="*50) # Un separador más fuerte para el inicio
print(f"RESUMEN DE LOGS:")
print(f"  > WARNING: {contador_warning.count()}")
print(f"  > ERROR:   {contador_error.count()}")
print(f"  > INFO:    {contador_info.count()}")
print("-" * 60) # Este es tu separador de 30 guiones dobles

#Si quiero ver solo los mensajes de warning:
print ("\n Primeros 5 Warning encontrados:")
contador_warning.select("value").show(5, truncate=False)

#Obtener la cantidad palabras spark : Transformacion
# No ejecuta nada aún, solo define qué filas queremos.
contador_spark = df.filter(df.value.contains("spark"))

print(f"  > SPARK:    {contador_spark.count()}")
print (f"{'--'*30}")
#Accion:collect()
# Aquí es donde Spark realmente lee el archivo y procesa el filtro.
# Recolectamos y guardamos en 'lista_filas'
lista_filas = contador_spark.collect()
print (f"Las lineas que hemos encontrado son las siguientes:")
# Ahora imprimimos el contenido de esa variable
for fila in lista_filas[:3]: # Imprimimos solo las primeras 3 para no llenar la pantalla
    print(f"Línea recuperada: {fila.value}")
print (f"{'--'*30}")


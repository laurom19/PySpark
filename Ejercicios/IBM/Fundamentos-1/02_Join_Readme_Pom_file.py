import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Limpieza de variables de entorno (Crucial para Windows)
os.environ['HADOOP_HOME'] = r'C:\hadoop'
sys.path.append(r'C:\hadoop\bin')
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# 2. Inicialización con parámetros de estabilidad de red y memoria
join = SparkSession.builder \
    .appName("Join_Readme_Pom") \
    .config("spark.master", "local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .getOrCreate()

sc = join.sparkContext
sc.setLogLevel("ERROR")

print("--- Sesión de Spark iniciada (Modo Estabilidad) ---")

try:
    print("--- Iniciando lectura con DataFrames ---")
    readme_path = r"C:\PySpark\Ejercicios\IBM\Fundamentos-1\LabData\Taxis\README.md"
    pom_path = r"C:\PySpark\Ejercicios\IBM\Fundamentos-1\LabData\Taxis\pom.xml"

    # 1. Leer el archivo como un DataFrame de una sola columna ("value")
    df_readme = join.read.text(readme_path)
    df_pom = join.read.text(pom_path)

    # 2. Transformaciones encadenadas:
    # a. split: Separamos las líneas en palabras (crea un array)
    # b. explode: Convertimos cada elemento del array en una fila nueva
    # c. regexp_replace: Limpiamos puntuación básica
    # d. length: Filtramos por longitud par
    df_palabras_readme = df_readme.select(
        F.explode(F.split(F.col("value"), r"\s+")).alias("palabra")
    ).select(
        F.regexp_replace(F.col("palabra"), r"[^\w]", "").alias("palabra_limpia")
    ).filter(
        (F.length(F.col("palabra_limpia")) > 0) & 
        (F.length(F.col("palabra_limpia")) % 2 == 0)
    )

    df_palabras_pom= df_pom.select(
        F.explode(F.split(F.col("value"), r"\s+")).alias("palabra")
    ).select(
        F.regexp_replace(F.col("palabra"), r"[^\w]", "").alias("palabra_limpia")
    ).filter(
        (F.length(F.col("palabra_limpia")) > 0) & 
        (F.length(F.col("palabra_limpia")) % 2 == 0)
    )

    # 3. Conteo (Acción de agregación)
    # Equivale al (K, V) que buscábamos antes
    df_conteo_readme = df_palabras_readme.groupBy("palabra_limpia") \
        .count() \
        .orderBy(F.desc("count"))
    print (f"{'--'*30}")
    print("------------- Archivo README -------------:")
    print("Resultados finales (Palabra | Conteo):")
    df_conteo_readme.show(20, truncate=False)

    df_conteo_pom = df_palabras_pom.groupBy("palabra_limpia") \
        .count() \
        .orderBy(F.desc("count"))
    print (f"{'--'*30}")
    print("------------- Archivo POM -------------:")
    print("Resultados finales (Palabra | Conteo):")
    df_conteo_pom.show(20, truncate=False)

    #JOIN FINAL
# 1. Renombramos las columnas de conteo para no confundirlas después del join
    df_readme_final = df_conteo_readme.withColumnRenamed("count", "count_readme")
    df_pom_final = df_conteo_pom.withColumnRenamed("count", "count_pom")


# 2. Realizamos el INNER JOIN por la columna 'palabra_limpia'
# Solo quedarán las palabras que existan en los dos archivos
    df_unido = df_readme_final.join(
    df_pom_final,
    on="palabra_limpia",
    how="inner"
    )

# 3. Calculamos un conteo total sumando ambos
#Le digo que las 2 columnas me las unifique a total_general 
    df_resultado = df_unido.withColumn(
    "total_general",
    F.col("count_readme")+F.col("count_pom") 
    )

#4. Ordeno el total de mayor a menor
    df_resultado = df_resultado.orderBy(F.desc("total_general"))


# 4. Mostrar el resultado del cruce de datos
    print("\n" + "="*70)
    print("RESULTADO DEL JOIN: PALABRAS PRESENTES EN README Y POM")
    print("="*70)
    df_resultado.show(20, truncate=False)


except Exception as e:
    print(f"Error con DataFrames: {e}")

finally:
# Solo al final de todo el trabajo cerramos la sesión
    print("--- Cerrando sesión de Spark ---")
    join.stop()



""""
#En el contexto de tu código de Spark, r"\s+" se desglosa así:
r (Raw string): Le indica a Python que trate el texto "tal cual" es, sin interpretar las barras invertidas (\) como caracteres especiales de escape (muy común en rutas de Windows o RegEx).
\s: Es el metacarácter para "espacio en blanco". 
Esto incluye:
Espacios normales ( ).Tabulaciones (\t).Saltos de línea (\n).Retornos de carro (\r).+: Es un cuantificador que significa "uno o más".
¿Qué hace en el código específicamente?
Al usar F.split(F.col("value"), r"\s+"), le ordene a Spark:
'Dividí el texto cada vez que encuentres un espacio, o varios espacios juntos, o una tabulación."
Con split(" "): Te devolvería ["Spark", "", "", "es", "", "", "", "", "genial"]. (Tendrías que limpiar los vacíos después).

Con split(r"\s+"): Te devuelve ["Spark", "es", "genial"]. (Directo al grano).
Es una práctica estándar en el preprocesamiento de texto para NLP (Procesamiento de Lenguaje Natural) y análisis de logs, como el que estuviste haciendo hoy.
"""



""""
[] (Corchetes): Definen un conjunto o clase de caracteres. Todo lo que esté adentro es lo que Spark va a buscar.
^ (Símbolo de intercalación): Cuando está adentro de los corchetes al principio, significa "NEGACIÓN". Es decir: "todo lo que NO sea lo siguiente".
\w (Word character): Representa cualquier "carácter de palabra".
Esto incluye:
Letras (A-Z, a-z).Números (0-9).Guiones bajos (_).
La expresión completa [^\w] le dice a Spark:
"Buscá cualquier cosa que NO sea una letra, un número o un guion bajo."
"""

import os
import sys
import pandas as pd
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# 1. Configuración de Entorno (Crucial para Windows)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

def analizar_automoviles():
    print("🚀 Iniciando motor Spark para Análisis de Automóviles...")
    
    # Iniciamos la sesión con los parches de estabilidad
    automoviles = SparkSession.builder \
        .appName("Automoviles") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    automoviles.sparkContext.setLogLevel("ERROR")

    try:

        path_csv = "C:/PySpark/Ejercicios/IBM/Fundamentos-1/LabData/Taxis/automoviles.csv"
        print("Spark cargando archivo...")
        
        # Agregamos header e inferSchema para que reconozca los números
        df_automoviles = automoviles.read.csv(path_csv, header=True, inferSchema=True)

        print("\n--- Vista Previa Original ---")
        df_automoviles.show(5)
        
        # FILTRADO (Sintaxis Correcta de Spark)
        # Usamos .filter() y .select()
        df_filtrado = df_automoviles.filter(df_automoviles['mpg'] > 20)

        #AGRUPAMIENTO o GROUPBY : (Para ver estadísticas)
        #Uso de GroupBy: calcular el peso promedio de los automóviles según sus cilindros ('cly') 
        #.agg (agregacion) cuenta lo que hay en el wt (peso libras) y le coloco un alias ("cantidad_autos")
        #.sort (clasificacion) le digo que lo que conto me lo ordene de forma ascendente
        df_resumen = df_automoviles.groupBy(['cyl'])\
        .agg(F.count("wt").alias("cantidad_autos")) \
        .sort(F.desc("cantidad_autos")) # <--- Ahora usás el alias
        
        # Esto muestra el conteo, pero no afecta al resto del código al asignarlo como df_resumen
        df_resumen.show(5)

        #CONVERSION 
        # Uso withColumn: Paso de libras a toneladas renombrando la columna. Wt: Libras. Al multiplicarlo por 0.45 lo paso a Toneladas
        df_filtrado = df_automoviles.withColumn('Peso_toneladas', df_filtrado['wt']*0.45)
        df_filtrado.show(5)

        # SELECCIÓN DE COLUMNAS QUE QUIERO MOSTRAR AL FINAL
        df_final_spark = df_filtrado.select('model', 'mpg', 'hp', 'Peso_Toneladas')

        # CONVERSIÓN A PANDAS (Para guardar el CSV sin dramas en Windows)
        # Usamos toPandas() para bajar los datos a la memoria local
        pdf_final = df_final_spark.toPandas()

        # GUARDAR (Sintaxis de Pandas)
        path_temp = r"C:\PySpark\Ejercicios\IBM\Fundamentos-1\LabData\Taxis\automoviles_filtrados.csv"
        pdf_final.to_csv(path_temp, index=False)
        print(f" Filtrado completado. Registros guardados: {len(pdf_final)}")

        # LECTURA FINAL (Usando el objeto 'automoviles' que es tu sesión)
        print("Spark loading filtered file...")
        df_temporal = automoviles.read.csv(path_temp, header=True, inferSchema=True)
        df_temporal.show()

    except Exception as e:
        print(f"\n Error detectado: {e}")
        
    finally:
        # Esto libera los puertos para que Oracle pueda funcionar después
        automoviles.stop()
        print(" Motor Spark detenido correctamente.")

if __name__ == "__main__":
    analizar_automoviles()
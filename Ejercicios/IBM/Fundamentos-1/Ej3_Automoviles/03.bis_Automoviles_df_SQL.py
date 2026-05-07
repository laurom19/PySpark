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
        #Registro el DF a una Tabla Temporal
        df_automoviles.createOrReplaceTempView("automoviles")

        #.SQL(COLOCO LA QUERY) - IMPORTANTE: ""LA CONSULTA SIEMPRE HACER DE LA SESION, NO DEL DF"""
        autos_rapidos = automoviles.sql ("""
                                         SELECT
                                            model AS modelo,
                                            gear AS marchas_auto,
                                            hp AS potencia
                                        FROM automoviles 
                                         WHERE cyl BETWEEN 4 AND 9
                                         """)
        autos_rapidos.show(6)

    except Exception as e:
        print(f"\n Error detectado: {e}")
        
    finally:
        # Esto libera los puertos para que Oracle pueda funcionar después
        automoviles.stop()
        print(" Motor Spark detenido correctamente.")

if __name__ == "__main__":
    analizar_automoviles()
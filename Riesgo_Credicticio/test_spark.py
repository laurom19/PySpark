import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path

# Forzar el uso del Python del entorno virtual actual
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configuración de Hadoop (Asegúrate que esta sea tu ruta real)
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

def analizar_riesgo():
    print("🚀 Iniciando motor Spark para Análisis de Riesgo...")
    
    spark = SparkSession.builder \
        .appName("AnalisisRiesgoCrediticio") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    try:
        # 1. Cargar el CSV de Clientes
        print("\nCargando datos de clientes...")
        df_clientes = spark.read.csv("clientes_riesgo_masivo.csv", header=True, inferSchema=True)
        
        # 2. Cargar el JSON de Pagos
        # Cambia la línea de carga del JSON por esta:
        print("Cargando historial de pagos...")
        df_pagos = spark.read.option("multiLine", "true").json("historial_pagos_masivo.json")

        # 3. Mostrar una muestra de ambos
        print("\n--- Vista Previa: Clientes ---")
        df_clientes.limit(5).show()

        print("\n--- Vista Previa: Pagos ---")
        df_pagos.limit(5).show()
        
        print("\n✅ ¡Lectura exitosa! El entorno es estable.")

    except Exception as e:
        print(f"\n❌ Error detectado: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    analizar_riesgo()
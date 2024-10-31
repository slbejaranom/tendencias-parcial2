from pyspark.sql import SparkSession
from pyspark.sql import functions

# Creamos la sesión de Spark
sparkSession = SparkSession.builder.appName("parcial-2").getOrCreate()

# Función para leer un dataset
def leer_dataset(ruta):
    return sparkSession.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta)

# Función para imprimir la estructura y las primeras filas de un DataFrame
def imprimir_info_dataframe(df):
    print("Las columnas de este dataset son: ")
    print(df.columns)
    print("Ejemplo del contenido del dataset:")
    df.show(5)

# Función para calcular y mostrar la moda de cada columna
def mostrar_moda(df):
    for columna in df.columns:
        moda = df.groupBy(columna).count().orderBy(functions.desc("count")).first()
        print("La moda para la columna " + columna + " es: " + str(moda.asDict()))

# Función para calcular la frecuencia relativa
def calcular_frecuencia_relativa(df, columna):
    total_count = df.count()  # Total de filas
    frecuencia = (
        df.groupBy(columna)
        .agg(functions.count("*").alias("frecuencia"))
        .withColumn("frecuencia_relativa", functions.col("frecuencia") / total_count)
        .orderBy(functions.col("frecuencia_relativa").desc())
    )
    return frecuencia

# Función para mostrar la frecuencia relativa de cada columna
def mostrar_frecuencia_relativa(df):
    for columna in df.columns:
        print(f"Frecuencia relativa para la columna '{columna}':")
        frecuencia_df = calcular_frecuencia_relativa(df, columna)
        frecuencia_df.show(truncate=False)
        print("\n")

# Leemos el primer dataset
# Tomado de https://www.kaggle.com/datasets/valakhorasani/mobile-device-usage-and-user-behavior-dataset
mobileUserBehaviorDataframe = leer_dataset("datasets/user_behavior_dataset.csv")
imprimir_info_dataframe(mobileUserBehaviorDataframe)
mostrar_moda(mobileUserBehaviorDataframe)
mostrar_frecuencia_relativa(mobileUserBehaviorDataframe)

print("Ahora seguiremos con los datos para el dataset de patrones de carga de vehículos eléctricos")

# Leemos el segundo dataset
# Tomado de https://www.kaggle.com/datasets/valakhorasani/electric-vehicle-charging-patterns
electricCarChargingPatternsDataFrame = leer_dataset("datasets/ev_charging_patterns.csv")
imprimir_info_dataframe(electricCarChargingPatternsDataFrame)
mostrar_moda(electricCarChargingPatternsDataFrame)
mostrar_frecuencia_relativa(electricCarChargingPatternsDataFrame)

print("Ahora seguiremos con los datos para el dataset de comportamiento de compras de clientes")

# Leemos el tercer dataset
# Tomado de https://www.kaggle.com/datasets/cameronseamons/electronic-sales-sep2023-sep2024
customerPurchasePatterns = leer_dataset("datasets/Electronic_sales_Sep2023-Sep2024.csv")
imprimir_info_dataframe(customerPurchasePatterns)
mostrar_moda(customerPurchasePatterns)
mostrar_frecuencia_relativa(customerPurchasePatterns)

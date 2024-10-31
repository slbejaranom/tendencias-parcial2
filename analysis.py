from pyspark.sql import SparkSession
from pyspark.sql import functions

#Creamos la sesión de spark
sparkSession = SparkSession.builder.appName("parcial-2").getOrCreate()

#Leemos el primer dataset
# Tomado de https://www.kaggle.com/datasets/valakhorasani/mobile-device-usage-and-user-behavior-dataset
mobileUserBehaviorDataframe = sparkSession.read.format("csv").option("header","true").option("inferSchema", "true").load("datasets/user_behavior_dataset.csv")
#Imprimimos 5 lineas aleatorias del dataset
print("Las columnas de este dataset son: ")
print(mobileUserBehaviorDataframe.columns)
print("Ejemplo del contenido del dataset")
row = mobileUserBehaviorDataframe.show(5)
#Moda por columna
for i in range(len(mobileUserBehaviorDataframe.columns)):
    columna = mobileUserBehaviorDataframe.columns[i]
    moda = mobileUserBehaviorDataframe.groupBy(columna).count().orderBy(functions.desc("count")).first()    
    print("La moda para la columna "+columna+" es:" +str(moda.asDict()))

#Función para calcular la frecuencia relativa
def calcular_frecuencia_relativa(df, columna):
    total_count = df.count()  # Total de filas
    frecuencia = (
        df.groupBy(columna)
        .agg(functions.count("*").alias("frecuencia"))
        .withColumn("frecuencia_relativa", functions.col("frecuencia") / total_count)
        .orderBy(functions.col("frecuencia_relativa").desc())
    )
    return frecuencia

#Frecuencia relativa en tabla de comportamiento de usuarios de dispositivo móvil separada por columna
for columna in mobileUserBehaviorDataframe.columns:
    print(f"Frecuencia relativa para la columna '{columna}':")
    frecuencia_df = calcular_frecuencia_relativa(mobileUserBehaviorDataframe, columna)
    frecuencia_df.show(truncate=False)
    print("\n")

print("Ahora seguiremos con los datos para el dataset de patrones de carga de vehículos eléctricos")

#Leemos el segundo dataset
# Tomado de https://www.kaggle.com/datasets/valakhorasani/electric-vehicle-charging-patterns
electricCarChargingPatternsDataFrame = sparkSession.read.format("csv").option("header","true").option("inferSchema", "true").load("datasets/ev_charging_patterns.csv")
#Imprimimos 5 lineas aleatorias del dataset
print("Las columnas de este dataset son: ")
print(electricCarChargingPatternsDataFrame.columns)
print("Ejemplo del contenido del dataset")
row = electricCarChargingPatternsDataFrame.show(5)
#Moda por columna
for i in range(len(electricCarChargingPatternsDataFrame.columns)):
    columna = electricCarChargingPatternsDataFrame.columns[i]
    moda = electricCarChargingPatternsDataFrame.groupBy(columna).count().orderBy(functions.desc("count")).first()    
    print("La moda para la columna "+columna+" es:" +str(moda.asDict()))

#Frecuencia relativa en tabla de patrones de carga de vehículos eléctricos separada por columna
for columna in electricCarChargingPatternsDataFrame.columns:
    print(f"Frecuencia relativa para la columna '{columna}':")
    frecuencia_df = calcular_frecuencia_relativa(electricCarChargingPatternsDataFrame, columna)
    frecuencia_df.show(truncate=False)
    print("\n")

print("Ahora seguiremos con los datos para el dataset de comportamiento de compras de clientes")

#Leemos el tercer dataset
# Tomado de https://www.kaggle.com/datasets/cameronseamons/electronic-sales-sep2023-sep2024
customerPurchasePatterns = sparkSession.read.format("csv").option("header","true").option("inferSchema", "true").load("datasets/Electronic_sales_Sep2023-Sep2024.csv")
#Imprimimos 5 lineas aleatorias del dataset
print("Las columnas de este dataset son: ")
print(customerPurchasePatterns.columns)
print("Ejemplo del contenido del dataset")
row = customerPurchasePatterns.show(5)
#Moda por columna
for i in range(len(customerPurchasePatterns.columns)):
    columna = customerPurchasePatterns.columns[i]
    moda = customerPurchasePatterns.groupBy(columna).count().orderBy(functions.desc("count")).first()    
    print("La moda para la columna "+columna+" es:" +str(moda.asDict()))

#Frecuencia relativa en tabla de comportamientos de compra separada por columna
for columna in customerPurchasePatterns.columns:
    print(f"Frecuencia relativa para la columna '{columna}':")
    frecuencia_df = calcular_frecuencia_relativa(customerPurchasePatterns, columna)
    frecuencia_df.show(truncate=False)
    print("\n")

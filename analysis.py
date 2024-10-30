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

for i in range(len(mobileUserBehaviorDataframe.columns)):
    columna = mobileUserBehaviorDataframe.columns[i]
    moda = mobileUserBehaviorDataframe.groupBy(columna).count().orderBy(functions.desc("count")).first()    
    print("La moda para la columna "+columna+" es:" +str(moda.asDict()))
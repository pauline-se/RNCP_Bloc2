# from pyspark.sql import SparkSession

# # Créez une session Spark
# spark = SparkSession.builder.getOrCreate()

# # Chargement du fichier CSV en tant que DataFrame
# df = spark.read.format("csv").option("header", "true").load("chroniques_full.csv")

# # Affiche le schéma et les premières lignes du DataFrame
# df.printSchema()
# df.show()

# # Transformation et enregistrement en tant que fichier Parquet
# df.write.format("parquet").save("data.parquet")


import logging
from pyspark.sql import SparkSession

# Configuration du logger
logging.basicConfig(filename='spark.log', level=logging.INFO)

# Création d'un objet logger
logger = logging.getLogger('spark_logger')

# Création d'un gestionnaire de fichiers pour écrire dans le fichier de log
file_handler = logging.FileHandler('spark.log')
file_handler.setLevel(logging.INFO)

# Ajout du gestionnaire de fichiers au logger
logger.addHandler(file_handler)

# Créez une session Spark
spark = SparkSession.builder.getOrCreate()

# Chargement du fichier CSV en tant que DataFrame
df = spark.read.format("csv").option("header", "true").load("chroniques_full.csv")

# Écriture du schéma dans le fichier de log
schema = df.schema
logger.info("Schema:\n%s", schema)

# Affichage des premières lignes dans le fichier de log
rows = df.take(20)
logger.info("Premières lignes:")
for row in rows:
    logger.info(row)

# Transformation et enregistrement en tant que fichier Parquet
df.write.format("parquet").save("data2.parquet")

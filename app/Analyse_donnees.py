import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType
# import re

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
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

schemaObservations = StructType([StructField('id', IntegerType() , True)
                    # , StructField('occurrenceID', IntegerType() , True)
                    , StructField('basisOfRecord', StringType() , True)
                    #, StructField('modified', TimestampType() , True)
                    , StructField('modified', StringType() , True)
                    # , StructField('institutionCode', StringType() , True)
                    # , StructField('collectionCode', StringType() , True)
                    , StructField('datasetName', StringType() , True)
                    , StructField('informationWithheld', StringType() , True)
                    # , StructField('catalogNumber', IntegerType() , True)
                    # , StructField('references', StringType() , True)
                    , StructField('occurrenceRemarks', StringType() , True)
                    , StructField('recordedBy', StringType() , True)
                    , StructField('recordedByID', StringType() , True)
                    # , StructField('identifiedBy', StringType() , True)
                    # , StructField('identifiedByID', StringType() , True)
                    , StructField('captive', StringType() , True)
                    #, StructField('eventDate', TimestampType() , True)
                    , StructField('eventDate', StringType() , True)
                    , StructField('eventTime', StringType() , True)
                    # , StructField('verbatimEventDate', StringType() , True)
                    # , StructField('verbatimLocality', StringType() , True)
                    , StructField('decimalLatitude', DoubleType() , True)
                    , StructField('decimalLongitude', DoubleType() , True)
                    , StructField('coordinateUncertaintyInMeters', FloatType() , True)
                    # , StructField('geodeticDatum', StringType() , True)
                    , StructField('countryCode', StringType() , True)
                    , StructField('stateProvince', StringType() , True)
                    , StructField('identificationID', StringType() , True)
                    #, StructField('dateIdentified', TimestampType() , True)
                    # , StructField('dateIdentified', StringType() , True)
                    # , StructField('identificationRemarks', StringType() , True)
                    , StructField('taxonID', IntegerType() , True)
                    , StructField('scientificName', StringType() , True)
                    , StructField('taxonRank', StringType() , True)
                    , StructField('kingdom', StringType() , True)
                    , StructField('phylum', StringType() , True)
                    , StructField('class', StringType() , True)
                    , StructField('order', StringType() , True)
                    , StructField('family', StringType() , True)
                    , StructField('genus', StringType() , True)
                    # , StructField('license', StringType() , True)
                    # , StructField('rightsHolder', StringType() , True)
                    # , StructField('inaturalistLogin', StringType() , True)
                    # , StructField('publishingCountry', StringType() , True)
                    , StructField('sex', StringType() , True)
                    , StructField('lifeStage', StringType() , True)
                    , StructField('reproductiveCondition', StringType() , True)
])


# Chargement du fichier CSV en tant que DataFrame
# df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("US_aves.csv")
df = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaObservations).load("US_aves_filtres_colonnes_pipe.csv")



# # Écriture du schéma dans le fichier de log
# schema = df.schema
# logger.info("Schema:\n%s", schema)

# # Affichage des premières lignes dans le fichier de log
# rows = df.take(20)
# logger.info("Premières lignes:")
# for row in rows:
#     logger.info(row)

# Transformation et enregistrement en tant que fichier Parquet
# df.write.mode("overwrite").format("parquet").save("US_aves.parquet")
df.write.mode("overwrite").format("parquet").save("US_aves.parquet")




#Intégration du fichier 

schemaNames = StructType([StructField('taxonID', IntegerType() , True)
                    , StructField('Name', StringType() , True)])

df_names = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaNames).load("French_names.csv")

df_names.write.mode("overwrite").format("parquet").save("French_names.parquet")



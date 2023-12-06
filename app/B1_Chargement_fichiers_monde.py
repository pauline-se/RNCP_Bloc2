import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType


# Création de la session spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

###########################################
# I - Fichier WORLD_month_country_taxon.csv
###########################################

# 1. Création du schéma de données pour le fichier WORLD_month_country_taxon.csv

schemaObservations = StructType([StructField('countryCode', StringType() , True)
                    , StructField('taxonID', IntegerType() , True)
                    , StructField('DateMois', StringType() , True)
                    , StructField('nb_obs', IntegerType() , True)
])


# 2. Chargement du fichier CSV US_aves_pipe en tant que DataFrame
df = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaObservations).load("/app/WORLD_month_country_taxon.csv")


# 3. Transformation et enregistrement du dataframe df en tant que fichier Parquet
df.write.mode("overwrite").format("parquet").save("/app/WORLD_month_country_taxon.parquet")





# ###########################################
# # I - Fichier WORLD_month_country_family.csv
# ###########################################

# # 1. Création du schéma de données pour le fichierWORLD_month_country_family.csv

# schemaObservations = StructType([StructField('countryCode', StringType() , True)
#                     , StructField('family', StringType() , True)
#                     , StructField('DateMois', StringType() , True)
#                     , StructField('nb_obs', DoubleType() , True)
# ])


# # 2. Chargement du fichier CSV US_aves_pipe en tant que DataFrame
# df = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaObservations).load("/app/WORLD_month_country_family.csv")


# # 3. Transformation et enregistrement du dataframe df en tant que fichier Parquet
# df.write.mode("overwrite").format("parquet").save("/app/WORLD_month_country_family.parquet")

###########################################
# II - Fichier Noms_codes_pays.csv
###########################################

# 1. Création du schéma de données pour le fichier Noms_codes_pays.csv

schemaNoms = StructType([StructField('code_inat', StringType() , True)
                    , StructField('code_ISO', StringType() , True)
                    , StructField('Noms_pays', StringType() , True)
])

# 2. Chargement du fichier CSV Noms_codes_pays en tant que DataFrame
df_nomspays = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaNoms).load("/app/Noms_codes_pays.csv")


# 3. Transformation et enregistrement du dataframe df en tant que fichier Parquet
df_nomspays.write.mode("overwrite").format("parquet").save("/app/Noms_codes_pays.parquet")

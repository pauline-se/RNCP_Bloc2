from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# Création de la session spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

########################################
# I - Fichier US_aves
########################################

# 1. Création du schéma de données pour le fichier US_aves

schemaObservations = StructType([StructField('id', IntegerType() , True)
                    , StructField('eventDate', StringType() , True)
                    , StructField('decimalLatitude', DoubleType() , True)
                    , StructField('decimalLongitude', DoubleType() , True)
                    , StructField('countryCode', StringType() , True)
                    , StructField('stateProvince', StringType() , True)
                    , StructField('taxonID', IntegerType() , True)
                    , StructField('scientificName', StringType() , True)
                    , StructField('taxonRank', StringType() , True)
                    , StructField('kingdom', StringType() , True)
                    , StructField('phylum', StringType() , True)
                    , StructField('class', StringType() , True)
                    , StructField('order', StringType() , True)
                    , StructField('family', StringType() , True)
                    , StructField('genus', StringType() , True)
])


# 2. Chargement du fichier CSV US_aves_pipe en tant que DataFrame
df = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaObservations).load("/app/US_aves.csv")


# 3. Transformation et enregistrement du dataframe df en tant que fichier Parquet
df.write.mode("overwrite").format("parquet").save("/app/US_aves.parquet")


########################################
# II - Fichier Noms_especes
########################################

# 1. Création du schéma de données pour le fichier Noms_especes
schemaNomsEspeces = StructType([StructField('taxonID', IntegerType() , True)
                    , StructField('Name', StringType() , True)])

# 2. Chargement du fichier CSV Noms_especes en tant que DataFrame
df_NomsEspeces = spark.read.format("csv").option("header", "true").option("delimiter","|").schema(schemaNomsEspeces).load("/app/Noms_especes.csv")

# 3. Transformation et enregistrement du dataframe df_NomsEspeces en tant que fichier Parquet
df_NomsEspeces.write.mode("overwrite").format("parquet").save("/app/Noms_especes.parquet")



########################################
# III - Fichier Noms_Etats
########################################

# 1. Création du schéma de données pour le fichier Noms_Etats
schemaStateNames = StructType([StructField('StateName', StringType() , True)
                    , StructField('StateCode', StringType() , True)])

# 2. Chargement du fichier CSV Noms_Etats en tant que DataFrame
df_StateNames = spark.read.format("csv").option("header", "true").option("delimiter",";").schema(schemaStateNames).load("/app/Noms_Etats.csv")

# 3. Transformation et enregistrement du dataframe df_StateNames en tant que fichier Parquet
df_StateNames.write.mode("overwrite").format("parquet").save("/app/Noms_Etats.parquet")
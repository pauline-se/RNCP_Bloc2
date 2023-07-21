from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, expr, concat, lit, to_date, when, date_format
import pyspark.sql

# from plotly.subplots import make_subplots
# import plotly.graph_objects as go
# import chart_studio
# import matplotlib.pyplot as plt
import pandas as pd
# import matplotlib.ticker as ticker
import shutil

# Création session Spark
# spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)



# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/US_aves.parquet")




# Chargement des fichiers Parquet French_names en tant que DataFrame
# df_names = spark.read.format("parquet").load("/app/French_names.parquet")


# Utilisez la fonction `withColumn()` pour ajouter une nouvelle colonne avec les 4 premiers caractères numériques
df_obs = df_obs.withColumn('yearEvent', expr("CASE WHEN cast(substring(eventDate, 1, 4) as double) IS NOT NULL THEN substring(eventDate, 1, 4) ELSE NULL END"))

df_obs = df_obs.withColumn('monthEvent', expr("CASE WHEN cast(substring(eventDate, 6, 2) as double) IS NOT NULL THEN substring(eventDate, 6, 2) ELSE NULL END"))

df_obs = df_obs.withColumn('dayEvent', expr("CASE WHEN cast(substring(eventDate, 9, 2) as double) IS NOT NULL THEN substring(eventDate, 9, 2) ELSE NULL END"))

df_obs = df_obs.drop('eventDate')

df_obs = df_obs.withColumn(
    'eventDate', 
    when(
        col('yearEvent').isNotNull() & col('monthEvent').isNotNull() & col('dayEvent').isNotNull(),
        to_date(concat(col('yearEvent'), lit('-'), col('monthEvent'), lit('-'), col('dayEvent'))),
    )
)

df_obs = df_obs.withColumn(
    'eventDate_month', 
    when(
        col('yearEvent').isNotNull() & col('monthEvent').isNotNull(),
        to_date(concat(col('yearEvent'), lit('-'), col('monthEvent'), lit('-01'))),
    )
)

# Convertir la colonne 'eventDate' en une chaîne de caractères au format de date souhaité
df_obs = df_obs.withColumn('eventDate', date_format('eventDate', 'yyyy-MM-dd'))
df_obs = df_obs.withColumn('eventDate_month', date_format('eventDate_month', 'yyyy-MM-dd'))

df_obs = df_obs.drop('dayEvent', 'eventTime', 'countryCode', 'modified')
# df_obs = df_obs.drop('yearEvent', 'eventTime', 'countryCode', 'modified')

# Écrivez le DataFrame dans un fichier CSV
# df_obs.write.mode("overwrite").format("csv").option("header", "true").mode("overwrite").save("/app/fichier_prep.csv")

# shutil.rmtree("/app/US_aves.parquet")
df_obs.write.mode("overwrite").format("parquet").save("/app/US_aves_postprep.parquet")





from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, lit, to_date, when, date_format, first
import plotly.graph_objects as go

spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
 




# 1. Chargement des fichiers Parquet US_aves en tant que DataFrame spark
df_obs = spark.read.format("parquet").load("/app/WORLD_month_country_taxon.parquet")


# 1. Chargement des fichiers parquet French_names en tant que DataFrame
df_names = spark.read.format("parquet").load("/app/Noms_codes_pays.parquet")

#Jointure entre les 2 fichiers, sur l'ID taxon
nb_lignes_avant_join = df_obs.count()

print("Nombre lignes dans dataframe df_obs avant jointure :",nb_lignes_avant_join)
print(" ")

# jointure gauche pour ne pas perdre de données si certaines observations n'ont pas leurs noms français de renseignées
df_obs = df_obs.join(df_names, df_obs["CountryCode"] == df_names["code_inat"], "left")

nb_lignes_après_join = df_obs.count()
print("Nombre lignes dans dataframe df_obs après jointure :", nb_lignes_après_join)
print(" ")

if (nb_lignes_après_join == nb_lignes_avant_join):
    Jointure_ok = "Vrai"
else:
    Jointure_ok = "Faux"

print("Jointure ne duplique pas les lignes : ", Jointure_ok )
print(" ")

df_obs.write.mode("overwrite").format("parquet").save("/app/WORLD_month_country_taxon_names.parquet")



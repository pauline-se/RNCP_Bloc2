from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, lit, to_date, when, date_format, first
import plotly.graph_objects as go


###############################################################
####    Analyse exploratoire et preparation des donnees    ####
###############################################################

###################################
# Initialisation session spark
###################################

spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)



##########################################
# I- Preparation donnees US_aves.parquet
##########################################

print("#############################################")
print("# I- Preparation donnees US_aves.parquet")
print("#############################################")
print(" ")
print(" ")


# 1. Chargement des fichiers Parquet US_aves en tant que DataFrame spark
print("################## 1. Chargement du fichier ######################")
df_obs = spark.read.format("parquet").load("/app/US_aves.parquet")
print("Chargement du fichier depuis /app/US_aves.parquet")
print("")
print("")
print("")


# 2. Exploration du fichier avant preparation des donnees
print("################## 2. Description du fichier ######################")

print("Description du fichier")
df_obs.describe().show()
print(" ")

print("Exploration colonne id :")
df_obs_id = df_obs.select("id")
df_obs_id.show()
print(" ")

print("Exploration colonne StateProvince :")
df_obs_state = df_obs.select("StateProvince")
df_obs_state.show()
print(" ")

print("Exploration colonne eventDate :")
df_obs_state = df_obs.select("eventDate")
df_obs_state.show()
print("")
print("")
print("")


# 2. Contrôle du nombre de lignes de donnees du fichier
print("############ 3. Controle nombre de lignes du fichier ############")
nb_lignes_init_US_aves = df_obs.count()
print("Nombre de lignes dans le fichier US_aves (initial) :", nb_lignes_init_US_aves)
print("")
print("")
print("")



# 3. Verification de l'unicite des id observations dans le dataframe
print("############ 4. Controle unicite des lignes du fichier ############")
id_obs_counts = df_obs.select("id").distinct().count()
print("Nombre de id observations uniques dans le dataframe US_aves :", id_obs_counts)
print("")
print("")
print("")


if (nb_lignes_init_US_aves == id_obs_counts):
    Verif_id_uniques_obs = "Vrai"
else:
    Verif_id_uniques_obs = "Faux"

print("Unicite des id observations : ", Verif_id_uniques_obs )
print("")
print("")
print("")



# 5. Gestion des dates
print("################ 5. Gestion des dates ################")

df_obs = df_obs.withColumn('yearEvent', expr("CASE WHEN cast(substring(eventDate, 1, 4) as double) IS NOT NULL THEN substring(eventDate, 1, 4) ELSE NULL END"))
df_obs = df_obs.withColumn('monthEvent', expr("CASE WHEN cast(substring(eventDate, 6, 2) as double) IS NOT NULL THEN substring(eventDate, 6, 2) ELSE NULL END"))
df_obs = df_obs.withColumn('dayEvent', expr("CASE WHEN cast(substring(eventDate, 9, 2) as double) IS NOT NULL THEN substring(eventDate, 9, 2) ELSE NULL END"))

#Supression de la colnne eventDate pour la recreer avec les donnees extraites et sous format de date, dont on est maintenant sur qu'elles sont bien formatees
df_obs = df_obs.drop('eventDate')
df_obs = df_obs.withColumn(
    'eventDate', 
    when(
        col('yearEvent').isNotNull() & col('monthEvent').isNotNull() & col('dayEvent').isNotNull(),
        to_date(concat(col('yearEvent'), lit('-'), col('monthEvent'), lit('-'), col('dayEvent'))),
    )
)
df_obs = df_obs.withColumn('eventDate', date_format('eventDate', 'yyyy-MM-dd'))

#Calcul d'une colonne Annee - Mois, avec date au 1er du mois. Facilite les agrégations futures par mois
df_obs = df_obs.withColumn(
    'yearMonthEvent', 
    when(
        col('yearEvent').isNotNull() & col('monthEvent').isNotNull(),
        to_date(concat(col('yearEvent'), lit('-'), col('monthEvent'), lit('-01'))),
    )
)

# Convertir la colonne 'yearMonthEvent' en une colonne de type date au format souhaite
df_obs = df_obs.withColumn('yearMonthEvent', date_format('yearMonthEvent', 'yyyy-MM-dd'))

#Comptage d nombre de lignes supprimées
nb_lignes_US_aves_post_drop_dates_nulles = df_obs.count()

print("Les lignes avec une date manquante ou mal renseignee ont ete supprimees. Total des lignes supprimees :", nb_lignes_init_US_aves - nb_lignes_US_aves_post_drop_dates_nulles)
print("")
print("")
print("")



# Selection de la plage de dates etudiees : creation d'un graph pour voir graphiquement l'etendue de la plage de dates

df_annee = df_obs.groupby('yearEvent').count()
df_annee_pd = df_annee.toPandas()
df_annee_pd.sort_values(by='yearEvent', ascending=True, inplace=True)

plotly_parannee = go.Figure(data=[go.Bar(
    x=df_annee_pd['yearEvent']
    , y=df_annee_pd['count']
    )])

plotly_parannee.update_layout(
    title='Graphique en barres du nombre d\'observations par annee',
    xaxis_title='Annees',
    yaxis_title='Nombre d\'observations'
)

plotly_parannee.write_image('/app/Repr_graphiques/AF2_1_Diagramme_nb_obs_par_annee_plotly.png')


# Au vu des resultats, on decide de n'observer que depuis 2017
df_obs = df_obs.where(df_obs['yearMonthEvent'] >= '2017-01-01')



nb_lignes_US_aves_post_drop_dates = df_obs.count()
print("Nombre de lignes supprimees pre 2017:", nb_lignes_US_aves_post_drop_dates_nulles - nb_lignes_US_aves_post_drop_dates)
print(" ")
print("Nouveau decompte de lignes :", nb_lignes_US_aves_post_drop_dates)
print(" ")


# 6. Verification de l'existence ou non de lignes avec taxons vides dans le dataframe :
print("############ 6. Supression des lignes sans taxon ou latitude ou longitude ############")
df_obs = df_obs.dropna(subset=["taxonID"])
df_obs = df_obs.dropna(subset=["decimalLatitude"])
df_obs = df_obs.dropna(subset=["decimalLongitude"])
print(" ")
print(" ")
print(" ")


print("############ 7. Supression des colonnes inutiles ############")
# Suppressions des colonnes inutiles pour na pas alourdir le dataframe
df_obs = df_obs.drop('dayEvent')
df_obs = df_obs.dropna(subset=["yearEvent"])
df_obs = df_obs.dropna(subset=["monthEvent"])
df_obs = df_obs.dropna(subset=["eventDate"])
df_obs = df_obs.dropna(subset=["yearMonthEvent"])
print(" ")
print(" ")
print(" ")


# 7. Verification du nombre de lignes final
nb_lignes_final_US_aves = df_obs.count()
print("Nombre de lignes final US_aves : ", nb_lignes_final_US_aves)
print(" ")
print(" ")
print(" ")

################################################
# II- Preparation fichier Noms_especes.parquet
################################################

print("################################################")
print("# II- Preparation fichier Noms_especes.parquet")
print("################################################")
print(" ")
print(" ")


# 1. Chargement des fichiers parquet French_names en tant que DataFrame
df_names = spark.read.format("parquet").load("/app/Noms_especes.parquet")

# 2. Exploration du fichier
print("Aperçu du fichier Noms_especes :")
print(df_names.tail(20))
print(" ")

print("Description du fichier Noms_especes :")
df_names.describe().show()
print(" ")

# 3. Recherche / Suppression des doublons

#liste de tous les id taxons
id_counts = df_names.groupBy("taxonID").count()

# Filtrer les ID taxons qui ont plus d'une occurrence (sont en double ou +)
duplicate_ids = id_counts.filter(col("count") > 1)

# Afficher les ID en double ou +
print("Taxons ID en double ou plus :")
duplicate_ids.show()
print(" ")

# Compter les ID en double
print("Nombre de doublons dans les ID taxons :", duplicate_ids.count())
print(" ")

# Selectionnez la première occurrence de chaque groupe d'ID avec son libelle correspondant
df_names = df_names.groupBy("taxonID").agg(first("Name"))
df_names = df_names.withColumnRenamed("first(Name)", "Name")


#Le fichier a-t-il toujours des doublons :
id_counts = df_names.groupBy("taxonID").count()

# Filtrer les ID qui ont plus d'une occurrence (sont en double)
no_duplicate_ids = id_counts.filter(col("count") > 1)

# Afficher les ID en double
print("lignes restantes en doublons :")
no_duplicate_ids.show()
print(" ")

print("fichier sans doublon :")
df_names.show()
print(" ")


nombre_lignes_french_names = df_names.count()

print("nombre de lignes dans french names :",nombre_lignes_french_names)
print(" ")


###########################################
# III- Preparation Noms_Etats.parquet
###########################################


print("########################################")
print("# III- Preparation Noms_Etats.parquet   ")
print("########################################")
print(" ")
print(" ")


# 1. Chargement des fichiers parquet French_names en tant que DataFrame
df_etats = spark.read.format("parquet").load("/app/Noms_Etats.parquet")

# 2. Exploration du fichier
print("Aperçu du fichier Noms_Etats :")
print(df_etats.tail(20))
print(" ")

print("Description du fichier Noms_Etats :")
df_etats.describe().show()
print(" ")

# 3. Recherche de doublons
statecode_counts = df_etats.groupBy("StateCode").count()
statename_counts = df_etats.groupBy("StateName").count()

# Filtrer les ID taxons qui ont plus d'une occurrence (sont en double ou +)
duplicate_statecode = statecode_counts.filter(col("count") > 1)
duplicate_statename = statename_counts.filter(col("count") > 1)

if duplicate_statecode.count() + duplicate_statename.count() == 0:
    print("Pas de doublons dans le fichier Noms_Etats")
else :
    print("ATTENTION, doublons dans le fichier Noms_Etats, A TRAITER")
print(" ")

########################################################################
# IV- Jointure des 3 dataframes et ecriture du nouveau fichier parquet
########################################################################

print("########################################################################")
print("# IV- Jointure des 3 dataframes et ecriture du nouveau fichier parquet")
print("########################################################################")
print(" ")
print(" ")

print("####### 1. Jointure entre les 2 dataframes df_obs et df_names, sur l'ID taxon #######")
#Jointure entre les 2 fichiers, sur l'ID taxon
nb_lignes_avant_join = df_obs.count()

print("Nombre lignes dans dataframe df_obs avant jointure :",nb_lignes_avant_join)
print(" ")

# jointure gauche pour ne pas perdre de donnees si certaines observations n'ont pas leurs noms français de renseignes

df_obs = df_obs.join(df_names,"taxonID", "left")

print("Nombre lignes Name nulles :", df_obs.filter(df_obs["Name"].isNull()).count())
print("Nombre lignes Name non nulles :", df_obs.filter(df_obs["Name"].isNotNull()).count())
print(" ")

# Modification pour mettre les idtaxons à la place des noms des taxons quand pas de correspondance
df_obs = df_obs.withColumn("Name", when((col("Name").isNull()), col("taxonID")).otherwise(col("Name")))

df_obs_Name = df_obs.select("Name")
print("Colonne Name : ")
df_obs_Name.show()
print("")
print("")
print("")



#Jointure avec le fichier Noms_Etats pour obtenir la colonne StateCode dans le fichier des observations
print("####### 2. Jointure entre nouveau df df_obs et df_etats, sur le nom Etat #######")

df_obs = df_obs.join(df_etats, df_obs["StateProvince"] == df_etats["StateName"], "inner")


df_obs_StateCode = df_obs.select("StateCode")
print("Colonne StateCode : ")
df_obs_StateCode.show()
print(" ")


nb_lignes_après_join = df_obs.count()
print("Nombre lignes dans dataframe df_obs après les 2 jointures :", nb_lignes_après_join)
print(" ")

if (nb_lignes_après_join == nb_lignes_avant_join):
    Jointure_ok = "Vrai"
else:
    Jointure_ok = "Faux"

print("Jointure ne duplique pas les lignes : ", Jointure_ok )
print("")
print("")
print("")


nb_lignes_avant_join = nb_lignes_après_join


print("################## 3. création du nouveau fichier parquet ######################")
df_obs.write.mode("overwrite").format("parquet").save("/app/US_aves_names.parquet")


from pyspark.sql import SparkSession
import plotly.graph_objects as go
import pandas as pd
import os
import plotly.express as px
from pyspark.sql.functions import count, col, sum


# Set the MPLCONFIGDIR environment variable to a writable directory
os.environ['MPLCONFIGDIR'] = '/app/matplotlib_config'

# Création session Spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/WORLD_month_country_taxon_names.parquet")


# df_obs.show(30)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
# df_obs_family = spark.read.format("parquet").load("/app/WORLD_month_country_family_names.parquet")
 

# df_obs_family.show(30)

#######################################################################
#######################################################################
# I- Carte du monde des grues
#######################################################################
#######################################################################

df_obs_grues = df_obs.where(df_obs['taxonID'] == '41')


####################################################################
# Nombre d'observations par Etats 2017-2022 toutes années confondues
####################################################################

df_obs_grues_Noms_pays = df_obs_grues.groupBy("Noms_pays").agg(sum("nb_obs").alias("nb_obs"))
df_obs_grues_Noms_pays_pd = df_obs_grues_Noms_pays.toPandas()
df_obs_grues_Noms_pays_pd.sort_values(by='nb_obs', ascending=False, inplace=True)

plotly_grues_par_pays = go.Figure(data=[go.Bar(x=df_obs_grues_Noms_pays_pd['Noms_pays'], y=df_obs_grues_Noms_pays_pd['nb_obs'])])

plotly_grues_par_pays.update_layout(
    title='Histogramme du nombre d\'ID par pays pour Grues',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_grues_par_pays.write_html('/app/Repr_graphiques/B3/BF3_1_Diagramme_nb_obs_par_Pays_grues_plotly.html')


####################################################################
# Nombre d'observations par année tous Etats confondus
####################################################################

df_obs_grues_mois = df_obs_grues.groupBy("DateMois").agg(sum("nb_obs").alias("nb_obs"))
df_obs_grues_mois_pd = df_obs_grues_mois.toPandas()
df_obs_grues_mois_pd.sort_values(by='DateMois', inplace=True)

plotly_grues_mois_pd = go.Figure(data=[go.Bar(x=df_obs_grues_mois_pd['DateMois'], y=df_obs_grues_mois_pd['nb_obs'])])

plotly_grues_mois_pd.update_layout(
    title='Graphique en barres du nombre d\'observations par mois pour grues',
    xaxis_title='Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_grues_mois_pd.write_html('/app/Repr_graphiques/B3/BF3_2_Diagramme_nb_obs_par_mois_grues_plotly.html')


####################################################################
# Carte animée observations migration des grues
####################################################################


df_obs_grues_pd = df_obs_grues.toPandas()
df_obs_grues_pd.sort_values(by='nb_obs', ascending=False, inplace=True)
df_obs_grues_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_grues_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs", 
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations de Grues par Pays en 2022",
                    color_continuous_scale="deep",
                    range_color=(0, df_obs_grues_pd["nb_obs"].max()))  


fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations de Grues par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/Repr_graphiques/B3/BF3_3_carte_animee_migration_grues.html')



#######################################################################
#######################################################################
# I- Carte du monde des pinsons
#######################################################################
#######################################################################

df_obs_pinsons = df_obs.where(df_obs['taxonID'] == '10069')
df_obs_pinsons_pd = df_obs_pinsons.toPandas()
df_obs_pinsons_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_pinsons_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des pinsons par Pays en 2022",
                    color_continuous_scale="deep",  
                    range_color=(0, df_obs_pinsons_pd["nb_obs"].max())) 


fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des pinsons par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/Repr_graphiques/B3/BF3_4_carte_animee_migration_pinsons.html')

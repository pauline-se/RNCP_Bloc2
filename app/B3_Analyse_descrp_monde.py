from pyspark.sql import SparkSession
from plotly.subplots import make_subplots
import plotly.graph_objects as go
# import chart_studio
# import matplotlib.pyplot as plt
import pandas as pd
# import matplotlib.ticker as ticker
import os
from pyspark.sql.functions import count, col, concat, lit
import plotly.express as px


# Set the MPLCONFIGDIR environment variable to a writable directory
os.environ['MPLCONFIGDIR'] = '/app/matplotlib_config'

# Création session Spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/WORLD_month_country_taxon_names.parquet")


# df_obs.show(30)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs_family = spark.read.format("parquet").load("/app/WORLD_month_country_family_names.parquet")


# df_obs_family.show(30)


#######################################################################
# I- carte du monde des grues
#######################################################################




df_obs_grues = df_obs.where(df_obs['taxonID'] == '41')


#################
# Nombre d'observations par Etats 2017-2022 tout confondus

df_paysGrues_pd = df_obs_grues.toPandas()
df_paysGrues_pd.sort_values(by='nb_obs', ascending=False, inplace=True)

plotly_grues_par_pays = go.Figure(data=[go.Bar(x=df_paysGrues_pd['countryCode'], y=df_paysGrues_pd['nb_obs'])])

plotly_grues_par_pays.update_layout(
    title='Histogramme du nombre d\'ID par pays pour Grues',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_grues_par_pays.write_image('/app/barchart_nb_obs_par_Pays_grues_plotly.png')


#################
# Nombre d'observations par année tous Etats confondus


plotly_canardparannee = go.Figure(data=[go.Bar(
    x=df_paysGrues_pd['DateMois']
    , y=df_paysGrues_pd['nb_obs']
    #, marker_color='#8D3787' 
    )])


plotly_canardparannee.update_layout(
    title='Graphique en barres du nombre d\'observations par mois pour grues',
    xaxis_title='Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_canardparannee.write_image('/app/barchart_nb_obs_par_mois_grues_plotly.png')




### Statistiques inférentielles

#################
# Carte animée



# df_annee_etat_canard.show()


# df_noms_etats = df_annee_etat_canard.select("StateProvince").distinct()
# df_noms_etats.show(100)

# Convertir le DataFrame Spark en DataFrame pandas

# Créer une carte animée avec Plotly Express

df_paysGrues_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_paysGrues_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations de Grues par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_paysGrues_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
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

fig.write_html('/app/carte_animee_grues.html')






#######################################################################
# I- carte du monde des Cigognes blanches
#######################################################################




df_obs_cigognes = df_obs.where(df_obs['taxonID'] == '4733')
df_payscigogness_pd = df_obs_cigognes.toPandas()
df_payscigogness_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_payscigogness_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations de Cigognes par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_payscigogness_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations de Cigognes par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_cigognes.html')



#######################################################################
# I- carte du monde des Canards colvert
#######################################################################




df_obs_colvert = df_obs.where(df_obs['taxonID'] == '6930')
df_payscolvert_pd = df_obs_colvert.toPandas()
df_payscolvert_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_payscolvert_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations de canards colverts par pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_payscolvert_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations de canards colverts par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_canards_colverts.html')



#######################################################################
# I- carte du monde des  Oies cendrées
#######################################################################




df_obs_oies = df_obs.where(df_obs['taxonID'] == '7018')
df_obs_oies_pd = df_obs_oies.toPandas()
df_obs_oies_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_oies_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des oies cendrées par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_oies_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des oies cendrées par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_oies_cendrees.html')






#######################################################################
# I- carte du monde des  hirondelles
#######################################################################




df_obs_hirondelles = df_obs.where(df_obs['taxonID'] == '64705')

df_obs_hirondelles_pd = df_obs_hirondelles.toPandas()

df_obs_hirondelles_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_hirondelles_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des hirondelles par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_hirondelles_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des hirondelles par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_hirondelles_fenêtre.html')



#######################################################################
# I- carte du monde des becs croisés des sapins
#######################################################################




df_obs_becs_croises = df_obs.where(df_obs['taxonID'] == '10411')

df_obs_becs_croises_pd = df_obs_becs_croises.toPandas()

df_obs_becs_croises_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_becs_croises_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des becs_croisés des sapins par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_becs_croises_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des becs_croisés des sapins par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_becs_croises.html')


#######################################################################
# I- carte du monde des papillons monarques
#######################################################################




df_obs_monarques = df_obs.where(df_obs['taxonID'] == '48662')
df_obs_monarques_pd = df_obs_monarques.toPandas()
df_obs_monarques_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_monarques_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des papillons monarques par Pays en 2022",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_monarques_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des papillons monarques par pays en 2022',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_papillons_monarques.html')







#######################################################################
# I- carte du monde des Hirundinidae
#######################################################################






df_obs_famhirondelles = df_obs_family.where(df_obs_family['family'] == 'Hirundinidae')

# Au vu des résultats, on décide de n'observer que depuis 2017
df_obs_famhirondelles = df_obs_famhirondelles.where(df_obs_famhirondelles['DateMois'] >= '2022-01-01')

# Au vu des résultats, on décide de n'observer que depuis et avant 2023
df_obs_famhirondelles = df_obs_famhirondelles.where(df_obs_famhirondelles['DateMois'] < '2023-01-01')

df_obs_famhirondelles_pd = df_obs_famhirondelles.toPandas()
df_obs_famhirondelles_pd.sort_values(by='DateMois', ascending=True, inplace=True)


fig = px.choropleth(df_obs_famhirondelles_pd,
                    locationmode="country names",
                    locations="Noms_pays",
                    color="nb_obs",  # Spécifiez la colonne pour la coloration
                    hover_name="Noms_pays",
                    animation_frame="DateMois",
                    title="Évolution du nombre d'observations des hirondelles par Pays",
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_famhirondelles_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
fig.update_geos(
    showcountries=True,
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig.update_layout(
    title='Évolution du nombre d\'observations des hirondelles par pays',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig.write_html('/app/carte_animee_fam_hirondelles.html')




#######################################################################
# I- carte du monde des pinsons
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
                    color_continuous_scale="deep",  # Choisissez une échelle de couleurs
                    range_color=(0, df_obs_pinsons_pd["nb_obs"].max()))  # Définissez la plage de couleurs

# Personnalisation de la carte
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

fig.write_html('/app/carte_animee_pinsons.html')

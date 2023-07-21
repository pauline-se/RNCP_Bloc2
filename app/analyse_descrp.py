from pyspark.sql import SparkSession
from plotly.subplots import make_subplots
import plotly.graph_objects as go
# import chart_studio
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker
import os

# Set the MPLCONFIGDIR environment variable to a writable directory
os.environ['MPLCONFIGDIR'] = '/app/matplotlib_config'

# Création session Spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/US_aves_postprep.parquet")

# Chargement des fichiers Parquet French_names en tant que DataFrame
df_names = spark.read.format("parquet").load("/app/French_names.parquet")


############### Première analyse descriptive : nombre d'observations par Etats, 15 premiers Etats #############

df_stateProvince  = df_obs.groupby('stateProvince').count()

df_stateProvince_pd = df_stateProvince.toPandas()
df_stateProvince_pd.sort_values(by='count', ascending=False, inplace=True)
df_stateProvince_pd = df_stateProvince_pd.head(15)

# Représentation graphique

# Create the bar chart using the data from the DataFrame
plotly_Paretat = go.Figure(data=[go.Bar(x=df_stateProvince_pd['stateProvince'], y=df_stateProvince_pd['count'])])

# Customize the layout of the chart
plotly_Paretat.update_layout(
    title='Histogramme du nombre d\'ID par état',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_Paretat.write_image('/app/barchart_nb_obs_par_Etat_plotly.png')




############### analyse descriptive : Espèces les plus observées #############


df_taxons  = df_obs.groupby('taxonID').count()

df_taxons = df_taxons.join(df_names,"taxonID")

df_taxons_pd = df_taxons.toPandas()
df_taxons_pd.sort_values(by='count', ascending=False, inplace=True)
df_taxons_pd = df_taxons_pd.head(25)

# Représentation graphique

# fig, ax2 = plt.subplots(figsize=(10, 6))
# ax2.bar(df_taxons_pd['Name'], df_taxons_pd['count'])
# ax2.set_xlabel('Espèce')
# ax2.set_ylabel('Nombre d\'observations')
# ax2.set_title('Graphique en barres du nombre d\'observations par espèce')
# ax2.tick_params(axis='x', rotation=90)

# # Ajustement automatique de la disposition du graphique
# plt.tight_layout()

# # Enregistrement de la représentation graphique produite
# plt.savefig('/app/barchart_nb_obs_par_espece.png')






# Create the bar chart using the data from the DataFrame
plotly_Paretat = go.Figure(data=[go.Bar(x=df_taxons_pd['Name'], y=df_taxons_pd['count'])])

# Customize the layout of the chart
plotly_Paretat.update_layout(
    title='Graphique en barres du nombre d\'observations par espèce',
    xaxis_title='Espèce',
    yaxis_title='Nombre d\'observations'
)

plotly_Paretat.write_image('/app/barchart_nb_obs_par_espece_plotly.png')


############### analyse descriptive : nb d'obs au fil des années #############


df_annee  = df_obs.groupby('yearEvent').count()

df_annee_pd = df_annee.toPandas()
df_annee_pd.sort_values(by='yearEvent', ascending=True, inplace=True)


# Create the bar chart using the data from the DataFrame
plotly_parannee = go.Figure(data=[go.Bar(
    x=df_annee_pd['yearEvent']
    , y=df_annee_pd['count']
    #, marker_color='#8D3787' 
    )])

# Customize the layout of the chart
plotly_parannee.update_layout(
    title='Graphique en barres du nombre d\'observations par mois',
    xaxis_title='Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_parannee.write_image('/app/barchart_nb_obs_par_mois_plotly.png')

print(df_annee_pd)


#vu les résultats, on décide de n'observer que depuis 2018


df_mois_pd = df_annee_pd.where(df_annee_pd['eventDate_month'] >= '2018-01-01')


from pyspark.sql import SparkSession
import plotly.graph_objects as go
import pandas as pd
import os
from pyspark.sql.functions import count, col
import plotly.express as px
import scipy.stats as stats
import numpy as np

###################################
# Initialisation session spark
################################### 

# Création session Spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/US_aves_names.parquet")

# Set the MPLCONFIGDIR environment variable to a writable directory
os.environ['MPLCONFIGDIR'] = '/app/matplotlib_config'


#####################################################
#####################################################

### Statistiques inférentielles

#####################################################
#####################################################

# Droite de regression linéaire :
# - Par année pour prédire le nombre d'observations de l'année suivante (2023)
# - Mois par mois, pour prédire le nombre d'observations de l'année suivante mois par mois



df_obs_merle = df_obs.where(df_obs['taxonID'] == '12727')

###########################################################################
# Inference du nombre d'observations de Merles d'Amériques aux US en 2023
###########################################################################

# Pour n'avoir que des années complètes, on décide de n'observer que jusqu'au 31/12/2022
df_obs_merle = df_obs_merle.where(df_obs['yearMonthEvent'] < '2023-01-01')

df_obs_annee_etat_merle = df_obs_merle.groupBy("yearEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent")
df_obs_annee_etat_merle_pd = df_obs_annee_etat_merle.toPandas()

df_obs_annee_etat_merle_pd['yearEvent'] = df_obs_annee_etat_merle_pd['yearEvent'].astype(float)

# Détermination de l'axe des x :
x = df_obs_annee_etat_merle_pd['yearEvent']


# Détermination de l'axe des y :
y = df_obs_annee_etat_merle_pd['nb_obs']


# Calcul de la droite de régression linéaire :
slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

# Affichage des coefficients de la régression linéaire :
print("Pente (slope) :", slope)
print("Interception (intercept) :", intercept)
print("Coefficient de corrélation (r_value) :", r_value)
print("Valeur de p (p_value) :", p_value)
print("Erreur standard (std_err) :", std_err)
print(" ")

# Fonction de regression linéaire :
def fonctionRegLineaire(x):
    y = slope*x + intercept
    return y

print("Valeur prédite de 2023 :", fonctionRegLineaire(2023))
print(" ")

# Représentation graphique de la droite de regression linéaire
fig_reg_lineaire = px.scatter(df_obs_annee_etat_merle_pd, x='yearEvent', y='nb_obs', title='Régression Linéaire')

x_range = [min(df_obs_annee_etat_merle_pd['yearEvent']), 2024]
y_range = [fonctionRegLineaire(x) for x in x_range]

line_trace_fig_reg_lineaire = go.Scatter(x=x_range, y=y_range, mode='lines', name=f'Régression Linéaire<br>(R={r_value:.2f})', line=dict(color='red'))

fig_reg_lineaire.add_trace(line_trace_fig_reg_lineaire)

fig_reg_lineaire.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023"])
fig_reg_lineaire.update_yaxes(title_text='Nb d\'observations Merle') 

projection_trace = go.Scatter(x=[2023], y=[fonctionRegLineaire(2023)], mode='markers', name='Projection 2023', marker=dict(color='purple', size=10))
fig_reg_lineaire.add_trace(projection_trace)

fig_reg_lineaire.write_image("/app/Repr_graphiques/AF4_12_Regression_lineaire_plotly.png")


################################################################################################################
# Inference du nombre d'observations de Merles d'Amériques aux US de août 2023 à juillet 2024 mois par mois
################################################################################################################

############
# On fait cela pour chacun des mois maintenant, de 2017 à 2022, puis projection sur chacun des mois de 2023

df_obs_merle = df_obs.where(df_obs['taxonID'] == '12727')

# Pour n'avoir que des mois complets, on décide de n'observer que jusqu'au 31/07/2023
df_obs_merle = df_obs_merle.where(df_obs['yearMonthEvent'] <= '2023-07-31')

df_obs_anneemois_merle = df_obs_merle.groupBy("yearEvent","monthEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent", "monthEvent")

global df_obs_annee_merle_pd

df_obs_annee_merle_pd = df_obs_anneemois_merle.toPandas()

df_obs_annee_merle_pd['yearEvent'] = df_obs_annee_merle_pd['yearEvent'].astype(float)
df_obs_annee_merle_pd['monthEvent'] = df_obs_annee_merle_pd['monthEvent'].astype(float)


print(df_obs_annee_merle_pd.head(30))



def reg_lineaire_mois_annee(mois, annee):

    # Je filtre le dataframe sur le mois en cours et créé un nouveau dataframe pandas "df_obs_anneemois_merle_pd"
    df_obs_anneemois_merle_pd = df_obs_annee_merle_pd[df_obs_annee_merle_pd['monthEvent'] == mois]
    x_mois = df_obs_anneemois_merle_pd['yearEvent']
    y_mois = df_obs_anneemois_merle_pd['nb_obs']

    slope_mois, intercept_mois, r_value_mois, p_value_mois, std_err_mois = stats.linregress(x_mois, y_mois)

    nouveau_point = slope_mois*annee + intercept_mois

    # Nouvelle ligne pour alimenter mon dataframe de départ
    new_row = pd.DataFrame({'yearEvent' : [annee], 'monthEvent': [mois], 'nb_obs': np.nan, 'nb_obs_proj': nouveau_point})


    # Representation graphique
    fig_mois = px.scatter(df_obs_anneemois_merle_pd, x='yearEvent', y='nb_obs', title=f'Régression Linéaire mois {str(mois).zfill(2)} ; (R={r_value_mois:.2f})')

    # Ajoutez la ligne de régression linéaire au graphique
    x_range_mois = [2016, 2025]
    y_range_mois = [(slope_mois*annee + intercept_mois) for annee in x_range_mois]

    line_trace_mois = go.Scatter(x=x_range_mois, y=y_range_mois, mode='lines', name=f'Régression Linéaire mois {str(mois).zfill(2)}<br>(R={r_value_mois:.2f})', line=dict(color='red'))

    fig_mois.add_trace(line_trace_mois)

    # Mettez à jour les étiquettes des axes
    
    ticktext_mois = [f"{year}-{str(mois).zfill(2)}" for year in range(2016, 2026)]

    fig_mois.update_xaxes(title_text='Année', tickangle=45, tickvals=list(range(2016, 2026)), ticktext=ticktext_mois)
    fig_mois.update_yaxes(title_text='Nb d\'observations Merle', range=[0, 8000]) 

    # Ajoutez un point distinct pour l'année
    projection_trace = go.Scatter(x=[annee], y=[nouveau_point], mode='markers', name=f"Projection ({str(annee)}-{str(mois).zfill(2)})", marker=dict(color='purple', size=10))
    fig_mois.add_trace(projection_trace)

    nom_html = f"/app/Repr_graphiques/AF4_13_regression_lineaire_plotly_{str(annee)}-{str(mois).zfill(2)}.html"
    # nom_image = f"/app/Repr_graphiques/AF4_13_regression_lineaire_plotly_{str(annee)}-{str(mois).zfill(2)}.png"

    fig_mois.write_html(nom_html)
    # fig_mois.write_image(nom_image)


    return new_row


for mois in range(8,13):
    df_obs_annee_merle_pd = pd.concat([df_obs_annee_merle_pd, reg_lineaire_mois_annee(mois, 2023)], ignore_index=True, sort=False)

for mois in range(1,8):
    df_obs_annee_merle_pd = pd.concat([df_obs_annee_merle_pd, reg_lineaire_mois_annee(mois, 2024)], ignore_index=True, sort=False)



df_obs_annee_merle_pd['yearMonth'] = df_obs_annee_merle_pd['yearEvent'].astype(int).astype(str) + '-' + df_obs_annee_merle_pd['monthEvent'].astype(int).astype(str).str.zfill(2)

print(df_obs_annee_merle_pd)

fig_global = px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs', title='Nombre d\'observations par mois')

fig_global.add_trace(px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs_proj', title='Nombre d\'observations par mois (Projection 2023 - 2024)', color_discrete_sequence=['red']).data[0])

# Mettez à jour les étiquettes des axes
fig_global.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023","2024"])
fig_global.update_yaxes(title_text='Nb d\'observations Merle', range=[0, 8000]) 

fig_global.write_html("/app/Repr_graphiques/AF4_14_Regression_linéaire_projection_globale.html")

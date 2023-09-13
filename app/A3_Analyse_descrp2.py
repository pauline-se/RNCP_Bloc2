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
from pyspark.sql import functions as F
from statistics import mean, median, pvariance, variance
import scipy.stats as stats
import numpy as np


# Set the MPLCONFIGDIR environment variable to a writable directory
os.environ['MPLCONFIGDIR'] = '/app/matplotlib_config'

# Création session Spark
spark = SparkSession.builder.config("spark.driver.memory", "15g").config("spark.executor.memory", "15g").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Chargement des fichiers Parquet US Aves en tant que DataFrame
df_obs = spark.read.format("parquet").load("/app/US_aves_names.parquet")



#######################################################################
# III- On se concentre sur le merle d'Amérique
#######################################################################


#####################################################
### Statistiques inférentielles
#####################################################

# Droite de regression linéaire

# Par année pour prédire le nombre d'observations de l'année suivante



# Mois par mois, pour prédire le nombre d'observatin de l'année suivante mois par mois



df_obs_merle = df_obs.where(df_obs['taxonID'] == '12727')

df_obs_annee_etat_merle = df_obs_merle.groupBy("yearEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent")

df_obs_annee_etat_merle_pd = df_obs_annee_etat_merle.toPandas()



# Extraction des données
df_obs_annee_etat_merle_pd['yearEvent'] = df_obs_annee_etat_merle_pd['yearEvent'].astype(float)
x = df_obs_annee_etat_merle_pd['yearEvent']
y = df_obs_annee_etat_merle_pd['nb_obs']

print('x :')
print(x)

print('y :')
print(y)

# Calcul de la droite de régression linéaire

# result = stats.linregress(x, y)



slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

# # Affichage des coefficients de la régression linéaire
print("Pente (slope) :", slope)
print("Interception (intercept) :", intercept)
print("Coefficient de corrélation (r_value) :", r_value)
print("Valeur de p (p_value) :", p_value)
print("Erreur standard (std_err) :", std_err)

def find_new_countObs(x):
    y = slope*x + intercept
    return y

print("Valeur prédite de 2023 :", find_new_countObs(2023))

# Calcul des nouvelles valeurs à partir de la régression linéaire
new_y_values = list(map(find_new_countObs, x))

#########
# Représentation graphique de la droite de regression linéaire

fig_reg_lineaire = px.scatter(df_obs_annee_etat_merle_pd, x='yearEvent', y='nb_obs', title='Régression Linéaire')


# Ajoutez la ligne de régression linéaire au graphique
x_range = [min(df_obs_annee_etat_merle_pd['yearEvent']), 2024]
y_range = [find_new_countObs(x) for x in x_range]
# y_range = [find_new_countObs(x_range[0]) , find_new_countObs(x_range[1])]
line_trace_fig_reg_lineaire = go.Scatter(x=x_range, y=y_range, mode='lines', name='Régression Linéaire', line=dict(color='red'))

fig_reg_lineaire.add_trace(line_trace_fig_reg_lineaire)

# Mettez à jour les étiquettes des axes
fig_reg_lineaire.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023"])
fig_reg_lineaire.update_yaxes(title_text='Nb d\'observations Merle') 


# Ajoutez un point distinct pour l'année 2023
projection_trace = go.Scatter(x=[2023], y=[find_new_countObs(2023)], mode='markers', name='Projection 2023', marker=dict(color='blue', size=10))
fig_reg_lineaire.add_trace(projection_trace)


fig_reg_lineaire.write_image("/app/regression_lineaire_plotly.png")



############
# On tente de faire ça pour tous les mois maintenant, de 2017 à 2022, projection sur 2023

df_obs_anneemois_merle = df_obs_merle.groupBy("yearEvent","monthEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent", "monthEvent")

df_obs_annee_merle_pd = df_obs_anneemois_merle.toPandas()

df_obs_annee_merle_pd['yearEvent'] = df_obs_annee_merle_pd['yearEvent'].astype(float)
df_obs_annee_merle_pd['monthEvent'] = df_obs_annee_merle_pd['monthEvent'].astype(float)
# df_obs_annee_merle_pd['nb_obs_proj'] = None

print(df_obs_annee_merle_pd.head(30))

list_slope = []
list_intercept = []
list_new_obs_count = []



for i in range(1,13):
    df_obs_anneemois_merle_pd = df_obs_annee_merle_pd[df_obs_annee_merle_pd['monthEvent'] == i]
    print(df_obs_anneemois_merle_pd)
    x_mois = df_obs_anneemois_merle_pd['yearEvent']
    y_mois = df_obs_anneemois_merle_pd['nb_obs']

    slope_mois, intercept_mois, r_value_mois, p_value_mois, std_err_mois = stats.linregress(x_mois, y_mois)

    def find_new_countObs_mois(x):
        y = slope_mois*x + intercept_mois
        return y

    list_slope.append(slope_mois)
    list_intercept.append(intercept_mois)
    nouveau_point = find_new_countObs_mois(2023)
    list_new_obs_count.append(nouveau_point)

    # J'alimente mon dataframe de départ

    new_row = pd.DataFrame({'yearEvent' : [2023], 'monthEvent': [i], 'nb_obs': np.nan, 'nb_obs_proj': nouveau_point})

    df_obs_annee_merle_pd = pd.concat([df_obs_annee_merle_pd, new_row], ignore_index=True, sort=False)



    # Representation graphique
    fig_mois = px.scatter(df_obs_anneemois_merle_pd, x='yearEvent', y='nb_obs', title='Régression Linéaire '+ str(i))


    # Ajoutez la ligne de régression linéaire au graphique
    x_range = [min(df_obs_anneemois_merle_pd['yearEvent']), 2024]
    y_range = [find_new_countObs_mois(x) for x in x_range]

    line_trace_mois = go.Scatter(x=x_range, y=y_range, mode='lines', name='Régression Linéaire mois '+ str(i), line=dict(color='red'))

    fig_mois.add_trace(line_trace_mois)

    # Mettez à jour les étiquettes des axes
    fig_mois.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023"])
    fig_mois.update_yaxes(title_text='Nb d\'observations Merle', range=[0, 8000]) 

    # Ajoutez un point distinct pour l'année 2023
    projection_trace = go.Scatter(x=[2023], y=[nouveau_point], mode='markers', name='Projection 2023', marker=dict(color='blue', size=10))
    fig_mois.add_trace(projection_trace)

    nom_image = "/app/regression_lineaire_plotly_mois_" + str(i)+".png"

    fig_mois.write_image(nom_image)







print("list_slope :", list_slope)
print("list_intercept :", list_intercept)
print("list_new_obs_count :", list_new_obs_count)

print(df_obs_annee_merle_pd)

# Créez une nouvelle colonne "yearMonth" en concaténant "yearEvent" et "monthEvent"
# df_obs_annee_merle_pd['yearMonth'] = df_obs_annee_merle_pd.apply(lambda row: f"{int(row['yearEvent'])}-{int(row['monthEvent']):02d}", axis=1)
df_obs_annee_merle_pd['yearMonth'] = df_obs_annee_merle_pd['yearEvent'].astype(int).astype(str) + '-' + df_obs_annee_merle_pd['monthEvent'].astype(int).astype(str).str.zfill(2)


print(df_obs_annee_merle_pd)

# # Convertissez la colonne "yearMonth" en format DateTime
# df_obs_annee_merle_pd['yearMonth'] = pd.to_datetime(df_obs_annee_merle_pd['yearMonth'], format='%Y-%m')

# Créez un graphique de ligne avec Plotly Express
fig_global = px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs', title='Nombre d\'observations par mois')

# Ajoutez une nouvelle série y avec un DataFrame différent ('df2') et une couleur différente
fig_global.add_trace(px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs_proj', title='Nombre d\'observations par mois (Projection 2023)', color_discrete_sequence=['red']).data[0])



fig_global.write_image("/app/projection_globale.png")

fig_global.write_html('/app/projection_globale.html')



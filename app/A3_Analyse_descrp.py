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

### Statistiques exploratoires

#####################################################
#####################################################

###################################################################
# I- Graph du nombre d'observations par Etats, 15 premiers Etats
###################################################################

df_statesTop15 = df_obs.groupby('stateProvince').count()
df_statesTop15_pd = df_statesTop15.toPandas()
df_statesTop15_pd.sort_values(by='count', ascending=False, inplace=True)
df_statesTop15_pd = df_statesTop15_pd.head(15)

plotly_statesTop15 = go.Figure(data=[go.Bar(x=df_statesTop15_pd['stateProvince'], y=df_statesTop15_pd['count'])])

plotly_statesTop15.update_layout(
    title='Diagramme du nombre d\'observations par Etat (top 15)',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_statesTop15.write_image('/app/A3_1_Diagramme_nb_obs_par_Etat_top15.png')


###################################################################
# II- Graph des espèces les plus observées, 15 espèces les + obs.
###################################################################

df_taxonsTop25  = df_obs.groupby('taxonID', 'Name').count()

df_taxonsTop25_pd = df_taxonsTop25.toPandas()
df_taxonsTop25_pd.sort_values(by='count', ascending=False, inplace=True)
df_taxonsTop25_pd = df_taxonsTop25_pd.head(25)

plotly_taxonsTop25 = go.Figure(data=[go.Bar(x=df_taxonsTop25_pd['Name'], y=df_taxonsTop25_pd['count'])])

plotly_taxonsTop25.update_layout(
    title='Graphique en barres du nombre d\'observations par espèce (25 premières espèces)',
    xaxis_title='Espèce',
    yaxis_title='Nombre d\'observations'
)

plotly_taxonsTop25.write_image('/app/A3_2_Diagramme_nb_obs_par_espece_top25.png')



#####################################################
#####################################################

### Statistiques descriptives

#####################################################
#####################################################


##############################################################################
# III- On se concentre sur le merle d'Amérique, taxon 12727
##############################################################################

# Parmi les espèces les plus observées (3ème espèce la plus observée aux US), c'est celle avec la meilleure répartition sur tous les Etats

df_obs_merle = df_obs.where(df_obs['taxonID'] == '12727')


############################################################################## 
# Nombre d'observations par Etats 2017-2022 tout confondus
##############################################################################

df_obs_state_merle = df_obs_merle.groupby('stateProvince').count()
df_obs_merle_pd = df_obs_state_merle.toPandas()
df_obs_merle_pd.sort_values(by='count', ascending=False, inplace=True)

plotly_obs_merle_par_etat = go.Figure(data=[go.Bar(x=df_obs_merle_pd['stateProvince'], y=df_obs_merle_pd['count'])])

plotly_obs_merle_par_etat.update_layout(
    title='Diagramme du nombre d\'observations par état pour le Merle d\'Amérique (2017-2022)',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_obs_merle_par_etat.write_image('/app/A3_4_Diagramme_nb_obs_par_Etat_merle.png')


##############################################################################
# Nombre d'observations par Etats 2017-2022 tout confondus (top 20)
##############################################################################

df_obs_merle_pd = df_obs_merle_pd.head(20)

plotly_merle_par_etat_20 = go.Figure(data=[go.Bar(x=df_obs_merle_pd['stateProvince'], y=df_obs_merle_pd['count'])])

plotly_merle_par_etat_20.update_layout(
    title='Diagramme du nombre d\'observations par état pour le Merle d\'Amérique (2017-2022, 20 premiers Etats)',
    xaxis_title='Etats',
    yaxis_title='Nombre d\'observations'
)

plotly_merle_par_etat_20.write_image('/app/A3_5_Diagramme_nb_obs_par_Etat_merle_top20.png')


##############################################################################
# Nombre d'observations par année Merle d'Amérique tous Etats confondus
##############################################################################

df_obs_annee_merle = df_obs_merle.groupby('yearEvent').count()
df_obs_annee_merle_pd = df_obs_annee_merle.toPandas()
df_obs_annee_merle_pd.sort_values(by='yearEvent', ascending=True, inplace=True)

plotly_obs_merle_par_annee = go.Figure(data=[go.Bar(x=df_obs_annee_merle_pd['yearEvent'],y=df_obs_annee_merle_pd['count'])])

plotly_obs_merle_par_annee.update_layout(
    title='Diagramme du nombre d\'observations par année tous états confondus pour le Merle d\'Amérique',
    xaxis_title='Année',
    yaxis_title='Nombre d\'observations'
)

plotly_obs_merle_par_annee.write_image('/app/A3_6_Diagramme_nb_obs_par_annee_merle_tous_etats.png')


##############################################################################
# Nombre d'observations par année-mois Merle d'Amérique tous Etats confondus
##############################################################################


df_obs_annee_mois_merle  = df_obs_merle.groupby('yearMonthEvent').count()
df_obs_annee_mois_merle_pd = df_obs_annee_mois_merle.toPandas()
df_obs_annee_mois_merle_pd.sort_values(by='yearMonthEvent', ascending=True, inplace=True)

plotly_merle_annee_mois = go.Figure(data=[go.Bar(x=df_obs_annee_mois_merle_pd['yearMonthEvent'], y=df_obs_annee_mois_merle_pd['count'])])

plotly_merle_annee_mois.update_layout(
    title='Graphique en barres du nombre d\'observations par mois pour le Merle d\'Amérique tous etats confondus',
    xaxis_title='Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_merle_annee_mois.write_image('/app/A3_7_Diagramme_nb_obs_par_annee_mois_merle_tous_etats.png')



###############################################################################################
# Carte animée : Evolution nombre d'observations du Merle d'Amérique par année par état
###############################################################################################

df_obs_annee_etat_merle = df_obs_merle.groupBy("yearEvent", "StateCode", "StateProvince").agg(count("*").alias("nb_obs"))

df_obs_annee_etat_merle = df_obs_annee_etat_merle.orderBy(col("yearEvent"))
df_obs_annee_etat_merle_pd = df_obs_annee_etat_merle.toPandas()

fig_merle_annee_etat = px.choropleth(df_obs_annee_etat_merle_pd,
                    locationmode="USA-states",
                    locations="StateCode",
                    color="nb_obs",
                    hover_name="StateProvince",
                    animation_frame="yearEvent",
                    title="Évolution du nombre d'observations de Merle d\'Amérique par État par année aux États-Unis",
                    color_continuous_scale="deep", 
                    range_color=(0, df_obs_annee_etat_merle_pd["nb_obs"].max()))

fig_merle_annee_etat.update_geos(
    scope="usa",
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig_merle_annee_etat.update_layout(
    title='Évolution du nombre d\'observations de Merle d\'Amérique par État par année aux États-Unis',
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title='Nombre d\'observations'),
)

fig_merle_annee_etat.write_html('/app/A3_8_Répartition_nb_obs_par_etat_mois_carte_animee.html')



###########################################################################
# Statistiques descriptives année 2022 (année avec le + d'obs))
###########################################################################

###############################################################################################
# Carte animée : Répartition du nombre d'observations du Merle d'Amérique par état en 2022
###############################################################################################

df_obs_annee_etat_merle_pd = df_obs_annee_etat_merle_pd[df_obs_annee_etat_merle_pd["yearEvent"] == "2022"]

fig_merle_2022 = px.choropleth(df_obs_annee_etat_merle_pd,
                    locationmode="USA-states",
                    locations="StateCode",
                    color="nb_obs", 
                    hover_name="StateProvince",
                    title="Répartition du nombre d'observations de Merle d'Amérique par État aux États-Unis en 2022",
                    color_continuous_scale="deep", 
                    range_color=(0, df_obs_annee_etat_merle_pd["nb_obs"].max())) 

fig_merle_2022.update_geos(
    scope="usa",
    showcoastlines=True,
    coastlinecolor="Black",
    projection_scale=1,
)

fig_merle_2022.update_layout(
    title="Répartition du nombre d'observations de Merle d'Amérique par État aux États-Unis en 2022",
    geo=dict(showcoastlines=True, coastlinecolor="Black"),
    coloraxis_colorbar=dict(title="Nombre d'observations"),
)

fig_merle_2022.write_html('/app/A3_9_Répartition_nb_obs_par_etat_2022_carte_animee.html')

###############################################################################################
# Carte animée : Répartition du nombre d'observations du Merle d'Amérique par mois en 2022
###############################################################################################

df_obs_anneemois_merle = df_obs_merle.groupBy("yearEvent", "yearMonthEvent").agg(count("*").alias("nb_obs"))

df_obs_anneemois_merle_2022 = df_obs_anneemois_merle.where(df_obs_anneemois_merle['yearEvent'] == '2022')

df_obs_anneemois_merle_2022_pd = df_obs_anneemois_merle_2022.toPandas()

plotly_obs_merle_par_mois_2022 = go.Figure(data=[go.Bar(x=df_obs_anneemois_merle_2022_pd['yearMonthEvent'], y=df_obs_anneemois_merle_2022_pd['nb_obs'])])

plotly_obs_merle_par_mois_2022.update_layout(
    title='Diagramme du nombre d\'observations par mois pour le Merle d\'Amérique (2022)',
    xaxis_title='Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_obs_merle_par_mois_2022.write_image('/app/A3_10_Diagramme_nb_obs_par_mois_merle_2022.png')

#############################################################################################################
# Carte animée : Répartition du nombre d'observations du Merle d'Amérique par mois toutes années confondues
#############################################################################################################

df_obs_anneemois_merle_pd = df_obs_anneemois_merle.toPandas()

plotly_obs_merle_par_mois = go.Figure(data=[go.Bar(x=df_obs_anneemois_merle_pd['yearMonthEvent'], y=df_obs_anneemois_merle_pd['nb_obs'])])

plotly_obs_merle_par_mois.update_layout(
    title='Diagramme du nombre d\'observations par mois pour le Merle d\'Amérique 2017 - 2022',
    xaxis_title='Année - Mois',
    yaxis_title='Nombre d\'observations'
)

plotly_obs_merle_par_mois.write_html('/app/A3_11_Diagramme_nb_obs_par_mois_merle.html')


#################################################################################################################################
# Boxplot : Diagramme en boite de la répartition du nombre d'observations du Merle d'Amérique par mois toutes années confondues
#################################################################################################################################


df_obs_mois_merle = df_obs_merle.groupBy("yearEvent", "monthEvent").agg(count("*").alias("nb_obs")).orderBy("monthEvent")

df_obs_mois_merle_pd = df_obs_mois_merle.toPandas()

plotly_obs_merle_par_mois = go.Figure(data=[go.Bar(x=df_obs_mois_merle_pd['monthEvent'], y=df_obs_mois_merle_pd['nb_obs'])])

plotly_obs_merle_par_etat_bloxplot = px.box(df_obs_mois_merle_pd, x="monthEvent", y="nb_obs",
             title="Boxplot du nombre d'observations par mois en 2022",
             labels={"monthEvent": "Mois", "nb_obs": "Nombre d'observations"})

plotly_obs_merle_par_etat_bloxplot.update_traces(boxpoints="all")  # Afficher tous les points, y compris les valeurs aberrantes


plotly_obs_merle_par_etat_bloxplot.write_image('/app/A3_12_Diagramme_nb_obs_par_mois_boxplot_merle.png')

# On en conclu une saisonnalité des observations.
# On affiche le blox plot sur ce graphique


###################################################################################################
# Statistiques descriptives sur le nombre d'observations par mois de Merles d'Amérique en 2022
###################################################################################################

df_obs_annee_etat_merle = df_obs_merle.groupBy("yearEvent", "yearMonthEvent", "StateCode", "StateProvince").agg(count("*").alias("nb_obs"))

df_obs_annee_etat_merle_2022 = df_obs_annee_etat_merle.where(df_obs_annee_etat_merle['yearEvent'] == '2022')

df_obs_annee_etat_merle_2022_pd = df_obs_annee_etat_merle_2022.toPandas()

moyenne_par_mois = df_obs_annee_etat_merle_2022_pd.groupby('yearMonthEvent')['nb_obs'].mean()

print("Description nombre d'observations par mois de Merles d\'Amérique en 2022 :")
print(moyenne_par_mois.describe())

"""
Résultats :
Description nombre d'observations par mois de Merles d'Amérique en 2022 :
count     12.000000
mean      48.079482
std       28.485014
min       19.086957
25%       28.633847
50%       37.573256
75%       61.581020
max      109.080000
Name: nb_obs, dtype: float64

Commentaires des statistiques : 

Une moyenne de 48 observations par mois, mais la plage des observations mensuelles est assez large, 
avec un maximum de 109 et un minimum de 19 : les obsevations sont réparties très inégalement selon les mois, ce qui conforte ce qui a été constaté sur le boxplot


La plage interquartile (IQR) va de 28,63 observations (Q1) à 61,58 observations (Q3), indiquant une grande variabilité entre les mois.

Le quartile inférieur (Q1) est de 28 observations, ce qui signifie que 28 % des couples mois ont moins de 28 observations, 
tandis que le quartile supérieur (Q3) est de 61 observations, montrant qu'un quart des mois ont plus de 61 observations.



"""


###################################################################################################
# Statistiques descriptives sur le nombre d'observations par état de Merles d'Amérique en 2022
###################################################################################################

moyenne_par_etat = df_obs_annee_etat_merle_2022_pd.groupby('StateProvince')['nb_obs'].mean()

print("Description nombre d'observations par Etat de Merles d\'Amérique en 2022 :")
print(moyenne_par_etat.describe())


"""
Résultats :
Description nombre d'observations par Etat de Merles d'Am├®rique en 2022 :
count     50.000000
mean      47.268636
std       51.407329
min        4.200000
25%       12.343750
50%       24.125000
75%       61.625000
max      253.583333
Name: nb_obs, dtype: float64

Commentaires des statistiques : 


"""





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

df_obs_annee_etat_merle = df_obs_merle.groupBy("yearEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent")
df_obs_annee_etat_merle_pd = df_obs_annee_etat_merle.toPandas()

df_obs_annee_etat_merle_pd['yearEvent'] = df_obs_annee_etat_merle_pd['yearEvent'].astype(float)

# Détermination de l'axe des x :
x = df_obs_annee_etat_merle_pd['yearEvent']
# print('x (yearEvent) :')
# print(x)
# print(" ")

# Détermination de l'axe des y :
y = df_obs_annee_etat_merle_pd['nb_obs']
# print('y (nb_obs) :')
# print(y)
# print(" ")

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

line_trace_fig_reg_lineaire = go.Scatter(x=x_range, y=y_range, mode='lines', name='Régression Linéaire', line=dict(color='red'))

fig_reg_lineaire.add_trace(line_trace_fig_reg_lineaire)

fig_reg_lineaire.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023"])
fig_reg_lineaire.update_yaxes(title_text='Nb d\'observations Merle') 

projection_trace = go.Scatter(x=[2023], y=[fonctionRegLineaire(2023)], mode='markers', name='Projection 2023', marker=dict(color='blue', size=10))
fig_reg_lineaire.add_trace(projection_trace)

fig_reg_lineaire.write_image("/app/A3_13_Regression_lineaire_plotly.png")


##########################################################################################
# Inference du nombre d'observations de Merles d'Amériques aux US en 2023 mois par mois
##########################################################################################

############
# On fait cela pour chacun des mois maintenant, de 2017 à 2022, puis projection sur chacun des mois de 2023

df_obs_anneemois_merle = df_obs_merle.groupBy("yearEvent","monthEvent").agg(count("*").alias("nb_obs")).orderBy("yearEvent", "monthEvent")

df_obs_annee_merle_pd = df_obs_anneemois_merle.toPandas()

df_obs_annee_merle_pd['yearEvent'] = df_obs_annee_merle_pd['yearEvent'].astype(float)
df_obs_annee_merle_pd['monthEvent'] = df_obs_annee_merle_pd['monthEvent'].astype(float)


print(df_obs_annee_merle_pd.head(30))

list_new_obs_count = []

for i in range(1,13):
    df_obs_anneemois_merle_pd = df_obs_annee_merle_pd[df_obs_annee_merle_pd['monthEvent'] == i]
    print(df_obs_anneemois_merle_pd)
    x_mois = df_obs_anneemois_merle_pd['yearEvent']
    y_mois = df_obs_anneemois_merle_pd['nb_obs']

    slope_mois, intercept_mois, r_value_mois, p_value_mois, std_err_mois = stats.linregress(x_mois, y_mois)

    def fonctionRegLineaire_mois(x):
        y = slope_mois*x + intercept_mois
        return y

    nouveau_point = fonctionRegLineaire_mois(2023)
    list_new_obs_count.append(nouveau_point)

    # J'alimente mon dataframe de départ, avec une nouvelle colonne : nb_obs_proj
    new_row = pd.DataFrame({'yearEvent' : [2023], 'monthEvent': [i], 'nb_obs': np.nan, 'nb_obs_proj': nouveau_point})
    df_obs_annee_merle_pd = pd.concat([df_obs_annee_merle_pd, new_row], ignore_index=True, sort=False)

    # Representation graphique
    fig_mois = px.scatter(df_obs_anneemois_merle_pd, x='yearEvent', y='nb_obs', title='Régression Linéaire '+ str(i))


    # Ajoutez la ligne de régression linéaire au graphique
    x_range_mois = [min(df_obs_anneemois_merle_pd['yearEvent']), 2024]
    y_range_mois = [fonctionRegLineaire_mois(x) for x in x_range_mois]

    line_trace_mois = go.Scatter(x=x_range_mois, y=y_range_mois, mode='lines', name='Régression Linéaire mois '+ str(i), line=dict(color='red'))

    fig_mois.add_trace(line_trace_mois)

    # Mettez à jour les étiquettes des axes
    fig_mois.update_xaxes(title_text='Année', tickangle=45, tickvals=["2017","2018","2019","2020","2021","2022","2023"])
    fig_mois.update_yaxes(title_text='Nb d\'observations Merle', range=[0, 8000]) 

    # Ajoutez un point distinct pour l'année 2023
    projection_trace = go.Scatter(x=[2023], y=[nouveau_point], mode='markers', name='Projection 2023', marker=dict(color='blue', size=10))
    fig_mois.add_trace(projection_trace)

    nom_image = "/app/A3_14_regression_lineaire_plotly_mois_" + str(i)+".png"

    fig_mois.write_image(nom_image)


df_obs_annee_merle_pd['yearMonth'] = df_obs_annee_merle_pd['yearEvent'].astype(int).astype(str) + '-' + df_obs_annee_merle_pd['monthEvent'].astype(int).astype(str).str.zfill(2)


fig_global = px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs', title='Nombre d\'observations par mois')

fig_global.add_trace(px.line(df_obs_annee_merle_pd, x='yearMonth', y='nb_obs_proj', title='Nombre d\'observations par mois (Projection 2023)', color_discrete_sequence=['red']).data[0])


fig_global.write_image("/app/A3_15_Regression_linéaire_projection_globale.png")
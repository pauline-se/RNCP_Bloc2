cd C:\Users\Pauline\Documents\Formation\RNCP\Bloc2\Docker3


docker exec spark_bloc23 python /app/A1_Analyse_donnees.py > ./app/logsA1.txt
docker exec spark_bloc23 python /app/A2_Data_prep.py > ./app/logsA2.txt
docker exec spark_bloc23 python /app/A3_Analyse_descrp.py > ./app/logsA3.txt

docker exec spark_bloc23 python /app/B1_Analyse_donnees_monde.py > ./app/logsB1.txt
docker exec spark_bloc23 python /app/B2_Data_prep_monde.py > ./app/logsB2.txt
docker exec spark_bloc23 python /app/B3_Analyse_descrp_monde.py > ./app/logsB3.txt


pause
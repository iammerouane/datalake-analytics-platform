#Create hadoop base image
cd hadoop/base && docker build . -t hadoop-base && cd ../..
#Create spark base image
cd spark/base && docker build . -t spark-base && cd ../..
#Build Docker images
docker compose build
#Start all containers
docker compose up -d


echo "Chargement des fichiers dans HDFS ..."
echo "(assurez vous d'avoir mis les fichiers csv dans le dossier data)"
sleep 60

# Chargement des fichiers CSV dans HDFS
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/bikes.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/bikeshops.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/orders.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/customers.csv /

# Récupération de l'adresse de la BD MYSQL
ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mysql)

# Execution du script PySpark
docker exec -it jupyter-notebook python run.py $ip

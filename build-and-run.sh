#Create hadoop base image
cd hadoop/base && docker build . -t hadoop-base && cd ../..
#Create spark base image
cd spark/base && docker build . -t spark-base && cd ../..
#Build Docker images
docker compose build
#Start all containers
docker compose up -d

sleep 30

docker exec -it hadoop-namenode hdfs --loglevel FATAL dfs -copyFromLocal /data/bikes.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/bikeshops.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/orders.csv /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/bikes.xlsx /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/bikeshops.xlsx /
docker exec -it hadoop-namenode hdfs dfs -copyFromLocal /data/orders.xlsx /

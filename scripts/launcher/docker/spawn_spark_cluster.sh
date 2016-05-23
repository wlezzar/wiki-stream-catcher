# Create the network
docker network create spark_cluster

# Launch the master
docker run --rm -p 8080:8080 -it -e SPARK_MASTER_IP=master --name master --net=spark_cluster yaronr/spark
../sbin/start-master.sh && tail -f /usr/spark-1.5.2-bin-hadoop2.6/logs/spark-*

# Launch the slaves
docker run --rm -it --name slave01 --net=spark_cluster yaronr/spark
docker run --rm -it --name slave02 --net=spark_cluster yaronr/spark
docker run --rm -it --name slave03 --net=spark_cluster yaronr/spark
docker run --rm -it --name slave04 --net=spark_cluster yaronr/spark

# For each worker, execute this : 
../sbin/start-slave.sh spark://master:7077 -c 1 -m 1g && tail -f /usr/spark-1.5.2-bin-hadoop2.6/logs/spark-*

# Optional : to open a driver
docker run --rm -p 4041:4040 -it --name driver --net=spark_cluster yaronr/spark
./spark-shell --master spark://master:7077
sc.parallelize(1 to 1000).groupBy(_ % 2 == 0).mapValues(_.reduce(_+_)).collect

# To visualise the network
sudo wget -O /usr/local/bin/scope https://git.io/scope
sudo chmod a+x /usr/local/bin/scope
sudo scope launch
-> connect to : localhost:4040

weave launch
eval $(weave env)

docker run --rm -p 8080:8080 -it -e SPARK_MASTER_IP=master --name master yaronr/spark ../sbin/start-master.sh && tail -f /usr/spark-1.5.2-bin-hadoop2.6/logs/spark-*
echo 'SPARK_MASTER_IP=master' > ../conf/spark-env.sh && ../sbin/start-master.sh
tail -f /usr/spark-1.5.2-bin-hadoop2.6/logs/spark-*

docker run --rm -it --name slave01 yaronr/spark
docker run --rm -it --name slave02 yaronr/spark
docker run --rm -it --name slave03 yaronr/spark
docker run --rm -it --name slave04 yaronr/spark

echo 'SPARK_WORKER_MEMORY=1g' >> ../conf/spark-env.sh && \
echo 'SPARK_WORKER_CORES=1' >> ../conf/spark-env.sh && \
../sbin/start-slave.sh spark://master:7077 && \
tail -f /usr/spark-1.5.2-bin-hadoop2.6/logs/spark-*

eval $(weave env)
docker run --rm -p 4040:4040 -it --name driver yaronr/spark
./spark-shell --master spark://master:7077

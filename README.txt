# start vm
sudo docker run -i -t -v /home/demo/zhi:/mnt tristartom/hadoop-hbase-ubuntu:ycsb /bin/bash

# go to home and copy code
cd
cp -r /mnt/code .

# set environment variables
export JAVA_HOME=/usr/lib/jvm/jdk1.7.0_07
export HADOOP_PREFIX=/root/app/hadoop-2.6.0
export PATH=$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin:$JAVA_HOME/bin
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

# install maven
sudo apt-get update
sudo apt-get install maven

# compile
hadoop com.sun.tools.javac.Main Profiler.java
jar cf profiler.jar Profiler*.class
cd statsd-jvm-profiler
mvn package

# add java agent arguments to hdfs environment variable
vi $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
# add the following line AT THE END
# export HADOOP_DATANODE_OPTS="-javaagent:/root/code/statsd-jvm-profiler/target/statsd-jvm-profiler-0.7.2-SNAPSHOT.jar=profilers=CCTProfiler,server=localhost,port=55555,packageWhitelist=org.apache $HADOOP_DATANODE_OPTS"

# add java agent argument to hbase environment variable
vi /root/app/hbase-0.99.2/conf/hbase-env.sh
# add the following line AT THE END
# export HBASE_OPTS="-javaagent:/root/code/statsd-jvm-profiler/target/statsd-jvm-profiler-0.7.2-SNAPSHOT.jar=profilers=CCTProfiler,server=localhost,port=55555,packageWhitelist=org.apache $HBASE_OPTS"

# start ssh daemon
sudo service ssh start

# start hdfs                       
cd ~/app/hadoop-2.6.0
bin/hdfs namenode -format
sbin/start-dfs.sh
# verify hdfs runs successfully
grep -w "FATAL\|ERROR" logs/*

# run hbase                         
cd ~/app/hbase-0.99.2
bin/start-hbase.sh
bin/hbase shell
# inside hbase shell
create 'tablename', 'cf'
# verify hdfs runs successfully
grep -w "FATAL\|ERROR" logs/*

# then run ycsb
cd ~/app/ycsb-0.1.4
java -cp /root/app/ycsb-0.1.4/core/lib/core-0.1.4.jar:/root/app/ycsb-0.1.4/hbase-binding/lib/hbase-binding-0.1.4.jar:/root/app/ycsb-0.1.4/hbase-binding/conf com.yahoo.ycsb.Client -db com.yahoo.ycsb.db.HBaseClient -P workloads/workloada -p columnfamily=cf -p recordcount=1 -p threadcount=1 -t -p table=tablename

# copy profiler mapreduce input
hdfs dfs -cp profiler pin
# run profiler's mapreduce
hadoop jar profiler.jar Profiler pin pout
# see the top 10 high-latency contexts from the output
hdfs dfs -get pout pout
head pout/*

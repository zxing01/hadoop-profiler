### Software Installation

1. Install Java JDK and set `JAVA_HOME` environment variable accordingly.

2. Install Maven with:
```
sudo apt-get update
sudo apt-get install maven
```

3. Download the latest distribution of Hadoop from http://hadoop.apache.org. Set the environment varialbe `HADOOP_PREFIX` to the downloaded directory.

4. Download this context-profiling package. Set the environment varialbe `PROFILER_HOME` to the downloaded directory.

5. Set `PATH` and `HADOOP_CLASSPATH` with the following commands:
```
export PATH=$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin:$JAVA_HOME/bin
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
```

6. In $PROFILER_HOME, compile the the examples and profiler with the following command:
```
hadoop com.sun.tools.javac.Main GoodWordCount.java
jar cf good.jar GoodWordCount*.class
```
```
hadoop com.sun.tools.javac.Main BadWordCount.java
jar cf bad.jar BadWordCount*.class
```
```
hadoop com.sun.tools.javac.Main Profiler.java
jar cf profiler.jar Profiler*.class
```
```
cd statsd-jvm-profiler
mvn package
```

### Two-node Hadoop Cluster Setup

Assume the user name is username.

1. Suppose the master machine's IP is 192.168.0.1, and the slave machine's IP is 192.168.0.2. Add the following to /etc/hosts on **both** machines:
```
192.168.0.1	master
192.168.0.2	slave
```

2. Set up passphraseless ssh connection to localhost on the **master**:
```
ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
```

3. Set up master's passphraseless ssh connection to slave on the **master**:
```
ssh-copy-id -i ~/.ssh/id_rsa.pub username@slave
```

4. Put the following content in file `$HADOOP_PREFIX/etc/hadoop/masters` on **both** machines:
```
master  
```

5. Put the following content in file `$HADOOP_PREFIX/etc/hadoop/slaves` on **both** machines:
```
master
slave
```

6. Edit `$HADOOP_PREFIX/etc/hadoop/core-site.xml` by adding (modifying) the following on **both** machines:
```
<property>
        <name>fs.defaultFS</name>
        <value>hdfs://master/</value>
</property>
```

7. Edit `$HADOOP_PREFIX/etc/hadoop/yarn-site.xml` by adding (modifying) the following on **both** machines:
```
<property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
</property>
```
```
<property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
</property>
```

8. Edit `$HADOOP_PREFIX/etc/hadoop/mapred-site.xml` by adding (modifying) the following on the **master** (change `$PROFILER_HOME` to its value):
```
<property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
</property>
```
```
<property>
        <name>mapreduce.jobtracker.address</name>
        <value>master</value>
</property>
```
```
<property>
        <name>mapreduce.map.memory.mb</name>
        <value>2048</value>
</property>
```
```
<property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>4096</value>
</property>
```
```
<property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx1536m -javaagent:$PROFILER_HOME/statsd-jvm-profiler/target/statsd-jvm-profiler-0.7.2-SNAPSHOT.jar=profilers=CCTProfiler,server=localhost,port=55555,packageWhitelist=org.apache.hadoop.mapreduce:org.apache.hadoop.mapred</value>
</property>
```
```
<property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx3072m -javaagent:$PROFILER_HOME/statsd-jvm-profiler/target/statsd-jvm-profiler-0.7.2-SNAPSHOT.jar=profilers=CCTProfiler,server=localhost,port=55555,packageWhitelist=org.apache.hadoop.mapreduce:org.apache.hadoop.mapred</value>
</property>
```

### Profiler Testing

Assume the user name is username.

1. If the cluster is running, stop it first with the following commands:
```
stop-dfs.sh
stop-yarn.sh
```

2. Format the namenode of HDFS:
```
hdfs namenode -format
```

3. Restart the cluster: 
```
start-dfs.sh
start-yarn.sh
```

4. Create the HDFS directories:
```
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/username
hdfs dfs -mkdir /user/username/profiler # output directory for profiler's Java agents
```

5. Copy the input files to HDFS:
```
hdfs dfs -put $HADOOP_PREFIX/etc/hadoop input
```

6. Run an example job (using BadWordCount as example):
```
hadoop jar $PROFILER_HOME/bad.jar BadWordCount input output
```

7. Copy the profiler's Java agents' output files to another directory on HDFS:
```
hdfs dfs -cp profiler pin
```

8. Run the profiler's MapReduce jobs to generate the final results:
```
hadoop jar $PROFILER_HOME/profiler.jar Profiler pin pout
```

9. Copy the profiler's output to local file system:
```
hdfs dfs -get pout pout
```

10. Check the top 10 high-latency function contexts:
```
head pout/*
```
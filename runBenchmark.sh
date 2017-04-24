#!/bin/sh

currentDir=`pwd`
clusternodes=clusternodes
dataDir="$HOME/repos/git/mjs/diss/eval/data"

master=`cat $clusternodes | grep -v -e "^#" | head -n 1`


# clean old data
echo "Cleaning up old remote LRB stats"
ssh $master "bash -l -c 'cleanLrbStats.sh'"


# start monitoring
echo "Starting up Nmon at..."
for worker in `cat $clusternodes | grep -v -e "^#" | tail -n +2`
do
  echo "$worker"
  ssh $worker "bash -l -c 'startNmon.sh'"
done



# run benchmark
echo "Starting Topology"
classpath="queries/lrb/target/LinearRoadBenchmark.jar"
classpath="$classpath:$HOME/.m2/repository/net/sf/jopt-simple/jopt-simple/5.0.2/jopt-simple-5.0.2.jar"
classpath="$classpath:$HOME/.m2/repository/org/apache/storm/storm-core/0.9.3/storm-core-0.9.3.jar"
classpath="$classpath:$HOME/.m2/repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar"
classpath="$classpath:$HOME/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar"
classpath="$classpath:$HOME/.m2/repository/org/yaml/snakeyaml/1.11/snakeyaml-1.11.jar"
classpath="$classpath:$HOME/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar"

java \
  -cp $classpath \
  de.hub.cs.dbis.lrb.queries.LinearRoad \
  --input /data/mjsax/LR3hours.txt \
  --acc-output /data/mjsax/lrb-result/acc.out \
  --toll-output /data/mjsax/lrb-result/toll.out \
  --toll-ass-output /data/mjsax/lrb-result/tollass.out \
  --measureThroughput 1000 \
  --measureLatency 100 \
  --realtime \


runtime=11400 # 3h 10min
#runtime=120
echo "Benchmark started at `date`. Sleeping for runtime of $runtime seconds."
sleep $runtime


# stop topology
echo "Stopping Topology"
java \
  -cp $classpath \
  de.hub.cs.dbis.lrb.queries.LinearRoad \
  --stop Linear-Road-Benchmark


# stop monitoring
echo "Stopping Nmon at..."
for worker in `cat $clusternodes | grep -v -e "^#" | tail -n +2`
do
  echo "$worker"
  ssh $worker "bash -l -c 'stopNmon.sh'"
done


# collect data
echo "Collecting data..."

echo "Nmon:"
for worker in `cat $clusternodes | grep -v -e "^#" | tail -n +2`
do
  # pre-process remotely before copying
  echo "Preprocessing data at $worker"
  ssh $worker "bash -l -c 'filterNmon.sh'"
  scp "$worker:nmon-stats/*.cpu nmon-stats/*.netIO" $dataDir
done

echo "LRB stats:"
echo "Copy all LRB stats to $master"
ssh $master "bash -l -c 'collectLrbStats.sh'"
cd $dataDir
./collectLrbStats.sh $master


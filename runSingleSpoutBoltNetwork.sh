#!/bin/sh

workerConfig=`grep "TOPOLOGY_WORKERS" micro.cfg | cut -d':' -f 2 | tr -d ' '`
if [ $workerConfig -eq 1 ]
then
  echo "Invalid config for TOPOLOGY_WORKERS in micro.cfg. Must not be 1. Canceling benchmark."
  exit -1
fi

batchsize=$1
recordsize=$2

currentDir=`pwd`
clusternodes=clusternodes
dataDir="$HOME/repos/git/mjs/diss/eval/micro/singleSpoutBoltNetwork"
mkdir -p $dataDir

master=`cat $clusternodes | grep -v -e "^#" | head -n 1`


# clean old data
echo "Cleaning up old remote micro stats"
ssh $master "bash -l -c 'cleanLrbStats.sh'"


# start monitoring
echo "Starting up Nmon at..."
for worker in `cat $clusternodes | grep -v -e "^#" | tail -n +2`
do
  echo "$worker"
  ssh $worker "bash -l -c 'startNmon.sh'"
done



# run benchmark
echo "Starting micro benchmark"
classpath="monitoring/target/Microbenchmarks.jar"
classpath="$classpath:$HOME/.m2/repository/net/sf/jopt-simple/jopt-simple/5.0.2/jopt-simple-5.0.2.jar"
classpath="$classpath:$HOME/.m2/repository/org/apache/storm/storm-core/0.9.3/storm-core-0.9.3.jar"
classpath="$classpath:$HOME/.m2/repository/org/slf4j/slf4j-api/1.7.5/slf4j-api-1.7.5.jar"
classpath="$classpath:$HOME/.m2/repository/commons-lang/commons-lang/2.5/commons-lang-2.5.jar"
classpath="$classpath:$HOME/.m2/repository/org/yaml/snakeyaml/1.11/snakeyaml-1.11.jar"
classpath="$classpath:$HOME/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar"

java \
  -cp $classpath \
  de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.SpoutBenchmark \
  --batchSize $batchsize \
  --recordSize $recordsize \
  --measureThroughput 1000 \
  --measureLatency 100 \



runtime=120
echo "Microbenchmark started at `date`. Sleeping for runtime of $runtime seconds."
sleep $runtime


# stop topology
echo "Stopping Topology"
java \
  -cp $classpath \
  de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.SpoutBenchmark \
  --stop SpoutMicroBenchmark


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
echo "Copy all micro stats to $master"
ssh $master "bash -l -c 'collectLrbStats.sh'"
cd $dataDir
./../../collectStats.sh $master

for file in `ls *.cpu *.netIO *.throughput *.latencies`
do
  mv $file $file.$batchsize-$recordsize
done


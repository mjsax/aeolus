#!/bin/sh

# comma separated lists
batchsizes=$1
recordsizes=$2
ingestionrates=$3

batchSizeCnt=0
recordSizeCnt=0
ingestionRateCnt=0
for batchsize in `echo $batchsizes | sed "s/,/ /g"`
do
  batchSizeArguments="$batchSizeArguments --batchSize $batchsize"
  batchSizeCnt=$((batchSizeCnt + 1))
done
for recordsize in `echo $recordsizes| sed "s/,/ /g"`
do
  recordSizeArguments="$recordSizeArguments --recordSize $recordsize"
  recordSizeCnt=$((recordSizeCnt + 1))
done
for ingestionrate in `echo $ingestionrates | sed "s/,/ /g"`
do
  ingestionRateArguments="$ingestionRateArguments --ingestionRate $ingestionrate"
  ingestionRateCnt=$((ingestionRateCnt + 1))
done

if [ $batchSizeCnt -ne $recordSizeCnt ]
then
  echo "Invalid Arguments: Number of batch sizes must be equal to number of record sizes."
  exit -1
fi
if [ $ingestionRateCnt -ne 0 ] && [ $ingestionRateCnt -ne $batchSizeCnt ]
then
  echo "Invalid Arguments: Number of ingestion rates must be zero or equal to number of batch and record sizes."
  exit -1
fi

workerConfig=`grep "TOPOLOGY_WORKERS" micro.cfg | cut -d':' -f 2 | tr -d ' '`
if [ $workerConfig -lt $((batchSizeCnt + 1)) ]
then
  echo "Invalid config for TOPOLOGY_WORKERS in micro.cfg. Must be at least $((batchSizeCnt + 1)). Canceling benchmark."
  exit -1
fi

currentDir=`pwd`
clusternodes=clusternodes
dataDir="$HOME/repos/git/mjs/diss/eval/micro/effectiveBatchSize"
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
  de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.EffectiveBatchSizeBenchmark \
  $batchSizeArguments \
  $recordSizeArguments \
  $ingestionRateArguments \
  --measureThroughput 1000 \
  --measureLatency 100 \



runtime=120
echo "Microbenchmark started at `date`. Sleeping for runtime of $runtime seconds."
sleep $runtime


# stop topology
echo "Stopping Topology"
java \
  -cp $classpath \
  de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.EffectiveBatchSizeBenchmark \
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
  mv $file $file.$batchsizes-$recordsizes-$ingestionrates
done


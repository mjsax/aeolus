#!/bin/bash

if [ -z $1 ] || [ -z $2 ] || [ -z $3 ] || [ -z $4 ]
then
  echo "missing parameter: ./runSingleBatchingBenchmark <sampleInterval> <numberOfSamples> <dataRate> <batchSize>"
  echo "  <sampleInterval> in seconds"
  echo "  <dataRate> in tuples per seconds (tps)"
  echo "  <batchSize> in #tuples (0 => disable batching)"
  exit -1
fi

# configure sampling interval and duration (= sampleInterval * numberOfSamples)
sampleInterval=$1 # in seconds
numberOfSamples=$2

# with network interface should be monitored
networkInterface=eth0

counterFile=microbenchmarks
outputFile=$counterFile-MeasureOutputRate
remoteDir=/tmp
localDir=bb-res

# maven version
aeolusVersion=1.0-SNAPSHOT

############################################
# do no change any value below

# on remote machine (ie, where spout is executed)
sampleOutputFile=$remoteDir/$outputFile.nmon
# on local machine
resultOutputFile=$localDir/$outputFile-$3-$4.res
tmpFile=/tmp/aeolus-single-benchmark.tmp


# deploy topology and retrieve host name of machine executing spout
storm jar target/monitoring-$aeolusVersion-microbenchmarks.jar \
    -Dlogback.configurationFile=src/main/resources/logback.xml \
    -Daeolus.microbenchmarks.dataRate=$3 \
    -Daeolus.microbenchmarks.batchSize=$4 \
    de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.MeasureOutputDataRate \
  | grep -e "^Aeolus.MeasureOutputDataRate" > $tmpFile

spoutHost=`cat $tmpFile | grep -e "^Aeolus.MeasureOutputDataRate.spoutHost=" | cut -d"=" -f 2`
spoutStatsHost=`cat $tmpFile | grep -e "^Aeolus.MeasureOutputDataRate.spoutStatsHost=" | cut -d"=" -f 2`
spoutStatsFile=`cat $tmpFile | grep -e "^Aeolus.MeasureOutputDataRate.spoutStatsFile=" | cut -d"=" -f 2`
sinkStatsHost=`cat $tmpFile | grep -e "^Aeolus.MeasureOutputDataRate.sinkStatsHost=" | cut -d"=" -f 2`
sinkStatsFile=`cat $tmpFile | grep -e "^Aeolus.MeasureOutputDataRate.sinkStatsFile=" | cut -d"=" -f 2`

if [ -z $spoutHost ]
then
  echo "Could not determine spout host."
  exit -1
fi
if [ -z $spoutStatsHost ]
then
  echo "Could not determine spout-stats host."
  exit -1
fi
if [ -z $spoutStatsFile ]
then
  echo "Could not determine spout-stats file."
  exit -1
fi
if [ -z $sinkStatsHost ]
then
  echo "Could not determine sink-stats host."
  exit -1
fi
if [ -z $sinkStatsFile ]
then
  echo "Could not determine sink-stats file"
  exit -1
fi

rm $tmpFile



# start sampling with nmon and write result to file
ssh $spoutHost "nmon -F $sampleOutputFile -s$sampleInterval -c$numberOfSamples"
# wait until all samples are collected
sleep $((sampleInterval * numberOfSamples))



# extract relevant data from nmon-result-file and pipe into local file
ssh $spoutHost \
  "index=1
   for token in \`cat $sampleOutputFile | grep -e '^NET,' | head -n 1 | sed 's/ /-/g' | sed 's/,/ /g'\`
   do
     if [ \$token = $networkInterface-write-KB/s ];
     then
       break
     fi
     index=\$((\$index + 1))
   done
   cat $sampleOutputFile | grep -e '^NET,' | cut -d, -f 6
   rm $sampleOutputFile
" > $resultOutputFile
scp $spoutStatsHost:$spoutStatsFile $localDir/$counterFile-spout-$3-$4.stats
scp $sinkStatsHost:$sinkStatsFile $localDir/$counterFile-sink-$3-$4.stats



# stop topology
storm jar target/monitoring-$aeolusVersion-microbenchmarks.jar \
    -Dlogback.configurationFile=src/main/resources/logback.xml \
    de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.MeasureOutputDataRate --kill \
  > /dev/null


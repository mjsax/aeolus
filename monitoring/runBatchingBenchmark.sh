#!/bin/bash

# executes the micro-benchmark "MeasureOutputRate" for the specified data rates and spout output batch sizes
# -> the spcified "sampling interval" and "number of samples" determine the runtime of a single experiment

outputRates="200000 400000 600000 800000 1000000 1200000 1400000 1600000 1800000 2000000"
batchSizes="0 1 2 3 4 5 6 7 8 9 10"
# runtime: sampleInterval * numberOfSamples
sampleInterval=1 # in seconds
numberOfSamples=30



resultDir=spout-batching
aeolusVersion=1.0-SNAPSHOT

##################################################
tmpFile=/tmp/batching-benchmark.tmp

for outputRate in $outputRates
do
  for batchSize in $batchSizes
  do
    echo "Run with output rate of $outputRate tps and batches of $batchSize tuples."
    localDir=results/$resultDir/rate-$outputRate-bS-$batchSize

    jarCommand="target/monitoring-$aeolusVersion-microbenchmarks.jar
                -Dlogback.configurationFile=src/main/resources/logback.xml \
                -Daeolus.microbenchmarks.reportingInterval=$((sampleInterval * 1000)) \
                -Daeolus.microbenchmarks.dataRate=$outputRate \
                -Daeolus.microbenchmarks.batchSize=$batchSize \
                de.hub.cs.dbis.aeolus.monitoring.microbenchmarks.MeasureOutputDataRate"

    # run benchmark
    bash runSingleBenchmark.sh "$jarCommand" $sampleInterval $numberOfSamples $localDir \
      | grep -e "^Aeolus.MeasureOutputDataRate" > $tmpFile

    # get benchmark specific meta data
    spoutHost=`cat $tmpFile | grep "spoutHost" | cut -d= -f 2`
    sinkHost=`cat $tmpFile | grep "sinkHost" | cut -d= -f 2`
    spoutStatsHost=`cat $tmpFile | grep "spoutStatsHost" | cut -d= -f 2`
    spoutStatsFile=`cat $tmpFile | grep "spoutStatsFile" | cut -d= -f 2`
    sinkStatsHost=`cat $tmpFile | grep "sinkStatsHost" | cut -d= -f 2`
    sinkStatsFile=`cat $tmpFile | grep "sinkStatsFile" | cut -d= -f 2`
    rm $tmpFile

    # get Aeolus monitoring data
    echo "spout=$spoutHost" > $localDir/usedHosts
    echo "sink=$sinkHost" >> $localDir/usedHosts
    scp $spoutStatsHost:$spoutStatsFile $localDir/aeolus-benchmark-spout.stats
    scp $sinkStatsHost:$sinkStatsFile $localDir/aeolus-benchmark-sink.stats

    # need to sleep before next run, because Storm takes some while to clear the killed topology
    sleep 30
  done
done


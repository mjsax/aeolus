#!/bin/bash

if [ -z "$1" ] || [ -z $2 ] || [ -z $3 ] || [ -z $4 ]
then
  echo "missing parameter: ./runSingleBenchmark <jar-command> <sampleInterval> <numberOfSamples> <localResDir>"
  echo "  <jar-command>		jar-name (plus -D flags) plus main-class"
  echo "  <sampleInterval>	in seconds"
  echo "  <numberOfSamples>"
  echo "  <localResDir>		local directory for statistic files"
  exit -1
fi

# configure sampling interval and duration (= sampleInterval * numberOfSamples)
jarCommand=$1
sampleInterval=$2 # in seconds
numberOfSamples=$3
localDir=$4

##################################################
sampleOutputFile=/tmp/aeolus.nmon

# deploy topology
storm jar $jarCommand

# start sampling with nmon and write result to file
for host in `cat hosts`
do
  ssh $host "nmon -F $sampleOutputFile -s$sampleInterval -c$numberOfSamples"
done

# wait until all samples are collected
sleep $((sampleInterval * numberOfSamples))



# get topology id
id=`storm list | grep ACTIVE | cut -d" " -f 1`
# stop topology
storm kill $id



# get and process nmon file
mkdir -p $localDir
bash processNmonFile.sh $sampleOutputFile $localDir $sampleInterval



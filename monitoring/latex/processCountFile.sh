#!/bin/bash

filePrefix=$1
outputRate=$2
batchSize=$3
operatorId=$4 

tmpFile=/tmp/aeolus.eval.tmp
statsFile=$filePrefix-$operatorId-$outputRate-${batchSize}.stats

 # get output streams for operator
for line in `cat $statsFile`
do
  echo $line | cut -d, -f 2 >> $tmpFile
done
sort $tmpFile -u > ${tmpFile}.unique

# for each output stream, extact data for it
for stream in `cat ${tmpFile}.unique`
do
  resultFile=$filePrefix-$operatorId-$stream-$outputRate-${batchSize}.res
  echo $stream > $resultFile
  grep -e ",$stream," $statsFile | cut -d, -f 4 >> $resultFile
done

rm $tmpFile
rm ${tmpFile}.unique


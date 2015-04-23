#!/bin/bash

filePrefix=$1
outputRate=$2
batchSize=$3
operatorId=$4 

tmpFile=/tmp/aeolus.eval.tmp
statsFile=$filePrefix-$operatorId-$outputRate-${batchSize}.stats

firstTS=`head -n 1 $statsFile | cut -d, -f 1`

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

  # header line
  echo "ts $stream" > $resultFile
  for line in `grep -e ",$stream," $statsFile`
  do
    value=`echo $line | cut -d, -f 4`
    TS=`echo $line | cut -d, -f 1`
    # normalize to seconds
    TS=$(( (TS - firstTS) / 1000 ))
    # data line
    echo "$TS $value" >> $resultFile
  done
done

rm $tmpFile
rm ${tmpFile}.unique


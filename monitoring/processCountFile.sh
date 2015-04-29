#!/bin/bash

# processes a Aeolus throughput count file
#  -> splits the file input multiple files (one output file for each logical stream)

if [ -z $1 ]
then
  echo "missing parameter: ./processCountFile.sh <filename>"
  echo "  <filename>	the name of the Aeolus throughput count file"
  exit -1
fi

file=$1

##################################################
tmpFile=/tmp/aeolus-eval.tmp
statsFile=$file

firstTS=`head -n 1 $statsFile | cut -d, -f 1`

# get output streams for operator
for line in `cat $statsFile`
do
  echo $line | cut -d, -f 2 >> $tmpFile
done
sort $tmpFile -u > $tmpFile.unique


# for each output stream, extact data for it
for stream in `cat $tmpFile.unique`
do
  resultFile=`echo $statsFile | cut -d. -f 1`-$stream.res

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
rm $tmpFile.unique


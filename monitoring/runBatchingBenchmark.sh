#!/bin/bash

for outputRate in 200000 400000 600000 800000 1000000 1200000 1400000 1600000 1800000 2000000
do
  for batchSize in 0 1 2 3 4 5 6 7 8 9 10
  do
    echo "Run with output rate of $outputRate tps and batches of $batchSize tuples."
    bash runSingleBatchingBenchmark.sh 1 30 $outputRate $batchSize
    if [ ! $? -eq 0 ]
    then
      exit -1
    fi
    # need to sleep, because killing topology takes some time
    sleep 10
  done
done


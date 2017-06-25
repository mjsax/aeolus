#!/bin/bash

# single Spout-Bolt in-memory
for batchsize in 1 5 10 50 100 500 1000
do
  for recordsize in 100 10240 102400
  do
    echo "Runnig Spout microbenchmark with batch size $batchsize and record size $recordsize"
    bash runSingleSpoutBoltInMemory.sh $batchsize $recordsize
  done
done

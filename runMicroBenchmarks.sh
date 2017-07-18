#!/bin/bash

# single Spout-Bolt single process
if [ ] # empty==false
then
  echo "Runing Spout-Bolt single process"
  for batchsize in 1 10 100 1000 10000
  do
    for recordsize in 100
    do
      echo "Runnig Spout microbenchmark with batch size $batchsize and record size $recordsize"
      bash runSingleSpoutBoltSingleProcess.sh $batchsize $recordsize
    done
  done
fi

# single Spout-Bolt single process (shuffle)
if [ ] # empty==false
then
  echo "Runing Spout-Bolt single process"
  for batchsize in 1 10 100 1000 10000
  do
    for recordsize in 100
    do
      echo "Runnig Spout microbenchmark with batch size $batchsize and record size $recordsize"
      bash runSingleSpoutBoltShuffle.sh $batchsize $recordsize
    done
  done
fi

# single Spout-Bolt
if [ ] # empty==false
then
  echo "Runing Spout-Bolt (multiple workers)"
  for batchsize in 1 10 100 1000 10000
  do
    for recordsize in 100
    do
      echo "Runnig Spout microbenchmark with batch size $batchsize and record size $recordsize"
      bash runSingleSpoutBolt.sh $batchsize $recordsize
    done
  done
fi

# single Spout-Bolt network
if [ ] # empty==false
then
  echo "Runing Spout-Bolt network"
  for batchsize in 1 10 100 1000 10000
  do
    for recordsize in 100
    do
      echo "Runnig Spout microbenchmark with batch size $batchsize and record size $recordsize"
      bash runSingleSpoutBoltNetwork.sh $batchsize $recordsize
    done
  done
fi

# single Spout-Bolt network with data rates
if [ 1 ] # empty==false
then
  echo "Runing Spout-Bolt network (data rates"
  for batchsize in 1 10 100 1000 10000
  do
    for recordsize in 100
    do
      for ingestionrate in 1000 10000 100000 1000000
      do
        echo "Runnig Spout microbenchmark with batch size $batchsize, record size $recordsize, and ingestion rate $ingestionrate"
        bash runSingleSpoutBoltRate.sh $batchsize $recordsize $ingestionrate
      done
    done
  done
fi



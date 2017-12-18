#!/bin/bash

# single Spout-Bolt single process
if [ ] # empty==false | anything-else==true
then
  echo "Running Spout-Bolt single process"
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
if [ ] # empty==false | anything-else==true
then
  echo "Running Spout-Bolt single process"
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
if [ ] # empty==false | anything-else==true
then
  echo "Running Spout-Bolt (multiple workers)"
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
if [ ] # empty==false | anything-else==true
then
  echo "Running Spout-Bolt network"
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
if [ 1 ] # empty==false | anything-else==true
then
  echo "Running Spout-Bolt network (data rates)"
  for recordsize in 100
  do
    for batchsize in 1 10 100 1000 10000
    do
      for ingestionrate in 1000 10000 100000 1000000 "inf"
      do
        echo "Runnig Spout microbenchmark with batch size $batchsize, record size $recordsize, and ingestion rate $ingestionrate"
        bash runSingleSpoutBoltRate.sh $batchsize $recordsize $ingestionrate
      done
    done
  done
fi


# effective input batch size: multiple-Spouts--single-Bolt network with data rates
if [ 1 ] # empty==false | anything-else==true
then
  echo "Running effective batch size"
  for recordsizes in "100,100"
  do
    for batchsizes in "1,1" "1,5" "1,10" "5,10"
    do
      for ingestionrates in "500000,500000" "10000,90000" "90000,10000" "30000,70000" "70000,30000"
      do
        echo "Runnig Effective-Batch-Size microbenchmark"
        bash runEffectiveBatchSize.sh "$batchsizes" "$recordsizes" "$ingestionrates"
      done
    done
  done
fi


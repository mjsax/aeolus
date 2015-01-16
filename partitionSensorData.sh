#!/bin/bash

# schema: sid, ts, x, y, z, |v|, |a|, vx, vy, vz, ax, ay, az
inputFile=full-game
outputFilePrefix=sensor
# by default, do not overwirte output files automatically
forceOutput=0

# see config.txt
sensorIDs="4 8 10 12 13 14 97 98 47 16 49 88 19 52 53 54 23 24 57 58 59 28 61 62 99 100 63 64 65 66 67 68 69 38 71 40 73 74 75 44 105 106"


### check command line arguments ###
flagInputFile="-i"
flagPrefix="-o"
flagForce="-f"

# parameters available ?
if [ $# -ge 1 ]
then
  if [ $1 == "-h" ] || [ $1 == "--help" ] || [ $1 == "--h" ] || [ $1 == "-help" ]
  then
    echo "./partitionSensorData.sh [$flagInputFile <inputFile>] [$flagPrefix <prefix>] [$flagForce] | [-h]"
    echo "  $flagInputFile <inputFile>     use input file <inputFile> instead of default input file '$inputFile'"
    echo "  $flagPrefix <prefix>        set prefix for output files: <prefix>-[sendorId] (default prefix: $outputFilePrefix)"
    echo "  $flagForce                 overwrite output files"
    echo "  -h                 displays this help"
    echo "Flags must be used in correct order."
    exit 0
  fi

  if [ $1 == $flagInputFile ]
  then
    # value missing?
    if [ $# -eq 1 ]
    then
      echo "Error: parameter <inputFile> is missing"
      echo "./partitionSensorData.sh $flagInputFile <inputFile>"
      exit -1
    fi
    
    inputFile=$2
    shift 2
  fi
  
  if [ $# -ge 1 ] && [ $1 == $flagPrefix ]
  then
    # value missing?
    if [ $# -eq 1 ]
    then
      echo "Error: parameter <prefix> is missing"
      echo "./partitionSensorData.sh $flagPrefix <prefix>"
      exit -1
    fi

    outputFilePrefix=$2
    shift 2
  fi

  if [ $# -ge 1 ] && [ $1 == $flagForce ]
  then
    forceOutput=1
    shift 1
  fi

  # additional arguments present
  if [ $# -gt 1 ]
  then
    echo "Unknow flag '$2'. Use -h to get help."
    exit -1
  fi
fi
### end check command line arguments ###





# check if input file is there
if [ ! -f $inputFile ]
then
  echo "Error: input file '$inputFile' not found."
  exit -1
fi

# if flag --force is not set, check if any output file exits
# abort with error if any output file is found
if [ ! $forceOutput ]
then
  error=0
  for ID in $sensorIDs
  do
    outputFile=$outputFilePrefix-$ID
    if [ -f  $outputFile ]
    then
      echo "Error: output file '$outputFile' exists already."
    fi

    # if error occurs, write help message and exit
    if [ $error ]
    then
      echo "Use flag --force to overwrite output files."
      exit -1
    fi
  done
fi

# lets split the input file
for ID in $sensorIDs
do
  grep "^$ID," $inputFile > $outputFilePrefix-$ID
done


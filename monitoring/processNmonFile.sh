#!/bin/sh

# processes nmon files on multiple hosts and extracts
#  - network input (KB/s)
#  - network output (KB/s)
#  - CPU utilization (in %) for user, sys, and wait
#
# the location of the nmon file must be the same on each remote machine

if [ -z $1 ] || [ -z $2 ] || [ -z $3 ]
then
  echo "missing parameter: ./processNmonFile.sh <remoteNmonFile> <localResDir> <sampleInterval>"
  echo "  <remoteNmonFile>	path to nmon file in all remote machines"
  echo "  <localResDir>		path to local result files directory"
  echo "  <sampleInterval>	used nmon sample interval in seconds"
  exit -1
fi

remoteNmonFile=$1
localResultDirectory=$2
sampleInterval=$3

##################################################
networkInterface=eth0

nmonTmp=/tmp/nmon.tmp
resultTmp=/tmp/result.tmp

for host in `cat hosts`
do
  scp $host:$remoteNmonFile $nmonTmp

  # each token specifies (tokens are separated by #)
  #  - a line prefix in the nmon file (eg, NET)
  #  - the column name in the nmon file of the value that should be extracted (eg, eth0-write-KB/s)
  #  - a file suffix to be used for the result file
  for statistic in NET\#$networkInterface-write-KB/s\#nwOut \
                   NET\#$networkInterface-read-KB/s\#nwIn \
                   CPU_ALL\#User%\#CPUUser \
                   CPU_ALL\#Sys%\#CPUSys \
                   CPU_ALL\#Wait%\#CPUWait
  do
    statsPrefix=`echo $statistic | cut -d# -f 1`
    columnName=`echo $statistic | cut -d# -f 2`
    fileSuffix=`echo $statistic | cut -d# -f 3`

    resultFile=$localResultDirectory/aeolus-benchmark-$host-$fileSuffix.res

    # get field index of column name
    index=1
    for token in `cat $nmonTmp | grep -e "^$statsPrefix," | head -n 1 | sed 's/ /-/g' | sed 's/,/ /g'`
    do
      if [ $token = $columnName ]
      then
        break
      fi
      index=$((index + 1))
    done
    # get data for statistic (filter by line, extract column); head included
    cat $nmonTmp | grep -e "^$statsPrefix," | cut -d, -f $index > $resultTmp

    # post-processing to add timestampe to each line
    TS=__HEADER__
    for line in `cat $resultTmp`
    do
      if [ $TS = __HEADER__ ]
      then
        # header line
        echo "ts $line" > $resultFile
        TS=0
      else
        # data line
        echo "$TS $line" >> $resultFile
        TS=$((TS + sampleInterval))
      fi
    done

    rm $resultTmp
  done

  rm $nmonTmp
done


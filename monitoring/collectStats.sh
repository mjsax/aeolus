#!/bin/bash

statsPath=/tmp

#for file in 
#do
#  rm $file 2> /dev/null
  for host in `cat hosts`
  do
    ssh $host "ls /tmp/*.stats"
#    rm /tmp/file 2> /dev/null
#    scp $host:$dataPath/$file /tmp/$file
#    cat /tmp/$file >> $file
  done
#done


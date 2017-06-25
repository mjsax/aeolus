#!/bin/bash

statsPath=/tmp

for host in `cat hosts`
do
  echo $host
  echo "rm $statsPath/*.stats at"
  ssh $host "rm $statsPath/*.stats"
done


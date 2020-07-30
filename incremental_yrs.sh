#!/bin/bash

for i in $(seq 1987 2007);
do
  match="((1987"
  for ((j=1987; j<$i; j++));
  do
    match=$match")|("$j
  done
  match=$match"))*"
  time bash mr_sorted.sh $1 input/$match /out_$j
done
#!/bin/bash

for i in $(seq 1987 2007);
do
  match="{x"
  for ((j=1987; j<$i; j++));
  do
    match=$match","$j.csv.bz2
  done
  match=$match"}"
  j="$(($j-1))"
  echo "Running through year: $j"
  { time bash timing.sh $1 input/$match /out_$j ; } 2>> timing_$1.txt
done
#!/bin/bash

yarn jar njit-644-airlines.jar $1 /$2 /unsort_temp
yarn jar njit-644-airlines.jar SortDescending /unsort_temp /$3
hdfs dfs -rm -r /unsort_temp

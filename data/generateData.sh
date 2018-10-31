#!/bin/bash

mkdir -p csv/
for IX in {0..1000000};do echo ${IX}","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000)); done > csv/A.csv & 
for IX in {0..1000000};do echo ${IX}","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000)); done > csv/B.csv &
for IX in {0..1000000};do echo ${IX}","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000))","$(($RANDOM % 1000000)); done > csv/C.csv &


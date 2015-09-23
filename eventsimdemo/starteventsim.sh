#!/bin/bash

cd /home/vmuser/host/repos/eventsim

#bin/eventsim -c "examples/example-config.json" --from 365 \
#--nusers 50 --growth-rate 0.01 \
#--kafkaBrokerList broker1:9092 --kafkaTopic eventsim \
#--nocontinuous  data/fake.json

bin/eventsim \
-c "examples/example-config.json" \
--start-time "`date +"%Y-%m-%dT%H:%M:%S"`" \
--end-time "2016-09-16T17:05:53" \
--nusers 20000 --growth-rate 0.01 \
--kafkaBrokerList broker1:9092 --kafkaTopic eventsim \
--continuous \
data/fake.json
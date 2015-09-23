#!/bin/bash

cd "$(dirname "$(readlink -f "$0")")"

mvn exec:java -Dexec.mainClass="jmxmetric.JMXMetricProducer" -Dexec.args="metricstopic localhost:9999 broker1:9092 5000"
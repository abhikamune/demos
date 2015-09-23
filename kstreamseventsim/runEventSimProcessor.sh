#!/bin/bash

cd "$(dirname "$(readlink -f "$0")")"

mvn exec:java -Dexec.mainClass="eventsim.EventsimProcessorJob" -Dexec.args="10000 60"
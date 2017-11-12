#!/usr/bin/env bash

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

export EXECUTION_PLAN_DIR="$base_dir/deploy/plan"
mkdir -p $EXECUTION_PLAN_DIR

exec $(dirname $0)/deploy/bin/run-class.sh streambench.BenchmarkApplication --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/config/streambench.properties

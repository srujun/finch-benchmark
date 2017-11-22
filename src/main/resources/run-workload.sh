#!/bin/bash

if [ -z "$1" ]
  then
    echo "Need argument --config-path"
    exit 1
fi

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

export EXECUTION_PLAN_DIR="$base_dir/plan"
mkdir -p $EXECUTION_PLAN_DIR

[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

exec $(dirname $0)/run-class.sh streambench.BenchmarkApplicationMain  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory "$@"

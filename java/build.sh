#!/bin/bash
# Compile the Spark jobs and make java/build/cloud-bigdata-project.jar
set -e
cd "$(dirname "$0")"
rm -rf build && mkdir -p build/classes
javac --release 11 -cp "$SPARK_HOME/jars/*" -d build/classes $(find src -name '*.java')
jar cf build/cloud-bigdata-project.jar -C build/classes .
echo "built java/build/cloud-bigdata-project.jar"

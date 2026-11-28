#!/bin/bash
# build java/build/app.jar
set -e
cd "$(dirname "$0")"
rm -rf build && mkdir -p build/classes
javac --release 11 -cp "$SPARK_HOME/jars/*" -d build/classes src/compression/*.java
jar cf build/app.jar -C build/classes .
echo "built java/build/app.jar"

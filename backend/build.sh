#!/usr/bin/env bash
# Compile all AgentGrid sources into backend/build/classes.
set -euo pipefail

cd "$(dirname "$0")"

rm -rf build/classes
mkdir -p build/classes

javac -Xlint:all -d build/classes $(find src/main/java -name '*.java')

# Resources (the document corpus) are loaded from the classpath by every node.
if [ -d src/main/resources ]; then
    cp -R src/main/resources/. build/classes/
fi

echo "Compiled to $(pwd)/build/classes"

#!/usr/bin/env bash
# Compile all AgentGrid sources into backend/build/classes.
set -euo pipefail

cd "$(dirname "$0")"

rm -rf build/classes
mkdir -p build/classes

javac -Xlint:all -d build/classes $(find src/main/java -name '*.java')

echo "Compiled to $(pwd)/build/classes"

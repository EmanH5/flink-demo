#!/usr/bin/env bash
set -euo pipefail

# Install Apache Flink 1.17.1 (Scala 2.12)

if [ -d "flink-1.17.1" ]; then
  echo "Flink 1.17.1 already present at $(pwd)/flink-1.17.1"
  exit 0
fi

echo "Downloading Flink 1.17.1..."
curl -sSL https://archive.apache.org/dist/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz -o flink-1.17.1.tgz

echo "Extracting..."
tar -xzf flink-1.17.1.tgz
rm -f flink-1.17.1.tgz

echo "Flink installed at: $(pwd)/flink-1.17.1"
# (Optional) show version
./flink-1.17.1/bin/flink --version || true

#!/usr/bin/env bash
set -euo pipefail

# ---- Packages ----
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk maven curl gnupg

# Make Java 17 the default (helpful because the base image is Java 11)
if [ -x /usr/sbin/update-alternatives ]; then
  sudo update-alternatives --set java  /usr/lib/jvm/java-17-openjdk-amd64/bin/java || true
  sudo update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac || true
fi
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# ---- Confluent CLI ----
sudo mkdir -p /etc/apt/keyrings
curl -s https://packages.confluent.io/confluent-cli/deb/archive.key | sudo gpg --dearmor -o /etc/apt/keyrings/confluent-cli.gpg
echo "deb [signed-by=/etc/apt/keyrings/confluent-cli.gpg] https://packages.confluent.io/confluent-cli/deb stable main" | sudo tee /etc/apt/sources.list.d/confluent-cli.list >/dev/null
sudo apt-get update && sudo apt-get install -y confluent-cli

# ---- Flink 2.0.0 (binary) ----
if [ ! -d "flink-2.0.0" ]; then
  curl -LO https://downloads.apache.org/flink/flink-2.0.0/flink-2.0.0-bin-scala_2.12.tgz
  tar -xzf flink-2.0.0-bin-scala_2.12.tgz
  rm flink-2.0.0-bin-scala_2.12.tgz
fi

# ---- Minimal conf tweaks for Gitpod ----
# Bind UI to 0.0.0.0 so the preview can reach it
sed -i 's@^#\?rest.bind-address:.*@rest.bind-address: 0.0.0.0@g' flink-2.0.0/conf/flink-conf.yaml

# Reasonable local defaults
grep -q '^parallelism.default' flink-2.0.0/conf/flink-conf.yaml || echo 'parallelism.default: 2' >> flink-2.0.0/conf/flink-conf.yaml
grep -q '^taskmanager.numberOfTaskSlots' flink-2.0.0/conf/flink-conf.yaml || echo 'taskmanager.numberOfTaskSlots: 2' >> flink-2.0.0/conf/flink-conf.yaml

echo "Java: $(java -version 2>&1 | head -n1)"
echo "Maven: $(mvn -v | head -n1)"
echo "Flink installed at: $PWD/flink-2.0.0"

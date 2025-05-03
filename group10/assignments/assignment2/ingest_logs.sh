#!/bin/bash

# Define HDFS directories
LOGS_DIR="/raw/logs"
META_DIR="/raw/metadata"

# Create HDFS directories if they don't exist
hdfs dfs -mkdir -p $LOGS_DIR
hdfs dfs -mkdir -p $META_DIR

# Copy local files to HDFS
hdfs dfs -put -f user_log.csv $LOGS_DIR/
hdfs dfs -put -f meta_data.csv $META_DIR/

echo "Data ingested into HDFS at $LOGS_DIR and $META_DIR"


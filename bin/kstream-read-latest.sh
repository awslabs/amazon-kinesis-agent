#!/bin/bash

# Stream name parameter (mandatory)
streamName="$1"
if [ -z "$streamName" ]; then
    echo "Usage: $0 <stream-name> [latest records number to display]"
    exit 1
fi

# Records number to return parameter (optional, default 10)
recordsNumber=$2
if [ -z $recordsNumber ]; then
    recordsNumber=10
fi
recordsNumber=$(( recordsNumber ))

iteratorType="LATEST"
iteratorType="TRIM_HORIZON"

# Get shard ID, first shard in the list
shardId=$(aws kinesis describe-stream \
    --stream-name $streamName | grep SHARDS | awk '{print $2}')
if [ -z "$shardId" ]; then
    echo "Shard not found: [$shardId]"
    exit 2
fi

# Get shard iterator for latest entries
shardIter=$(aws kinesis get-shard-iterator \
    --stream-name "$streamName" \
    --shard-id "$shardId" \
    --shard-iterator-type "$iteratorType")

# Read latest records
tmpFile=$(mktemp)
kMaxReadDepth=1
kRecordsRead=0
while [ $kRecordsRead -lt $recordsNumber ] && [ $kMaxReadDepth -gt 0 ]; do
    aws kinesis get-records --shard-iterator "$shardIter" --limit $recordsNumber > "$tmpFile"
    foundRowsNum=$(cat "$tmpFile" | awk 'NR==1{print $1; exit}')
    shardIter=$(cat "$tmpFile" | awk 'NR==1{print $2; exit}')
    cat "$tmpFile" | awk '{print $3}' | while read line; do
        echo -n ">> $kRecordsRead: "
        echo $line | base64 --decode
        kRecordsRead=$(( kRecordsRead + 1 ))
    done
    kMaxReadDepth=$(( kMaxReadDepth - 1 ))
done

rm -f "$tmpFile"

echo ""


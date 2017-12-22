#!/bin/bash

# Log files are in /var/log/aws-kinesis-agent/

JAVA_START_HEAP="256m"
JAVA_MAX_HEAP="512m"
OOME_ARGS_DEFAULT="\"/bin/kill -9 %p\""
OOME_ARGS="-XX:OnOutOfMemoryError=${OOME_ARGS:-$OOME_ARGS_DEFAULT}"

JAVA_DIR="/usr/share/java"
LIB_DIR="/usr/share/aws-kinesis-agent/lib"
#CLASSPATH="$JAVA_DIR"/*:"$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d:):"$CLASSPATH"
CLASSPATH="$LIB_DIR":$(find "$LIB_DIR" -type f -name \*.jar | paste -s -d:):"$CLASSPATH"

JAVACMD="java"
JVM_ARGS="-server -Xms${JAVA_START_HEAP} -Xmx${JAVA_MAX_HEAP} $JVM_ARGS"

MAIN_CLASS="com.amazon.kinesis.streaming.agent.Agent"

exec $JAVACMD $JVM_ARGS "$OOME_ARGS" \
  -cp "$CLASSPATH" \
  $MAIN_CLASS "$@"

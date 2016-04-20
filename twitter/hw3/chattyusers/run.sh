#!/bin/sh

export JAVA_HOME=/usr/java/default
export HADOOP_HOME=/opt/hadoop

ANALYTIC_NAME=chattyusers
ANALYTIC_DIR=$HOME/analytics/$ANALYTIC_NAME
LOG_DIR=$ANALYTIC_DIR/log
LOG_FILE=$LOG_DIR/`date +%Y%m%d_%H%M%S`.log

mkdir -p $LOG_DIR

PIG_SCRIPT=$ANALYTIC_DIR/${ANALYTIC_NAME}.pig
OUTPUT=/analytics/$ANALYTIC_NAME/`date -u +%Y/%m/%d/%H`
INPUT=/in/tweets/`date -u -d '1 hour ago' +%Y/%m/%d/%H`

RUN_CMD="/opt/pig/bin/pig -p input=$INPUT -p output=$OUTPUT -f $PIG_SCRIPT"
echo $RUN_CMD > $LOG_FILE
$RUN_CMD >> $LOG_FILE 2>&1


#!/bin/sh

cd /home/$USER/analytics/reverseindex

LOG_DIR=/home/$USER/analytics/reverseindex/log
LOG_FILE=$LOG_DIR/`date +%Y%m%d_%H%M%S`.log

mkdir -p $LOG_DIR

PIG_SCRIPT=reverseindex.pig
OUTPUT=/analytics/reverseindex/`date -u +%Y/%m/%d/%H`
INPUT=/in/lol/match/`date -u -d '1 hour ago' +%Y/%m/%d/%H`

RUN_CMD="/opt/pig/bin/pig -p input=$INPUT -p output=$OUTPUT -f $PIG_SCRIPT"
echo $RUN_CMD > $LOG_FILE
$RUN_CMD &>> $LOG_FILE


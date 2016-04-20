#/bin/sh

export JAVA_HOME=/usr/java/default
export HADOOP_HOME=/opt/hadoop

ANALYTIC_NAME=accumulo-ingest
ANALYTIC_DIR=$HOME/analytics/$ANALYTIC_NAME
LOG_DIR=$ANALYTIC_DIR/log
LOG_FILE=$LOG_DIR/`date +%Y%m%d_%H%M%S`.log

INSTANCE="default"
ZOOKEEPERS="localhost:2181"
USER="root"
PASSWORD="secret"
TWEET_DIR=/in/tweets/`date -u -d '2 hour ago' +%Y/%m/%d/%H`
HASHTAGS_DIR=/analytics/tophashtags/`date -u -d '1 hour ago' +%Y/%m/%d/%H`
INDEX_DIR=/analytics/tweetindex/`date -u -d '1 hour ago' +%Y/%m/%d/%H`
POP_USERS_DIR=/analytics/popularusers/`date -u -d '1 hour ago' +%Y/%m/%d/%H`

mkdir -p $LOG_DIR

JAR=$ANALYTIC_DIR/accumulo-ingest.jar

RUN_CMD="hadoop jar $JAR -i $INSTANCE -z $ZOOKEEPERS -u $USER -p $PASSWORD -t $TWEET_DIR -n $INDEX_DIR -h $HASHTAGS_DIR -o $POP_USERS_DIR"
echo $RUN_CMD > $LOG_FILE
$RUN_CMD >> $LOG_FILE 2>&1


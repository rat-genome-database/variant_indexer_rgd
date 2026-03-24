#!/usr/bin/env bash
. /etc/profile
APPNAME=variant_indexer_rgd
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=jthota@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=jthota@mcw.edu
fi
cd $APPDIR
pwd
DB_OPTS="-Dspring.config=/home/rgddata/pipelines/properties/default_db2.xml"
LOG4J_OPTS="-Dlog4j2.configurationFile=file://$APPDIR/properties/log4j2.xml"
java $DB_OPTS $LOG4J_OPTS -jar lib/${APPNAME}-all.jar "$@" 2>&1 | tee run.log
#mailx -s "[$SERVER] Variant Indexer Pipeline OK" $EMAIL_LIST < run.log

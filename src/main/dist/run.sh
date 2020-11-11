#!/usr/bin/env bash
. /etc/profile
APPNAME=variant_indexer_rgd
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=jthota@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=mtutaj@mcw.edu,jthota@mcw.edu,jdepons@mcw.edu
fi
cd $APPDIR
pwd
DB_OPTS="-Dspring.config=/home/rgddata/pipelines/properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export VARIANT_INDEXER_RGD_OPTS="$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" | tee run.log
#mailx -s "[$SERVER] Variant Indexer Pipeline OK" $EMAIL_LIST < run.log

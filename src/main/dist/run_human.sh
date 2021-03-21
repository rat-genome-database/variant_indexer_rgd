APPDIR=/home/rgddata/pipelines/variant_indexer_rgd
COMMAND=update
PROCESS=$1
MAPKEY=$2
SPECIESTYPEKEY=3
STORETYPE=db
ENV=test

#echo "STARTED processing variants of chromosome 1"
#echo "$APPDIR/run.sh reindex $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY 1"
#$APPDIR/run.sh reindex $ENV $PROCESS $SPECIESTYPEKEY $MAPKEY 1
#echo "********END processing variants of chromosome 1*******"
j=0
$APPDIR/run.sh reindex dev variants 1 38 X

for chr in {1..23..1}
do

        echo $chr
        $APPDIR/run.sh update dev variants 1 38 $chr


done
#echo "CHR X======================"
#$APPDIR/run.sh update dev variants 3 60 X
echo "CHR Y======================"
$APPDIR/run.sh update dev variants 1 38 Y
echo "CHR MT======================"
$APPDIR/run.sh update dev variants 1 38 MT

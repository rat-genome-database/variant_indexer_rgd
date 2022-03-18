APPDIR=/home/rgddata/pipelines/variant_indexer_rgd

ENV=$1
MAPKEY=$2
SPECIESTYPEKEY=$3

$APPDIR/run.sh reindex $ENV variants $SPECIESTYPEKEY $MAPKEY 1 18

echo "CHR X======================"
$APPDIR/run.sh update $ENV variants $SPECIESTYPEKEY $MAPKEY X
echo "CHR Y======================"
$APPDIR/run.sh update $ENV variants $SPECIESTYPEKEY $MAPKEY Y
echo "CHR MT======================"
$APPDIR/run.sh update $ENV variants $SPECIESTYPEKEY $MAPKEY MT

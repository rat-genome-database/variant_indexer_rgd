APPDIR=/home/rgddata/pipelines/variant_indexer_rgd

ENV=$1
MAPKEY=$2
SPECIESTYPEKEY=$3

$APPDIR/run.sh reindex $ENV variants $SPECIESTYPEKEY $MAPKEY X

for chr in {1..1..1}
do

        echo $chr
        $APPDIR/run.sh update $ENV variants $SPECIESTYPEKEY $MAPKEY $chr


done

echo "CHR Y======================"
$APPDIR/run.sh update $ENV variants $SPECIESTYPEKEY $MAPKEY Y
echo "CHR MT======================"
$APPDIR/run.sh $ENV variants $SPECIESTYPEKEY $MAPKEY MT

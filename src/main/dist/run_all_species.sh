APPDIR=/home/rgddata/pipelines/variant_indexer_rgd

ENV=$1
$APPDIR/rat_all_chr.sh $ENV 3 372
$APPDIR/rat_all_chr.sh $ENV 3 360
$APPDIR/rat_all_chr.sh $ENV 3 60
$APPDIR/rat_all_chr.sh $ENV 3 70

$APPDIR/dog_all_chr.sh $ENV 6 631

$APPDIR/pig_all_chr.sh $ENV 9 910
$APPDIR/pig_all_chr.sh $ENV 9 911

$APPDIR/greenMonkey_all_chr.sh $ENV 13 1311

$APPDIR/mouse_all_chr.sh $ENV 2 35

$APPDIR/human_all_chr.sh $ENV 1 38
$APPDIR/human_all_chr.sh $ENV 1 17


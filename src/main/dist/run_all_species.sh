APPDIR=/home/rgddata/pipelines/variant_indexer_rgd

ENV=$1
$APPDIR/rat_all_chr.sh $ENV 372 3
$APPDIR/rat_all_chr.sh $ENV 360 3
$APPDIR/rat_all_chr.sh $ENV 60 3
$APPDIR/rat_all_chr.sh $ENV 70 3

$APPDIR/dog_all_chr.sh $ENV 631 6

$APPDIR/pig_all_chr.sh $ENV 910 9
$APPDIR/pig_all_chr.sh $ENV 911 9

$APPDIR/greenMonkey_all_chr.sh $ENV 1311 13

$APPDIR/mouse_all_chr.sh $ENV 35 2
$APPDIR/mouse_all_chr.sh $ENV 239 2


$APPDIR/human_all_chr.sh $ENV 38 1
$APPDIR/human_all_chr.sh $ENV 17 1


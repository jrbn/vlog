#!/bin/sh

# This script only works on DAS5, because the location of the dataset is on das5
rm -rf  /var/scratch/uji300/vlog/materialization_claros

#echo "========================"
#../vlog load -i /var/scratch/jurbani/datasets/rdf/LUBM/lubm_1000/ -o indexDir_1000 2>&1
#echo "========================"

echo "========================"
../vlog mat -e edb-claros.conf --storemat_path /var/scratch/uji300/vlog/materialization_claros --storemat_format files --rules $HOME/vlog/examples/rules/aaai2016/Claros_LE.dlog 2>&1
echo "========================"

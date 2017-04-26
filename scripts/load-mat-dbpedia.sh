#!/bin/sh

# This script only works on DAS5, because the location of the dataset is on das5
rm -rf  /var/scratch/uji300/vlog/materialization_dbpedia

#echo "========================"
#../vlog load -i /var/scratch/jurbani/datasets/rdf/LUBM/lubm_1000/ -o indexDir_1000 2>&1
#echo "========================"

echo "========================"
../vlog mat -e edb-dbpedia.conf --storemat_path /var/scratch/uji300/vlog/materialization_dbpedia --storemat_format files --rules $HOME/vlog/examples/rules/aaai2016/DBpedia_L.dlog 2>&1
echo "========================"

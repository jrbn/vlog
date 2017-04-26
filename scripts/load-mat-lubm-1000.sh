#!/bin/sh

# This script only works on DAS5, because the location of the dataset is on das5
rm -rf  /var/scratch/uji300/vlog/materialization_lubm1_1000

# load the database in the ttl directory, creating the index in "indexDir".
#echo "========================"
#../vlog load -i /var/scratch/jurbani/datasets/rdf/LUBM/lubm_1000/ -o indexDir_1000 2>&1
#echo "========================"

#materialize using the LUBM1_LE rules and store the result in "materialization_lubm1".
echo "========================"
../vlog mat -e edb-lubm.conf --storemat_path /var/scratch/uji300/vlog/materialization_lubm1_1000 --storemat_format files --rules $HOME/vlog/examples/rules/aaai2016/LUBM1_LE.dlog 2>&1
echo "========================"

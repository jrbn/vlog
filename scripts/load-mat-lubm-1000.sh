#!/bin/sh

rm -rf indexDir materialization_lubm1

# load the database in the ttl directory, creating the index in "indexDir".
echo "========================"
../vlog load -i /Users/unmesh/kbs/lubm_1000/ -o indexDir-1000 2>&1
echo "========================"

#materialize using the LUBM1_LE rules and store the result in "materialization_lubm1".
echo "========================"
../vlog mat -e edb.conf --storemat_path materialization_lubm1_1000 --storemat_format files --decompressmat true --rules dlog/LUBM1_LE.dlog 2>&1
echo "========================"

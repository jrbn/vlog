# lubm
gnuplot -c hist.gnuplot lubm-1000-training-ablation-result.csv
gnuplot -c hist.gnuplot lubm-1000-test-typewise-accuracy.csv
epstopdf lubm-1000-training-ablation-result.csv.eps 
epstopdf lubm-1000-test-typewise-accuracy.csv.eps 

# dbpedia
gnuplot -c hist.gnuplot dbpedia-training-ablation-result.csv
gnuplot -c hist.gnuplot dbpedia-test-typewise-accuracy.csv
epstopdf dbpedia-training-ablation-result.csv.eps
epstopdf dbpedia-test-typewise-accuracy.csv.eps

# claros
gnuplot -c hist.gnuplot claros-training-ablation-result.csv
gnuplot -c hist.gnuplot claros-test-typewise-accuracy.csv
epstopdf claros-training-ablation-result.csv.eps
epstopdf claros-test-typewise-accuracy.csv.eps


gnuplot -c stacked-hist.gnuplot online-time.dat
epstopdf online-time.dat.eps

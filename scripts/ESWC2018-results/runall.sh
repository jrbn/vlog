python ablation-sklearn.py --train_data dbpedia-training.csv --test_data dbpedia-test.csv
python ablation-sklearn.py --train_data claros-training.csv --test_data claros-test.csv
python ablation-sklearn.py --train_data lubm-1000-training.csv --test_data lubm-1000-test.csv
python compareTimings.py --predFile claros-test-predictions.log --statsFile claros-test.stats
python compareTimings.py --predFile dbpedia-test-predictions.log --statsFile dbpedia-test.stats
python compareTimings.py --predFile lubm-1000-test-predictions.log --statsFile lubm-1000-test.stats

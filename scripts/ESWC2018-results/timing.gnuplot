#!/usr/bin/gnuplot

set term postscript eps enhanced color
set output ARG1.'.eps'

datafile = ARG1
firstrow = system('head -1 '.datafile)
set title "Timing performance of Online prediction"
set auto x
set yrange [100:3000]
set style data histogram
set style histogram cluster gap 1
set style fill solid border -1
set boxwidth 0.9
set xtic rotate by -45 scale 0
#set bmargin 10 
plot ARG1 using 2:xtic(1) ti col, '' u 3 ti col, '' u 4 ti col


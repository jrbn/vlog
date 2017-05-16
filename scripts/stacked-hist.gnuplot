#!/usr/bin/gnuplot

set term postscript eps enhanced color
set output ARG1.'.eps'

datafile = ARG1
firstrow = system('head -1 '.datafile)

#set colors classic
#set title "LUBM -1000 Query Types and Algorithm choices"
set title ARG1
set key top left outside horizontal autotitle columnhead

set xtics scale 3,2 rotate by 67 out nomirror
set xtics out offset 0,-3.0
#set xtics 0,50,500 out nomirror
#set autoscale x
#show xtics
set ytics out nomirror

set style fill solid border -1
set boxwidth 0.5 relative
set style data histograms
set style histogram rowstacked
set xlabel word(firstrow, 1) offset 0,-4.0

plot ARG1 using 2, '' using 3:xtic(1),
#set xrange [100:500];
#replot
#plot  ARG1 using 2, '' using 3:xtic(1)

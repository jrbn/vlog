#!/usr/bin/gnuplot

set term postscript eps enhanced color font ",30"

datafile = ARG1
outfile = system('echo '.datafile.' | cut -d. -f1')
outfile = outfile.'.eps'
set output outfile
firstrow = system('head -1 '.datafile)

#set colors classic
#set title "LUBM -1000 Query Types and Algorithm choices"
#set title ARG1
set key top left outside horizontal autotitle columnhead font ", 30"

set xtics font ", 30"
set ytics font ", 30"
#set xtics scale 3,2 rotate by 67 out nomirror
#set xtics out offset 0,-3.0
#set xtics 0,50,500 out nomirror
#set autoscale x
#show xtics
set ytics out nomirror
set logscale y

set style fill solid border -1
set boxwidth 1.0 relative
set style data histograms
set style histogram cluster gap 1 #columnstacked
set xlabel word(firstrow, 1) offset 28,2 font ", 30"
set ylabel "Time to run all queries (in seconds)" offset -2.3,-1.0 font ", 30"

#plot ARG1 using 2, '' using 3:xtic(1), 

#For 3 columns and clustered histogram
plot ARG1 using 2, '' using 3, ''using 4:xtic(1),\
'' using 0:2:2 with labels offset -3.0,1.0 font ",30" notitle,\
'' using 0:3:3 with labels offset 0,1.0 font ",30" notitle,\
'' using 0:4:4 with labels offset 3.0,1.25 font ",30" notitle

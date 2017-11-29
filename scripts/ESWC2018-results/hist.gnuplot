#!/usr/bin/gnuplot

set term postscript eps enhanced color font ",30"
set output ARG1.'.eps'

datafile = ARG1
firstrow = system('head -1 '.datafile)

#set title ARG1
set key top left outside horizontal autotitle columnhead font ", 30"
#set nokey
set auto x
#set xtics scale 3,2 rotate by 67 out nomirror
#set xtics out offset 0,-3.0
set xtics offset 0.6, graph 0.02 font ", 30"
set ytics font ", 30"
set yrange[0.0:1.1]
#set xtics 0,50,500 out nomirror
#set autoscale x
#show xtics
set ytics out nomirror

#set key inside left top

set style fill solid border -1
set boxwidth 1.2 relative
set style data histograms
set style histogram
#set xlabel word(firstrow, 1) offset 40,1.8 font ", 30"
set xlabel word(firstrow, 1) offset 0,-0.5 font ", 30"
#set xlabel "Query Type" offset 0,-0.5 font ", 30"

set ylabel word(firstrow, 2) offset -0.2,-2.5 font ", 30"

# working command
plot ARG1 using 2 :xtic(1), '' using 0:2:2 with labels offset 0.65,0.6 font ", 30" notitle

#plot ARG1 using 2 :xtic(1), '' using 1:2:2 with labels offset 0,0.65 font ", 20"

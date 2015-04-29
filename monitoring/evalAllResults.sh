#!/bin/bash

latexFile=main.tex

workingDir=`pwd`
cd results


for benchmark in `ls`
do
  cd $benchmark
  for run in `ls`
  do
    cd $run

    # preprocessing: generate .res files from .stats files
    for file in `ls *.stats`
    do
      bash $workingDir/processCountFile.sh $file 
    done



    # document header
    echo \
"\\documentclass{article}

\\usepackage{tikz}
\\usetikzlibrary{decorations.pathreplacing,calc}
\\usepackage{pgfplots,pgfplotstable}

\\begin{document}
" > $latexFile



    # nmon results pers host
    for line in `cat usedHosts`
    do
      host=`echo $line | cut -d= -f 2`


      # CPU

      # tikz-header
      echo \
"\begin{tikzpicture}
\begin{axis}[
  title=CPU Utilization on $host,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={CPU Utilization in \%},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

      # add plots
      for file in `ls aeolus-benchmark-$host-CPU*.res`
      do
        utilizationType=`echo $file | cut -d'.' -f 1 | cut -d'-' -f 4`
        # add single plot
        echo "\\addplot table[x index=0, y index=1] {$file}; \\addlegendentry{$utilizationType};" >> $latexFile
      done

      # close tikz
      echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



      # network

      # tikz-header
      echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Network Utilization on $host,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Network Utilization in KB/s},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

      # add plots
      for file in `ls aeolus-benchmark-$host-nw*.res`
      do
        utilizationType=`echo $file | cut -d'.' -f 1 | cut -d'-' -f 4`
        # add single plot
        echo "\\addplot table[x index=0, y index=1] {$file}; \\addlegendentry{$utilizationType};" >> $latexFile
      done

      # close tikz
      echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile

    done



    # throughput results pers operator
    for line in `cat usedHosts`
    do
      operator=`echo $line | cut -d= -f 1`

      # tikz-header
      echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Throughput of $operator,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Throughput in tps},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

      # add plots
      for file in `ls aeolus-benchmark-$operator-*.res`
      do
        utilizationType=`echo $file | cut -d'.' -f 1 | cut -d'-' -f 4`
        # add single plot
        echo "\\addplot table[x index=0, y index=1] {$file}; \\addlegendentry{$utilizationType};" >> $latexFile
      done

      # close tikz
      echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile

    done



    # close document
    echo \
"\\end{document} 
" >> $latexFile

    # compile pdf
    echo compiling $latexFile
    echo "" | pdflatex $latexFile > pdflatex.out
    if [ $? -ne 0 ]
    then
      cat pdflatex.out
    fi



    cd ..
  done
  cd ..
done


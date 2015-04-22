#!/bin/bash

latexFile=main.tex

outputRates="200000 400000 600000 800000 1000000 1200000 1400000 1600000 1800000 2000000"
batchSizes="0 1 2 3 4 5 6 7 8 9 10"
operators="spout sink"



# preprocessing
for outputRate in $outputRates
do
  for batchSize in $batchSizes
  do
    for operatorId in $operators
    do
      bash processCountFile.sh ../bb-res/microbenchmarks $outputRate $batchSize $operatorId
    done
  done
done





# document header
echo \
"\\documentclass{article}

\\usepackage{tikz}
\\usetikzlibrary{decorations.pathreplacing,calc}
\\usepackage{pgfplots,pgfplotstable}

\\begin{document}
" > $latexFile



for outputRate in $outputRates
do
  # tikz-header
  echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Data rate for $outputRate tps for different batch sizes,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={output rate in KB/s},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile
  for batchSize in $batchSizes
  do
    resultFile=../bb-res/microbenchmarks-MeasureOutputRate-$outputRate-$batchSize.res
    if [ ! -f $resultFile ]
    then
      echo "file $resultFile not found (skipping)"
    else
      # add sinlge plot
      echo \
"\\addplot table[x expr=\\coordindex, y index=0] {$resultFile}; \\addlegendentry{$batchSize};" >> $latexFile
    fi
  done

  # close tikz
  echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



  for operatorId in $operators
  do
    # tikz-header
    echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Measure data rates in tps for operator $operatorId,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={data rate in tps},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile
    for batchSize in $batchSizes
    do
      for resultFile in `ls ../bb-res/microbenchmarks-$operatorId-*-$outputRate-$batchSize.res | grep -v -e "::"`
      do
        # add sinlge plot
        echo \
"\\addplot table[x expr=\\coordindex, y index=0] {$resultFile}; \\addlegendentry{$batchSize};" >> $latexFile
      done
    done

    # close tikz
    echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile
  done
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


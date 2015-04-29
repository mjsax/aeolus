#!/bin/bash

if [ -z $1 ]
then
  echo "missing parameter: ./evalBatchingBenchmark.sh <dirName>"
  echo "  <dirName>	the child directory name of './results' of the batching benchmark run (eg, dirName='spout-batching' points to ./results/spout-batching)"
  exit -1
fi

resultDir=$1

##################################################
latexFile=main.tex
tmpFile=/tmp/aeolus-eval.tmp
outputRates=/tmp/aeolus-eval.rates
batchSizes=/tmp/aeolus-eval.bS

workingDir=`pwd`
cd results/$resultDir
rm main* pdflatex.out


for run in `ls`
do
  cd $run

  # preprocessing: generate .res files from .stats files
  for file in `ls *.stats`
  do
    bash $workingDir/processCountFile.sh $file 
  done

  cd ..
done



# gather all output rates
for dir in `ls`
do
  echo $dir | cut -d'-' -f 2 >> $tmpFile
done
sort -u $tmpFile > $outputRates
rm $tmpFile



# document header
echo \
"\\documentclass{article}

\\usepackage{tikz}
\\usetikzlibrary{decorations.pathreplacing,calc}
\\usepackage{pgfplots,pgfplotstable}

\\begin{document}
" > $latexFile


# add a plot for each output rate
for rate in `cat $outputRates`
do
  # gather all batch sizes
  for dir in `ls | grep -e "rate-$rate-bS-"`
  do
    echo $dir | cut -d'-' -f 4 >> $tmpFile
  done
  sort -u $tmpFile > $batchSizes
  rm $tmpFile



  # network out spout

  # tikz-header
  echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Network Out Spout ($rate tps) for different BS,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Network Utilization in KB/s},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

  # add spout plots
  for batchSize in `cat $batchSizes`
  do
    runDirectory=rate-$rate-bS-$batchSize
    spoutHost=`grep -e "spout" $runDirectory/usedHosts | cut -d= -f 2`

    # add single plots
    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-$spoutHost-nwOut.res}; \\addlegendentry{$batchSize};" >> $latexFile
  done

  # close tikz
  echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



  # network in sink

  # tikz-header
  echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Network In Sink ($rate tps) for different BS,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Network Utilization in KB/s},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

  # add spout plots
  for batchSize in `cat $batchSizes`
  do
    runDirectory=rate-$rate-bS-$batchSize
    sinkHost=`grep -e "sink" $runDirectory/usedHosts | cut -d= -f 2`

    # add single plots
    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-$sinkHost-nwIn.res}; \\addlegendentry{$batchSize};" >> $latexFile
  done

  # close tikz
  echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



  # output rate spout

  # tikz-header
  echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Output Rate Spout ($rate tps) for different BS,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Data Rate in tps},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

  # add spout plots
  for batchSize in `cat $batchSizes`
  do
    runDirectory=rate-$rate-bS-$batchSize
    spoutHost=`grep -e "spout" $runDirectory/usedHosts | cut -d= -f 2`

    # add single plots
    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-spout-out.res}; \\addlegendentry{$batchSize};" >> $latexFile
#    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-spout-out::default.res}; \\addlegendentry{default-$batchSize};" >> $latexFile
  done

  # close tikz
  echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



  # input rate sink

  # tikz-header
  echo \
"\begin{tikzpicture}
\begin{axis}[
  title=Input Rte Sink ($rate tps) for different BS,
  width=\textwidth,
  height=0.5\textwidth,
  ylabel={Data Rate in tps},
  xlabel={time in s},
  legend columns=3,
  legend style={at={(0.5,-0.3)},anchor=north},
]" >> $latexFile

  # add spout plots
  for batchSize in `cat $batchSizes`
  do
    runDirectory=rate-$rate-bS-$batchSize

    # add single plots
    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-sink-in.res}; \\addlegendentry{$batchSize};" >> $latexFile
#    echo "\\addplot table[x index=0, y index=1] {$runDirectory/aeolus-benchmark-sink-in::default.res}; \\addlegendentry{default-$batchSize};" >> $latexFile
  done

  # close tikz
  echo \
"\\end{axis}
\\end{tikzpicture}
" >> $latexFile



  # cleanup
  rm $batchSizes
done



# cleanup
rm $outputRates



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


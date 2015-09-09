Sys.setenv(HADOOP_HOME="/usr/lib/hadoop")
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar")
library(rmr2)
bp  = rmr.options("backend.parameters")
bp$hadoop[1] = "mapreduce.map.java.opts=-Xmx2048M"
bp$hadoop[2] = "mapreduce.reduce.java.opts=-Xmx2048M"
rmr.options(backend.parameters = bp)

library(rhdfs)

# initiate rhdfs package
hdfs.init()
hdfs.delete("/user/deepak/out")
map <- function(k,lines) {
  words.list <- strsplit(lines, '\\s')
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text", map=map, reduce=reduce)
}

## read text files from folder example/wordcount/data
hdfs.root <- '/user/deepak/'
hdfs.data <- file.path(hdfs.root, 'test.txt')

## save result in folder example/wordcount/out
hdfs.out <- file.path(hdfs.root, 'out')

## Submit job
out <- wordcount(hdfs.data, hdfs.out) 

## Fetch results from HDFS
results <- from.dfs(out)
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('word', 'count')

print(results.df)


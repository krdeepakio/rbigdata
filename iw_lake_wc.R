Sys.setenv(HADOOP_HOME="/usr/lib/hadoop")
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar")

library(RJSONIO)
library(rmr2)
bp  = rmr.options("backend.parameters")
bp$hadoop[1] = "mapreduce.map.java.opts=-Xmx2048M"
bp$hadoop[2] = "mapreduce.reduce.java.opts=-Xmx2048M"
rmr.options(backend.parameters = bp)
library(rhdfs)

# initiate rhdfs package
hdfs.init()
hdfs.delete("/user/deepak/avrowc1")


avro.jar = "/home/rstudio/avro-mapred-1.7.4-hadoop1.jar"

nullToNA <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

paste.fromJSON = 
  function(...)
    fromJSON(paste("[", paste(..., sep = ","), "]"))

mapply.fromJSON = 
  function(...)
    mapply(paste.fromJSON, ..., SIMPLIFY = FALSE)

avro.input.format = 
  function(con) {
    lines = readLines(con = con, n = 10000)
    if  (length(lines) == 0) NULL
    else
      do.call(
        keyval,
        unname(
          do.call(
            mapply.fromJSON,
            strsplit(
              lines,
              "\t"))))}

avroIF = 
  make.input.format(
    format = avro.input.format,
    mode = "text",
    streaming.format = "org.apache.avro.mapred.AvroAsTextInputFormat",
    backend.parameters = 
      list(
        hadoop = 
          list(
            libjars = avro.jar)))



map <- function(k,lines) {
  #lines <- as.character(lines)
  #words.list <- strsplit(lines, '\\s')
  lines = lapply(lines, nullToNA)
  len_col <- length(lines[[1]])
  res1 = matrix(unlist(lines), ncol=23, byrow=TRUE)
  words.list <- res1[,11]
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format=avroIF, map=map, reduce=reduce)
}

## read text files from folder example/wordcount/data
hdfs.data <- file.path("/sqoop/fa1162092216c174728044d6/55c4eeebe4b064a82e52c332/part-m-00000.avro")

## save result in folder example/wordcount/out
hdfs.out <- file.path("/user/deepak/avrowc1")

## Submit job
out <- wordcount(hdfs.data, hdfs.out) 

## Fetch results from HDFS
results <- from.dfs(out)
results.df <- as.data.frame(results, stringsAsFactors=F)
colnames(results.df) <- c('word', 'count')

results.df




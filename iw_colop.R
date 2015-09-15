# /sqoop/bf3a1990a2a6bf4bb0878392/55f2b0cbe4b0aed61979ecab/part-m-00000.avro
# /sqoop/bf3a1990a2a6bf4bb0878392/55f2b0cbe4b0aed61979ec9b/part-m-00000.avro

# deparse(substitute(a))
# ---------------------------------------------------

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

TABLE_COL_COUNT = 0
COL_INDEX = 0

# initiate rhdfs package
hdfs.init()
if (hdfs.exists("/user/deepak/avrowc1")) {
  hdfs.delete("/user/deepak/avrowc1")
}


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
  res1 = matrix(unlist(lines), ncol=TABLE_COL_COUNT+3, byrow=TRUE)
  words.list <- res1[,COL_INDEX+3]
  words <- unlist(words.list)
  return( keyval(words, 1) )
}

reduce <- function(word, counts) {
  keyval(word, sum(counts))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format=avroIF, map=map, reduce=reduce)
}


# -------------------------------------------



iw_avro_file_path <-function(col_name) {
  qualified_name = deparse(substitute(col_name))
  t1 = strsplit(qualified_name, split='.', fixed=TRUE)
  source_id = get(paste(t1[[1]][1:3], sep=".", collapse="."))$id
  table_id = get(paste(t1[[1]][1:5], sep=".", collapse="."))$id
  paste("/sqoop/", source_id,"/", table_id, "/part-m-00000.avro", sep="")
}

iw_wordcount <- function(col_name) {
  qualified_name = deparse(substitute(col_name))
  t1 = strsplit(qualified_name, split='.', fixed=TRUE)
  source_obj = paste(t1[[1]][1:3], sep=".", collapse=".")
  source_id = get(source_obj)$id
  table_obj = paste(t1[[1]][1:5], sep=".", collapse=".")
  table_id = get(table_obj)$id
  
  TABLE_COL_COUNT <<- get(table_obj)$col_count
  COL_INDEX <<- get(qualified_name)$index
  
  filepath = paste("/sqoop/", source_id,"/", table_id, "/part-m-00000.avro", sep="")
  print(filepath)
  
  
  if (hdfs.exists("/user/deepak/avrowc1")) {
    hdfs.delete("/user/deepak/avrowc1")
  }
  
  ## read text files from folder example/wordcount/data
  hdfs.data <- file.path(filepath)
  
  ## save result in folder example/wordcount/out
  hdfs.out <- file.path("/user/deepak/avrowc1")
  
  ## Submit job  qualified_name = deparse(substitute(col_name))
  t1 = strsplit(qualified_name, split='.', fixed=TRUE)
  source_id = get(paste(t1[[1]][1:3], sep=".", collapse="."))$id
  table_id = get(paste(t1[[1]][1:5], sep=".", collapse="."))$id
  paste("/sqoop/", source_id,"/", table_id, "/part-m-00000.avro", sep="")
  out <- wordcount(hdfs.data, hdfs.out) 
  
  ## Fetch results from HDFS
  results <- from.dfs(out)
  results.df <- as.data.frame(results, stringsAsFactors=F)
  colnames(results.df) <- c('word', 'count')
  
  results.df
}

res1 = iw_wordcount(iwr.sources.TPCDS1gb.tables.store_sales.columns.ss_quantity)
#     word count
#1      1 27419
#2      2 27608
#3      3 27484
#4      4 27555
#5      5 27621
#6      6 27323

print(res1)
res2 = iw_wordcount(iwr.sources.TPCDS1gb.tables.store.columns.s_city)
print(res2)




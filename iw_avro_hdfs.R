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

avro.jar = "/home/rstudio/avro-mapred-1.7.4-hadoop1.jar"

paste.fromJSON = 
  function(...)
    fromJSON(paste("[", paste(..., sep = ","), "]"))

mapply.fromJSON = 
  function(...)
    mapply(paste.fromJSON, ..., SIMPLIFY = FALSE)

avro.input.format = 
  function(con) {
    lines = readLines(con = con, n = 1000)
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

res = from.dfs(
  mapreduce(
    "/sqoop/8bc6edf9d0c0feeb4ed79304/55cdcf27e4b0bdf9e7ae2d76/part-m-00000.avro",
    input.format = avroIF))

final_res = res[[2]]



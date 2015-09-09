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

# known limitations: these formats work only with mapreduce, not with from.dfs or to.dfs, nor they work in on the local backend
# as a workaround, use a simple conversion job 
# from.dfs(mapreduce(some.input, input.format = avroIF)) or mapreduce(to.dfs(some.data), output.format = avroOF)
# avroOF uses a fixed schema "bytes" containing the JSON representation of the data.

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


avro.output.format =
  function(kv, con)
    writeLines(
      unlist(
        rmr2:::reduce.keyval(
          kv,
          function(k, v)
            paste(
              toJSON(k, .escapeEscapes = TRUE),
              toJSON(v, .escapeEscapes = TRUE),
              sep = "\t"))),
      con = con)

avroOF =
  make.output.format(
    format = avro.output.format,
    mode = "text",
    streaming.format = "org.apache.avro.mapred.AvroTextOutputFormat",
    backend.parameters =
      list(
        hadoop =
          list(
            libjars = avro.jar)))


avro.test = 
  mapreduce(
    to.dfs(keyval(1:2, 1:10000)), 
    output.format = avroOF)

res = from.dfs(
  mapreduce(
    avro.test,
    input.format = avroIF))

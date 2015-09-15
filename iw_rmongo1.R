library(RJSONIO)
library(RMongo)
mongo <- mongoDbConnect(dbName="infoworks-new", host="localhost",port='27017')
dbAuthenticate(mongo, 'infoworks', 'IN11**rk')

iwr = "Infoworks Datawarehouse"
sources = dbGetQueryForKeys(mongo, "sources", "{}", "{'name':1, 'tables':1}", 0, 1000)
assign("iwr.sources", sources)

for (i in 1:length(sources[[1]])) {
  source_name = sources$name[[i]]
  source_id = sources$X_id[[i]]
  tables = sources$tables[[i]]
  varname <- paste("iwr.sources", source_name, sep=".")
  assign(varname, list(name=source_name, id=source_id))
  varname <- paste(varname, "tables", sep=".")
  
  # get table details
  tables = dbGetQueryForKeys(mongo, "tables", paste("{'source': { '$oid': '", source_id, "' }}", sep=""), "{'table':1, 'source_columns':1}", 0, 10000)
  assign(varname, tables)
  
  for (j in 1:length(tables[[1]])) {
    table_name = tables$table[[j]]
    table_id = tables$X_id[[j]]
    table_source_columns = tables$source_columns[[j]]
    table_source_columns_json = fromJSON(table_source_columns)
    
    varname1 <- paste(varname, table_name, sep=".")
    assign(varname1, list(name=table_name, id=table_id, col_count=length(table_source_columns_json)))
    varname1 <- paste(varname1, "columns", sep=".")
    assign(varname1, table_source_columns_json)
    
    for (k in 1:length(table_source_columns_json)) {
      column_name = table_source_columns_json[[k]][[1]]
      varname2 <- paste(varname1, column_name, sep=".")
      cols = table_source_columns_json[[k]]
      cols[["index"]] = k
      assign(varname2, cols)
    }
    
  }
}










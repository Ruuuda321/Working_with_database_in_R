## Downloading multiple tables from database to csv file

library(RPostgreSQL)
library(DBI)
library(rgdal)
library(dplyr)


out<-"D:\\TEMP"

db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 


db_drv = dbDriver("PostgreSQL")
con = try(dbConnect(db_drv, dbname = db_name, host = db_hostname, port = db_port, user = db_user, password = db_password),silent = T)

schema = "schema_name"

table_list = c("table1","table2")



for (i in 1:length(table_list)){
  query = sprintf('select * from %s."%s"',schema,table_list[i])
  temp <-dbGetQuery(con,query)
  write.csv2(temp,paste0(out,"/",table_list[i],".csv"))
}


dbDisconnect(con)
closeAllConnections()
dbUnloadDriver(db_drv)

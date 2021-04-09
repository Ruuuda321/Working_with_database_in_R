#Renaming table in database

library(stringr)
library(RPostgreSQL)

db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 
schema = "schema_name"


table_name<-'table_name'
new_table_name<-"new_table_name"  

########################################################################################
con = try(dbConnect(dbDriver("PostgreSQL"), dbname = db_name, host = db_hostname, port = db_port, user = db_user, password = db_password),silent = T)

if(class(con)=="try-error")
{
  stop("Could not connect to database")
}

query1<-sprintf('SET search_path TO %s',schema)
query2<-sprintf('ALTER TABLE %s RENAME TO %s',table_name,new_table_name)
dbSendQuery(con, query1)
dbSendQuery(con, query2)



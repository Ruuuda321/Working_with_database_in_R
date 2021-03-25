# THIS SCRIPT IS DROPPING MULTIPLE TABLES FROM DATABASE BASED ON REGULAR EXPRESSION

#DATABASE access setting 

db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 

#TABLE details
schema = "database_schema" 
text_pattern <- "t_2020_files_([1-9]|[1-6][0-9])" #regular expression or table name


###########################################################################################################################################################
##########################################################################################################################################################
library(RPostgreSQL)
library(stringr)

con = try(dbConnect(dbDriver("PostgreSQL"), dbname = db_name, host = db_hostname, port = db_port, user = db_user, password = db_password),silent = T)

if(class(con)=="try-error")
{
  stop("Could not connect to database")
}


schema_tab_list <- dbGetQuery(con,paste("SELECT table_name FROM information_schema.tables WHERE table_schema='",schema,"'",sep=''))
vec <- c()
for (i in 1:nrow(schema_tab_list)){
  vec <- c(vec, schema_tab_list[i,1])
}


tables<-vec[which(str_detect(string = vec, pattern = text_pattern))]

sprintf('Number of tables to be dropped: %i', length(tables))
print(cat('Tables to be dropped:', tables,sep='\n '))


your_answer <- menu(c("Yes", "No"), title="Did you double checked selected tables? Are you sure you want to drop them permamently?")


if (your_answer == 1) {
  
  tables2drop <- paste(schema,'."',tables,'"',sep='')
  
  drop_query <- paste('DROP TABLE ', tables2drop,' CASCADE',sep='')
  
  for (i in 1:length(tables2drop)){
    dbSendQuery(con, drop_query[i])
  }
  
}


dbDisconnect(con)

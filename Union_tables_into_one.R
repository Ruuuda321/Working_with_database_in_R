#Union tables with matching names using pattern

db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 
schema = "schema_name"

text_pattern <- c("file_(.|..)$") ## tables pattrn; ".+" means multiple various characters


out_table<-c("union_file_name")


###############################################################################################################
library(RPostgreSQL)
library(stringr)

con = try(dbConnect(dbDriver("PostgreSQL"), dbname = db_name, host = db_hostname, port = db_port, user = db_user, password = db_password),silent = T)
if(class(con)=="try-error"){ stop("Could not connect to database") }


schema_tab_list <- dbGetQuery(con,paste("SELECT table_name FROM information_schema.tables WHERE table_schema='",schema,"'",sep=''))
vec <- c()
for (i in 1:nrow(schema_tab_list)){
  vec <- c(vec, schema_tab_list[i,1])
}

tables<-vec[which(str_detect(string = vec, pattern = text_pattern))]
print(tables)


#Control number of records
kontrola<-as.data.frame(matrix(ncol=3))
names(kontrola)<-c("Tabela","ilosc_rekordow","Ilosc_kolumn")

`%notin%` <- Negate(`%in%`)
col_pattern<-".+rtr([0-9]|[n]|[O])"

for (i in 1:length(tables)){
  if (i==1){
    
    get_datatype_query<-paste0("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '", tables[i],"'")
    dt<-dbGetQuery(con, get_datatype_query)
    
    columns<-dt$column_name[which(str_detect(string = dt$column_name, pattern = col_pattern))]
    
    for (j in 2:nrow(dt)){
      if(dt$column_name[j] %notin% columns & dt$data_type[j]=="boolean"){
        set_doubleprecision_query<-sprintf('ALTER TABLE %s."%s" ALTER COLUMN "%s" SET DATA TYPE double precision USING NULL;',schema,tables[i],dt$column_name[j])
        dbSendQuery(con, set_doubleprecision_query)
      } else if (dt$column_name[j] %in% columns & dt$data_type[j]=="boolean"){
        set_integer_query<-sprintf('ALTER TABLE %s."%s" ALTER COLUMN "%s" SET DATA TYPE integer USING NULL;',schema,tables[i],dt$column_name[j])
        dbSendQuery(con, set_integer_query)
      }
    }
    
    union_query<-sprintf('create table %s."%s" as select * from %s."%s"',schema,out_table,schema,tables[i])
    
    count_query<-sprintf('select count(*) from %s."%s"', schema,tables[i])
    count_col_query<-paste0("SELECT count(COLUMN_NAME) FROM information_schema.COLUMNS WHERE TABLE_NAME = '",tables[i],"'")
    
    c<-dbGetQuery(con,count_query)
    cc<-dbGetQuery(con,count_col_query)
    
    kontrola[i,]<-c(tables[i],c$count, cc$count)
  } else {
    a<-sprintf('union all select * from %s."%s"',schema,tables[i])
    union_query<-paste(union_query,a,sep=" ")
    
    get_datatype_query<-paste0("SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '", tables[i],"'")
    dt<-dbGetQuery(con, get_datatype_query)
    
    columns<-dt$column_name[which(str_detect(string = dt$column_name, pattern = col_pattern))]
    for (j in 2:nrow(dt)){
      if(dt$column_name[j] %notin% columns & dt$data_type[j]=="boolean"){
        
        set_datatype_query<-sprintf('ALTER TABLE %s."%s" ALTER COLUMN "%s" SET DATA TYPE double precision USING NULL;',schema,tables[i],dt$column_name[j])
        dbSendQuery(con, set_datatype_query)
      } else if (dt$column_name[j] %in% columns & dt$data_type[j]=="boolean"){
        set_integer_query<-sprintf('ALTER TABLE %s."%s" ALTER COLUMN "%s" SET DATA TYPE integer USING NULL;',schema,tables[i],dt$column_name[j])
        dbSendQuery(con, set_integer_query)
      }
    }
    
    count_query<-sprintf('select count(*) from %s."%s"', schema,tables[i])
    count_col_query<-paste0("SELECT count(COLUMN_NAME) FROM information_schema.COLUMNS WHERE TABLE_NAME = '",tables[i],"'")
    
    c<-dbGetQuery(con,count_query)
    cc<-dbGetQuery(con,count_col_query)
    
    kontrola[i,]<-c(tables[i],c$count, cc$count)
  }
}

dbSendQuery(con, union_query)


#Kontrola ilosci rekordow
kontrola[length(tables)+1,]<-c("SUMA WIERSZY/SREDNIA KOLUMN",sum(as.numeric(kontrola$ilosc_rekordow)),mean(as.numeric(kontrola$Ilosc_kolumn)))

count_query_out<-sprintf('select count(*) from %s."%s"', schema,out_table)
count_col_query_out<-paste0("SELECT count(COLUMN_NAME) FROM information_schema.COLUMNS WHERE TABLE_NAME = '",out_table,"'")

cout<-dbGetQuery(con,count_query_out)
ccout<-dbGetQuery(con,count_col_query_out)
kontrola[length(tables)+2,]<-c(out_table, cout$count, ccout$count)

print(kontrola[c((length(tables)+1),(length(tables)+2)),])


dbDisconnect(con)


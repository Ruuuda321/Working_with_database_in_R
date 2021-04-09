#Script for changing privilages on tables in schema by pattern name
library(stringr)
library(RPostgreSQL)


db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 
schema = "schema_name"


text_pattern <- c("t_2020_files_([1-9]|[1-6][0-9])") ##  wzorzec/wyrazenia tekstowe ktory zawieraja fragment nazw tabel dla ktorych chcemy nadaæ uprawnienia
user_to_privilege<-'username' #nazwa uzytkownika, ktoremu chcemy daæ uprawnienia; je¿eli wszystkim to grupa 'merytor'
action<-'grant' #akcja do wykonania - 'GRANT' nadawanie uprawnieñ, 'REVOKE' - usuñ uprawnienia
privileges<-'ALL'#'SELECT, INSERT, REFERENCES, TRIGGER' #'ALL'  #lista uprawnien do nadania (tutaj podstawowe); podajemy cala liste w 1 cudzys³owie jako 1 ci¹g znaków; mo¿liwe uprawnienia: 'ALL' - wszystkie mo¿liwe na raz, pojedyncze- 'SELECT, INSERT, REFERENCES, TRIGGER, UPDATE, DELETE, TRUNCATE'


################################################################
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
tables_sel<- paste(schema,'."',tables,'"',sep='')


revoke_query <- sprintf('REVOKE %s ON %s FROM %s',privileges, tables_sel, user_to_privilege) #odbieranie uprawnieñ
grant_query<- sprintf('GRANT %s ON %s TO %s',privileges, tables_sel, user_to_privilege) #dawanie uprawnieñ 


if (!(action %in% c('grant','GRANT', 'revoke', 'REVOKE'))) {
  print("Unused function - select 'GRANT' or 'REVOKE' only")
} else {
  for (i in 1:length(tables_sel)){
    if (toupper(action)=='GRANT'){
    dbSendQuery(con, grant_query[i])
    } else if (toupper(action)=='REVOKE'){
      dbSendQuery(con, revoke_query[i])
    }
  }
}

dbDisconnect(con)
closeAllConnections()

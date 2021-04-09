##Script for creating fishnet in postgis based on extend of polygons with interstion
library(RPostgreSQL)



db_name = "database_name"
db_hostname = "database_hostname"
db_port =  5432
db_user = "username"
db_password = "user_password" 
schema = "schema_name"

# table name with polygons (homogenous part of )
base_name <- 'table_name' 

# fishnet cell - width and height in meteres
fn_cell_w <- 5.0




t0 <- Sys.time()
con = try(dbConnect(dbDriver("PostgreSQL"), dbname = db_name, host = db_hostname, port = db_port, user = db_user, password = db_password),silent = T)

if(class(con)=="try-error")
{
  stop("Could not connect to database")
}

schema_tab_list <- dbGetQuery(con,paste("SELECT table_name FROM information_schema.tables WHERE table_schema='",schema,"'",sep=''))

#base_name <- schema_tab_list[29,1]


tab <- paste(schema,base_name,sep='.')

check_indexes <- dbGetQuery(con,paste("SELECT * FROM pg_indexes WHERE tablename = '",base_name,"';",sep=''))

if (nrow(check_indexes) == 0) {
  geomindex_query <- paste("CREATE INDEX ", base_name, "_gix ON ", tab, ' USING GIST(geom)',sep='')
  dbSendQuery(con,geomindex_query)
  idindex_query <- paste("CREATE UNIQUE INDEX ", base_name, "_idx ON ", tab, ' (id) TABLESPACE pg_default',sep='')
  dbSendQuery(con,idindex_query)
}




XMax <- dbGetQuery(con,paste("SELECT MAX(ST_XMax(geom)) AS xmax FROM ",tab, sep=''))
YMax <- dbGetQuery(con,paste("SELECT MAX(ST_YMax(geom)) AS ymax FROM ",tab, sep=''))
XMin <- dbGetQuery(con,paste("SELECT MIN(ST_XMin(geom)) AS xmin FROM ",tab, sep=''))
YMin <- dbGetQuery(con,paste("SELECT MIN(ST_YMin(geom)) AS ymin FROM ",tab, sep=''))

width  <- 50
height <- 50

X <- XMax-XMin
Y <- YMax - YMin

i_length <- round((X/(width*fn_cell_w))+1)
y_length <- round((Y/(height*fn_cell_w))+1)

t1 <- Sys.time()

for (i in 1:i_length[[1]]) {
  
  for (j in 1:y_length[[1]]) {
    pre_table <- paste(tab,'pre',i,j,sep='_')
    fn_query <- paste("SELECT (ST_PixelAsPolygons(ST_AddBand(ST_MakeEmptyRaster(",width,",", height,",", round(XMin-1)+(i-1)*width*fn_cell_w,",",round(YMin+1)+(j-1)*height*fn_cell_w , ",",fn_cell_w, ",",fn_cell_w,",", 0,",",0,",", 2180,")",", '8BSI'::text, 1, 0), 1, false)).geom INTO ",pre_table,sep='')
    dbSendQuery(con,fn_query)
    
    ext_query <- paste("SELECT ST_GeomFromText(ST_AsText(ST_Extent(",pre_table,".geom",")),2180) AS geom", " INTO ",paste(pre_table,'bb',sep='_'),' FROM ',pre_table ,sep='')
    ext <- dbSendQuery(con,ext_query)
    
    tables2intersect <- c(tab,paste(pre_table,'bb',sep='_'))
      
    intersects_query <- paste('SELECT ST_Intersects(',tables2intersect[1],'.geom, ',tables2intersect[2],'.geom)',
                              ' FROM ', tables2intersect[1],', ',tables2intersect[2],sep='')
    
   intersects <- dbGetQuery(con,intersects_query)
      
   if(!any(intersects[,1])) {
     tables2rmv <- c(pre_table,paste(pre_table,'bb',sep='_'))
     drop_query <-  paste("DROP TABLE ",tables2rmv,';', sep='')
       
     for (dr in 1:2) {
       dbSendQuery(con,drop_query[dr])
     }
       
   } else{
     drop_query <-  paste("DROP TABLE ",paste(pre_table,'bb',sep='_'),';', sep='')
     dbSendQuery(con,drop_query)
   }
      
      
  }
}

Sys.time() - t1

schema_tab_list <- dbGetQuery(con,paste("SELECT table_name FROM information_schema.tables WHERE table_schema='",schema,"'",sep=''))
schema_tab_list <- sort(schema_tab_list[,1])

sel_tabs <- grep(base_name,schema_tab_list)
tables2merge <- schema_tab_list[sel_tabs]
sel_pre_tabs <- grep('pre',tables2merge)
tables2merge <- tables2merge[sel_pre_tabs]


stpart_query <- paste('SELECT * INTO ',base_name,'_','all_fn',' FROM ', paste(schema,tables2merge[1],sep="."),sep='')
ndpart_query <- character()

for (i in 2:length(tables2merge)){
  ndpart_query <- paste0(ndpart_query,paste(' UNION ALL SELECT * FROM ', paste(schema,tables2merge[i],sep="."),sep=''),sep='')
}

merge_table_query <- paste(stpart_query,ndpart_query,sep='')


dbSendQuery(con,merge_table_query)

for (i in 1:length(tables2merge)){
  drop_table_query <- paste("DROP TABLE ",paste(schema,tables2merge[i],sep='.'),';', sep='')
  dbSendQuery(con,drop_table_query)
 }

schema_tab_list <- dbGetQuery(con,paste("SELECT table_name FROM information_schema.tables WHERE table_schema='",schema,"'",sep=''))
schema_tab_list <- sort(schema_tab_list[,1])
sel_tabs <- grep(base_name,schema_tab_list)
tables2intersect <- schema_tab_list[sel_tabs]
tables2intersect <- paste(schema,tables2intersect,sep='.')


geomindex_query <- paste("CREATE INDEX ", base_name, "a_gix ON ", tables2intersect[2], ' USING GIST(geom)',sep='')
dbSendQuery(con,geomindex_query)

add_id_query <- paste("ALTER TABLE ",tables2intersect[2], " ADD COLUMN id SERIAL PRIMARY KEY;",sep='')
dbSendQuery(con,add_id_query)

idindex_query <- paste("CREATE UNIQUE INDEX ", base_name, "a_idx ON ", tables2intersect[2], ' (id) TABLESPACE pg_default',sep='')
dbSendQuery(con,idindex_query)




intersect_query <- paste('SELECT ',tables2intersect[2],'.* INTO ',base_name,'_fn_',round(fn_cell_w),'x',round(fn_cell_w),'_inter_stands',
                         ' FROM ',tables2intersect[1],', ',tables2intersect[2],
                         ' WHERE ST_INTERSECTS(', tables2intersect[2],'.geom,',tables2intersect[1],'.geom)',sep='')
t1 <- Sys.time()
dbSendQuery(con,intersect_query)
Sys.time() - t1

geomindex_query <- paste("CREATE INDEX ", base_name, "b_gix ON ", paste(schema,'.',base_name,'_fn_',round(fn_cell_w),'x',round(fn_cell_w),'_inter_stands',sep=''), ' USING GIST(geom)',sep='')
dbSendQuery(con,geomindex_query)

idindex_query <- paste("CREATE INDEX ", base_name, "b_idx ON ", paste(schema,'.',base_name,'_fn_',round(fn_cell_w),'x',round(fn_cell_w),'_inter_stands',sep=''), ' (id) TABLESPACE pg_default',sep='')
dbSendQuery(con,idindex_query)

dbSendQuery(con,paste('DROP TABLE ',tables2intersect[2],sep=''))

Sys.time() - t0

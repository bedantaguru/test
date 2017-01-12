
library(plyr)
library(dplyr)
library(RJDBC)
library(rhdfs)


load.config <- function(table_name, 
                        pt_col_name, 
                        db_stg, 
                        db_final, 
                        hdfs_base_path_final, 
                        hdfs_base_path_staging,
                        base_staging_folder_depth,
                        flume_conf, 
                        partition_format_string, 
                        file_path_to_partition_str_convert,
                        partition_str_to_val_convert, 
                        partition_str_option = c("val","str","func"),
                        partition_str_option_function,
                        set_uniform_pt_col_val = F, 
                        uniform_pt_col_val,
                        use_max_id_while_inserting = T,
                        omit_last_partition = T,
                        no_interrupt_clean_on_partial_loading = F,
                        slow_update = T,
                        slow_update_partitions = 5,
                        ignore_partition_check_while_inserting = F,
                        recommendation_of_direct_insert_by_test_for_common_ids = F,
                        folder_filter_size_limit_in_GB = 2,
                        folder_filter_num_file_limit = 5000,
                        folder_filter_no_info_folder_process = F, 
                        folder_filter_by_size_and_file_num = T, 
                        template_sql_file_path){
  
  partition_str_option <- partition_str_option[1]
  if(!(partition_str_option %in% c("val","str","func"))){
    stop("partition_str_option should be either of val, str or func")
  }
  
  if(partition_str_option=="func"){
    if(missing(partition_str_option_function)){
      stop("partition_str_option_function should be specified if partition_str_option = func")
    }
  }
  
  if(!missing(flume_conf)){
    if(!file.exists(flume_conf)){
      stop(paste0("The file : ", flume_conf, " does not exists or may not be accessible."))
    }
    u <- suppressWarnings(readLines(flume_conf))
    v <- u[str_detect(u, "TwitterAgent.sinks.HDFS.hdfs.path =")]
    v <- strsplit(v, "master1.rbi.com:8020")[[1]][2]
  }else{
    if(missing(partition_format_string)){
      stop("Either of partition_format_string or flume_conf should be present.")
    }
    v <- partition_format_string
  }
  
  
  config <- list()
  if(missing(table_name)){
    config$table_name <- rev(strsplit(strsplit(v ,"/%")[[1]][1],"/")[[1]])[1]
    config$base_folder_src <- str_replace(paste0(strsplit(v, config$table_name)[[1]][1], config$table_name),"/+$","")
  }else{
    config$table_name <- table_name
  }
  
  config$staging_db <- db_stg
  config$final_db <- db_final
  if(is.null(config$base_folder_src)){
    if(!missing(hdfs_base_path_staging)){
      config$base_folder_src <- hdfs_base_path_staging
    }else{
      config$base_folder_src <- str_replace(substr(v,1,max(str_locate_all(str_split(v,"%")[[1]][1],"/")[[1]][,"start"])),"/+$","")
    }
  }
  config$hdfs_base_path_final<- str_replace(hdfs_base_path_final,"/+$","")
  
  config$pt_col <- pt_col_name
  if(missing(base_staging_folder_depth)){
    config$depth_of_folder_src <- str_count(v,"%")
  }else{
    config$depth_of_folder_src <- base_staging_folder_depth
  }
  
  if(missing(file_path_to_partition_str_convert)){
    config$file_path_to_partition_str_convert<- function(x){
      gsub(paste0(config$base_folder_src,"/"),"",x)
    }
  }else{
    config$file_path_to_partition_str_convert<- file_path_to_partition_str_convert
  }
  
  if(missing(partition_str_to_val_convert)){
    config$partition_str_to_val_convert<- function(x){
      # another approach: fmt <- str_replace(substr(v,str_locate(v,"%")[,"start"],nchar(v)),"/+$","")
      fmt <- str_replace(gsub(paste0(config$base_folder_src,"/"),"",v),"/+$","")
      as.POSIXct(x, format = fmt)
    }
  }else{
    config$partition_str_to_val_convert <- partition_str_to_val_convert
  }
  
  config$template_sql_path <- template_sql_file_path
  
  #partition_str_option will be taken into consideration while naming partitions
  config$partition_str_option <- partition_str_option
  if(!missing(partition_str_option_function)){
    config$partition_str_option_function <- partition_str_option_function
  }else{
    if(partition_str_option=="val"){
      config$partition_str_option_function <- function(val, str){
        val
      }
    }else{
      config$partition_str_option_function <- function(val, str){
        str
      }
    }
  }
  
  # insert handle 
  config$use_max_id_while_inserting <- use_max_id_while_inserting
  
  # exclusion of last partition
  config$omit_last_partition <- omit_last_partition
  
  # no_interrupt_clean_on_partial_loading implemented for large data split processing
  config$no_interrupt_clean_on_partial_loading <- no_interrupt_clean_on_partial_loading
  
  # slow update settings
  config$slow_update <- slow_update
  config$slow_update_partitions <- slow_update_partitions
  
  # uniform partition col helps in duplicate reduction
  config$set_uniform_pt_col_val <- set_uniform_pt_col_val
  if(set_uniform_pt_col_val){
    config$uniform_pt_col_val <- uniform_pt_col_val
  }
  
  # implemeneted ignore_partition_check_while_inserting for non partitioned tables
  
  config$ignore_partition_check_while_inserting <- ignore_partition_check_while_inserting
  
  # implemented recommendation_of_direct_insert_by_test_for_common_ids for parquet to parquet ingestion
  config$recommendation_of_direct_insert_by_test_for_common_ids <- recommendation_of_direct_insert_by_test_for_common_ids
  
  # folder filter related configs
  
  config$folder_filter_size_limit_in_GB <- folder_filter_size_limit_in_GB
  config$folder_filter_num_file_limit <- folder_filter_num_file_limit
  config$folder_filter_no_info_folder_process <- folder_filter_no_info_folder_process
  config$folder_filter_by_size_and_file_num <- folder_filter_by_size_and_file_num
  
  options(flow_control = config)
  
  return(invisible(config))
}

construct.DDL <- function(db_name, table_name, pt_col_name, partitioned = T, hdfs_path, is_in_impala = F, template_sql_path){
  
  base_code <- suppressWarnings(readLines(template_sql_path))
  #check
  if(!(c("<R.DB_NAME>","<R.TABLE_NAME>","<R.PARTITION_COL>") %>% lapply(str_detect, string = base_code) %>% lapply(any) %>% unlist() %>% all())){
    stop("Either of the <R.DB_NAME>,<R.TABLE_NAME>,<R.PARTITION_COL> is missing")
  }
  
  ddl_code <- base_code
  
  if(partitioned){
    ddl_code <- ddl_code[!str_detect(ddl_code,"<R.PARTITION_COL>")]
    is_in_impala <- F
  }else{
    is_in_impala <- T
  }
  
  ddl_code <- str_replace_all(ddl_code, "<R.DB_NAME>", db_name) %>%
    str_replace_all("<R.TABLE_NAME>", table_name) %>%
    str_replace_all("<R.PARTITION_COL>", pt_col_name)
  
  if(is_in_impala){
    ddl_code <- c(ddl_code, "STORED AS parquet",paste0("LOCATION '",str_replace_all(file.path(hdfs_path,table_name),"/+","/"),"'"))
  }else{
    ddl_code <- c(ddl_code, paste0("PARTITIONED BY (",pt_col_name," STRING)"),
                  "ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'",
                  paste0("LOCATION '",str_replace_all(file.path(hdfs_path),"/+","/"),"'"))
  }
  
  ddl_code<- paste0(ddl_code, collapse = "\n")
  
  return(ddl_code)
  
}

# a better version can be done :- (ref)
# https://stackoverflow.com/questions/16581327/get-folder-size-of-hdfs-from-java
# https://github.com/RevolutionAnalytics/rhdfs/blob/835d4e5c155e2e8af1f2b8783854b32dfd4cb448/java/com/revolutionanalytics/hadoop/hdfs/FileUtils.java

hdfs.get_folder_size <- function(path, method = c("rhdfs", "cmd"), ref_paths){
  
  method <- method[1]
  
  if(method == "cmd"){
    
    c_out <- system(paste0("hadoop fs -du -s ",path), intern = T)
    
    c_out<- strsplit(c_out, "\\s+")[[1]]
    names(c_out) <- c("size_in_HDFS_in_bytes","size_in_disk_in_bytes", "path")
    c_out <- as.list(c_out)
    c_out$size_in_HDFS_in_bytes <- as.numeric(c_out$size_in_HDFS_in_bytes)
    c_out$size_in_disk_in_bytes <- as.numeric(c_out$size_in_disk_in_bytes)
    c_out$size_in_HDFS_in_GB <- c_out$size_in_HDFS_in_bytes/1024^3
    c_out <- as.data.frame(c_out)
    
  }else{
    
    c_out <- hdfs.ls(path, recurse = T)
    
    if(missing(ref_paths)){
      # simple case which does not require further folder grouping
      c_out$path <- dirname(c_out$file)
    }else{
      
      ref_paths <- sort(ref_paths)
      c_out <- c_out[ order(c_out$file),]
      c_out$path <- ""
      for(rp in ref_paths){
        c_out$path[which(str_detect(c_out$file,rp))] <- rp
      }
      
      if(any(nchar(c_out$path)==0)){
        cat("\nhdfs.get_folder_size may produce undesirable result. Check ref_paths\n")
      }
      
    }
    
    
    c_out_a <- c_out %>% group_by(path) %>% dplyr::summarise(
      size_in_HDFS_in_bytes = sum(size),
      num_files = n()
    )
    
    c_out_a$isDir <- T
    
    c_out <- c_out_a
    
    c_out$size_in_HDFS_in_GB <- c_out$size_in_HDFS_in_bytes/1024^3
    
  }
  
  return(c_out)
}

hdfs.subfolder_stat <- function(path, method = c("rhdfs", "cmd")){
  method <- method[1]
  if(method == "cmd"){
    dout <- ldply(hdfs.ls(path)$file, hdfs.get_folder_size)
  }else{
    dout <- hdfs.get_folder_size(path, method = "rhdfs")
  }
  return(dout)
}

hdfs.get_folder_depth <- function(path){
  folder_size_info <- hdfs.subfolder_stat(path, method = "rhdfs")
  smpl_path <- folder_size_info$path[1]
  
  if(dirname(smpl_path)<path){
    depth <- 0
  }else{
    depth <- 1
  }
  
  
  while(dirname(smpl_path)>path) {
    depth <- depth + 1
    smpl_path <- dirname(smpl_path)
  }
  
  l_out <- list(depth = depth, folder_size_info = folder_size_info)
  
  return(l_out)
}

hdfs.clean_size_zero_folders<-function(path){
  size_info <- hdfs.subfolder_stat(path)
  if(nrow(size_info)>0){
    size_info_zeros <- size_info[ size_info$size_in_HDFS_in_bytes==0, ]
    if(nrow(size_info_zeros)>0){
      hdfs.delete(size_info_zeros$path)
    }
  }
  return(invisible(0))
}

# This code will be runing in Hadoop integrated environment only
begin_flow <- function(recreate_tables = T){
  
  # clean options 
  options(flow_hdfs_folder_sizes_stat = NULL)
  
  # load packages 
  Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
  hdfs.init()
  
  # config
  flow_control <- options("flow_control")[[1]]
  
  # initiate query engines (Hive and Impala)
  
  flow_control$query_engine <- list(hive = list(),
                                    impala = list())
  
  # Generic Function Required for query function generation
  
  query_fn_gen <- function(f){
    f_out <- function(qrys, need_result = F){
      
      out <- 0 
      
      if(length(qrys)==0) return(invisible(0))
      for( i in seq(length(qrys)) ){
        
        if(i!=length(qrys)){
          f(qrys[[i]], get_result = F)
        }else{
          out <- f(qrys[[i]], get_result = need_result)
        }
        
      }
      
      if(need_result){
        return(out)
      }else{
        return(invisible(0))
      }
    }
    
    return(f_out)
  }
  
  
  # Hive
  drvH <- JDBC("org.apache.hive.jdbc.HiveDriver", "/opt/cloudera/parcels/CDH/lib/hive/lib/hive-jdbc.jar", identifier.quote="`")
  connH <- dbConnect(drvH, "jdbc:hive2://master1.rbi.com:10000", "hdfs")
  
  queryH <- function(qry, get_result=F){
    if(get_result){
      dbGetQuery(connH, qry)
    }else{
      dbSendUpdate(connH, qry)
    }
  }
  
  
  flow_control$query_engine$hive$query <- query_fn_gen(queryH)
  
  #Impala
  drvI <- JDBC("org.apache.hive.jdbc.HiveDriver", "/opt/cloudera/drivers/ImpalaJDBC41.jar", "'")
  connI <- dbConnect(drvI, "jdbc:hive2://slave1.rbi.com:21050/;auth=noSasl")
  
  queryI <- function(qry, get_result=F){
    if(get_result){
      dbGetQuery(connI, qry)
    }else{
      dbSendUpdate(connI, qry)
    }
  }
  
  flow_control$query_engine$impala$query <- query_fn_gen(queryI)
  
  ####### Both the steps will run in HIVE ######
  
  if(recreate_tables){
    
    ####### Initialization setps ########
    # re-create staging table in Hive   #
    #####################################
    
    cmd <- paste0("USE ",flow_control$staging_db)
    cmd <- c(cmd, paste0("DROP TABLE IF EXISTS ", flow_control$table_name))
    
    cmd<- c(cmd, construct.DDL(db_name = flow_control$staging_db
                               ,pt_col_name = flow_control$pt_col
                               ,table_name = flow_control$table_name
                               ,partitioned = T
                               ,hdfs_path = flow_control$base_folder_src
                               ,is_in_impala = F
                               ,template_sql_path = flow_control$template_sql_path))
    
    
    flow_control$query_engine$hive$query(cmd)
    
    ####### Initialization setps ########
    # re-create final table in Impala   #
    #####################################
    
    cmd <- paste0("USE ",flow_control$final_db)
    cmd <- c(cmd, paste0("DROP TABLE IF EXISTS ", flow_control$table_name))
    
    cmd<- c(cmd, construct.DDL(db_name = flow_control$final_db
                               ,pt_col_name = flow_control$pt_col
                               ,table_name = flow_control$table_name
                               ,partitioned = F
                               ,hdfs_path = flow_control$hdfs_base_path_final
                               ,is_in_impala = T
                               ,template_sql_path = flow_control$template_sql_path))
    
    
    flow_control$query_engine$hive$query(cmd)
    
  }
  
  options(flow_control = flow_control)
  
  return(invisible(flow_control))
}


# get hdfs file summary
get_staging_folder_information <- function(folder_filter_by_size = F, return_detailed_info = F){
  
  flow_control <- options("flow_control")[[1]]
  
  if(!hdfs.exists(flow_control$base_folder_src)){
    #when there is no base folder
    return(data.frame())
  }
  
  data_now <- hdfs.ls(flow_control$base_folder_src)
  # get sub partitions 
  if(flow_control$depth_of_folder_src>1){
    for(i in seq(flow_control$depth_of_folder_src-1)){
      data_now <- ldply( data_now$file , hdfs.ls)
    }
  }
  
  
  if(folder_filter_by_size & flow_control$folder_filter_by_size_and_file_num){
    
    # check folder size # process only small folders
    if(is.null(options("flow_hdfs_folder_sizes_stat")[[1]])){
      folder_sizes <- hdfs.get_folder_size(flow_control$base_folder_src, ref_paths = data_now$file)
      options(flow_hdfs_folder_sizes_stat = folder_sizes)
    }else{
      folder_sizes <- options("flow_hdfs_folder_sizes_stat")[[1]]
    }
    
    
    data_now <- merge(data_now, folder_sizes, by.x = "file", by.y = "path", all.x = T, all.y = F)
    
    data_now$is_folder_size_present <- !(is.na(data_now$size_in_HDFS_in_GB))
    
    if(any(is.na(data_now$size_in_HDFS_in_GB))){
      if(flow_control$folder_filter_no_info_folder_process){
        cat("\nFolder Size in not complete. Folder filter may not be accurate. Number of such Folders",sum(is.na(data_now$size_in_HDFS_in_GB)),"\n")
        data_now$size_in_HDFS_in_GB[is.na(data_now$size_in_HDFS_in_GB)] <- flow_control$folder_filter_size_limit_in_GB/2 # dummy value
        data_now$num_files[is.na(data_now$num_files)] <- flow_control$folder_filter_num_file_limit/2 # dummy value
      }else{
        cat("\nFolder Size in not complete. Folder filter ingnoring them. Number of such Folders",sum(is.na(data_now$size_in_HDFS_in_GB)),"\n")
        data_now$size_in_HDFS_in_GB[is.na(data_now$size_in_HDFS_in_GB)] <- flow_control$folder_filter_size_limit_in_GB*2 # dummy value
        data_now$num_files[is.na(data_now$num_files)] <- flow_control$folder_filter_num_file_limit*2 # dummy value
      }
      
    }
    
    # limits are hard codes now [can be configured]
    if(any(data_now$size_in_HDFS_in_GB > flow_control$folder_filter_size_limit_in_GB) | 
       any(data_now$num_files > flow_control$folder_filter_num_file_limit)){
      
      folder_filetered <- data_now$file[ data_now$size_in_HDFS_in_GB > flow_control$folder_filter_size_limit_in_GB | 
                                           data_now$num_files > flow_control$folder_filter_num_file_limit ]
      data_filtered <- data_now[ (data_now$file %in% folder_filetered),]
      cat("\nFolders Excluded due to size limitations:-\n",paste0(folder_filetered, collapse = "\n"),"\n", sep="")
      data_now <- data_now[ !(data_now$file %in% folder_filetered),]
      
      # do not remove folder_filetered because if return_detailed_info is mentioned "folder_filetered" will be returned
      
    }
    
  }
  
  data_now$partition_str <- flow_control$file_path_to_partition_str_convert( data_now$file )
  data_now$partition_val <- flow_control$partition_str_to_val_convert( data_now$partition_str )
  data_now$partition_option_str <- as.character(flow_control$partition_str_option_function(val=data_now$partition_val, str=data_now$partition_str))
  
  
  if(flow_control$omit_last_partition){
    data_now <- data_now[ data_now$partition_val != max(data_now$partition_val), ]
  }
  
  if(return_detailed_info){
    # this is mainly required by large data separation flow
    l_out <- list(data = data_now)
    if(exists("folder_filetered")){
      l_out$folder_filetered <- folder_filetered
    }
    if(exists("folder_sizes")){
      l_out$folder_sizes <- folder_sizes
    }
    if(exists("data_filtered")){
      
      data_filtered$partition_str <- flow_control$file_path_to_partition_str_convert( data_filtered$file )
      data_filtered$partition_val <- flow_control$partition_str_to_val_convert( data_filtered$partition_str )
      data_filtered$partition_option_str <- as.character(flow_control$partition_str_option_function(val=data_filtered$partition_val, str=data_now$partition_str))
      
      l_out$data_filtered <- data_filtered
    }
    return(l_out)
  }
  
  return(data_now)
}

# add partitions into hive table
add_partition <- function(del_old_partitions = T){
  
  flow_control <- options("flow_control")[[1]]
  
  dat_now <- get_staging_folder_information(folder_filter_by_size = T)
  
  total_partitions <- nrow(dat_now)
  
  if(total_partitions==0){
    return(invisible(FALSE))
  }
  
  slow_update_partitions <- min(flow_control$slow_update_partitions, total_partitions)
  
  if(flow_control$slow_update){
    del_old_partitions <-T
    
    dat_now <- dat_now[order(dat_now$partition_val),]
    dat_now <- dat_now[seq(slow_update_partitions),]
    
  }
  
  # start the flow
  cmd <- paste0("USE ",flow_control$staging_db)
  
  # delete partition if required
  if(del_old_partitions){
    cmd <- c(cmd, paste0("ALTER TABLE ",flow_control$table_name," DROP PARTITION (",flow_control$pt_col," >= '0')"))
  }
  
  # create partition
  cmd <- c(cmd, paste0("ALTER TABLE ",flow_control$table_name," ADD IF NOT EXISTS PARTITION (",flow_control$pt_col," = '", dat_now$partition_option_str ,"') LOCATION '", dat_now$file ,"'"))
  
  
  flow_control$query_engine$hive$query(cmd)
  
  return(invisible(TRUE))
}


# structurized DB
insert_into_structurized_db <- function(){
  
  flow_control <- options("flow_control")[[1]]
  
  proceed <- F
  
  if(flow_control$ignore_partition_check_while_inserting){
    proceed <- T
  }else{
    d_pt <- flow_control$query_engine$hive$query(c(paste0("use ", flow_control$staging_db),
                                                   paste0("show partitions ", flow_control$table_name)),need_result = T)
    if(nrow(d_pt)>0){
      proceed <- T
    }
  }
  # load only if a single partition is present
  if(proceed){
    
    final_table_colnames <- flow_control$query_engine$hive$query(paste0("SHOW COLUMNS IN ",flow_control$final_db,".",flow_control$table_name), need_result = T)[[1]]
    
    # keeping partition col name will assist in duplicate reduction while using distinct
    if(flow_control$set_uniform_pt_col_val){
      final_table_colnames[which(final_table_colnames ==  flow_control$pt_col)] <- shQuote(flow_control$uniform_pt_col_val)
    }
    
    flow_control$query_engine$impala$query(c(paste0("use ", flow_control$final_db),
                                             "INVALIDATE METADATA"))
    
    # known issue with integer conversion
    dat_id_dest <- flow_control$query_engine$impala$query(paste0("select cast(max(id) as string), cast(min(id) as string) from ",
                                                                 flow_control$final_db,".",flow_control$table_name), need_result = T)
    max_id <- dat_id_dest[[1]][1]
    
    use_direct_ingest <- F
    
    if(is.na(max_id)){
      use_direct_ingest <- T
    }
    
    # small patch for insertion in case of impala to impala [parquet to parquet]
    # this checks for possible intersectio between srouce and destination
    # if it does not find any insersection it will recommend direct insert [this may be useful in case someone has kept old large files]
    # applicable if source and dest both are in parquet [mainly will be used in large file separation work flow]
    if(flow_control$recommendation_of_direct_insert_by_test_for_common_ids){
      dat_common_id <- flow_control$query_engine$impala$query(paste0("select distinct a.id from ",flow_control$staging_db,".",flow_control$table_name
                                                                     ," as a join ",flow_control$final_db,".",flow_control$table_name," as b on a.id = b.id"), need_result = T)
      if(nrow(dat_common_id)==0){
        use_direct_ingest <- T
      }
    }
    
    # init same info 
    dat_id_src <- dat_id_dest
    
    if(use_direct_ingest){
      # insert required from start
      cat("\ninsert_into_structurized_db:Performing unrestricted insert\n")
      cmd <- paste0("insert into ",flow_control$final_db,".",flow_control$table_name,"\n",
                    "select distinct ",paste(final_table_colnames,collapse = ","),"\n",
                    "from ",flow_control$staging_db,".",flow_control$table_name," where id is not NULL ")
      
    }else{
      
      is_use_max_id_while_inserting <- T
      
      # check for non use of max id case
      if(!flow_control$use_max_id_while_inserting){
        dat_id_src <- flow_control$query_engine$hive$query(paste0("select cast(max(id) as string), cast(min(id) as string) from ",flow_control$staging_db,".",flow_control$table_name), need_result = T)
        if(dat_id_dest[[1]] < dat_id_src[[2]]){
          # completely disconnected sets : dat_id_src[[1]] < dat_id_dest[[2]] | dat_id_dest[[1]] < dat_id_src[[2]]
          # but in reverse order max_id is not gonna work
          is_use_max_id_while_inserting <- T
        }else{
          is_use_max_id_while_inserting <- F
        }
        
      }
      # insert required (incremental)
      if(is_use_max_id_while_inserting){
        cat("\ninsert_into_structurized_db:Using 'max id' while inserting\n")
        cmd <- paste0("insert into ",flow_control$final_db,".",flow_control$table_name,"\n",
                      "select distinct ",paste(final_table_colnames,collapse = ","),"\n",
                      "from ",flow_control$staging_db,".",flow_control$table_name," where id is not NULL and id > ",max_id)
        
      }else{
        cat("\ninsert_into_structurized_db:Using 'NOT IN' while inserting\n")
        # using NOT in [may be slow and not ideal if final DB is large]
        # insert into table_dest select id,* from table_src where id not in (select distinct id from table_dest) - does not work
        # error will be like : FAILED: SemanticException [Error 10249]: Unsupported SubQuery Expression 'id': Correlating expression cannot contain unqualified column references.
        id_done_list <- flow_control$query_engine$impala$query(paste0("select distinct cast(id as string) from ",flow_control$final_db,".",flow_control$table_name), need_result = T)
        id_done_list <- id_done_list[[1]]
        id_done_list <- id_done_list[ id_done_list <= dat_id_src[[1]] & id_done_list >= dat_id_src[[2]] ]
        cmd <- paste0("insert into ",flow_control$final_db,".",flow_control$table_name,"\n",
                      "select distinct ",paste(final_table_colnames,collapse = ","),"\n",
                      "from ",flow_control$staging_db,".",flow_control$table_name," where id is not NULL and id not in (",paste0(id_done_list, collapse = ","),")")
        
      }
      
      
    }
    
    # run the cmd in hive
    flow_control$query_engine$hive$query(cmd)
    
  }
  
  return(invisible(0))
}

# check data load in two system
check_data_load <- function(info_out = F){
  
  flow_control <- options("flow_control")[[1]]
  
  result <- F
  
  done_partitions <- character(0)
  
  # differnet way of checking while using uniform_pt_col_val
  if(flow_control$set_uniform_pt_col_val){
    
    dh <- flow_control$query_engine$hive$query(paste0("select ",flow_control$pt_col,",count(distinct id) as raw_count, cast(max(id) as string) as max_id, cast(min(id) as string) as min_id from ",
                                                      flow_control$staging_db,".",flow_control$table_name,
                                                      " group by ",flow_control$pt_col,""), need_result = T)
    
    dh_id_unique <- flow_control$query_engine$hive$query(paste0("select count(distinct id) as id_raw_count from ",
                                                                flow_control$staging_db,".",flow_control$table_name), need_result = T)
    
    try(dh$num_unique_ids <- dh_id_unique$id_raw_count, silent = T)
    
    
  }else{
    
    dh <- flow_control$query_engine$hive$query(paste0("select ",flow_control$pt_col,",count(distinct id) as raw_count from ",
                                                      flow_control$staging_db,".",flow_control$table_name,
                                                      " group by ",flow_control$pt_col,""), need_result = T)
    
  }
  
  if(length(dh[[1]])>0){
    
    flow_control$query_engine$impala$query(c(paste0("use ", flow_control$final_db),
                                             "INVALIDATE METADATA"))
    
    # differnet way of checking while using uniform_pt_col_val
    if(flow_control$set_uniform_pt_col_val){
      di <- flow_control$query_engine$impala$query(paste0("select count(distinct id) as final_count, cast(max(id) as string) as max_id, cast(min(id) as string) as min_id  from ",
                                                          flow_control$final_db,".",flow_control$table_name," where id > = ", min(dh$min_id) ), need_result = T)
      # tests
      chk_rank <- 1
      if(max(dh$max_id)<=di$max_id[1]){
        chk_rank <- chk_rank + 1
      }else{
        cat("\ncheck_data_load:max id not matching\n")
      }
      
      if(min(dh$min_id)>=di$min_id[1]){
        chk_rank <- chk_rank + 1
      }else{
        cat("\ncheck_data_load:min id not matching\n")
      }
      
      # check based on unique ids
      if(dh$num_unique_ids[1] == di$final_count){
        chk_rank <- chk_rank + 1
      }else{
        cat("\nncheck_data_load:not matching unique counts\n")
      }
      
      if(chk_rank > 1){
        # dummy replacement for deletion as this test ensures data being loaded.
        di<-dh[c(flow_control$pt_col, "raw_count")]
        colnames(di)[2] <- "final_count"
      }
      
      
      
    }else{
      # ref why date alone can't be used https://community.cloudera.com/t5/Interactive-Short-cycle-SQL/Impala-table-with-column-names-as-reserved-words/td-p/19900
      di <- flow_control$query_engine$impala$query(paste0("select `",flow_control$pt_col,"`, count(1) as final_count  from ",
                                                          flow_control$final_db,".",flow_control$table_name,
                                                          " where ",flow_control$pt_col," in ", paste0("('",paste0(unique(dh[[1]]), collapse = "','"),"')"),
                                                          " group by `",flow_control$pt_col,"`"), need_result = T)
      
    }
    
  }else{
    di <- data.frame()
  }
  
  dm<- try( merge(di, dh))
  if(is.data.frame(dm)){
    if(nrow(dm)>0){
      dm$diff <- dm$raw_count - dm$final_count
      if(sum(abs(dm$diff))<0.1){
        result <- T
      }
      
      if(flow_control$no_interrupt_clean_on_partial_loading){
        
        # no interruption even is data is partial loaded [for large data ingestion where duplicates are possible]
        result <- T
        done_partitions <- dm$part_col
        
      }else{
        done_partitions <- dm$part_col [ dm$diff == 0 ]
      }
    }
  }
  
  if(info_out){
    result <- list(is_loaded = result, is_clean_required = (length(done_partitions)>0), data = list(impala = di, hive = dh, merge = dm, done_partitions = done_partitions))
  }
  
  return(result)
}

# delete old data
clear_old_unstructured_data <- function(selective_partitions){
  
  dat_now <- get_staging_folder_information()
  
  total_files <- nrow(dat_now)
  
  if(!missing(selective_partitions)){
    dat_now <- dat_now[ dat_now$partition_option_str %in% as.character(selective_partitions),]
  }
  
  if(nrow(dat_now)>0){
    
    cat("\nDeleting :", nrow(dat_now), "files out of", total_files, "files.\n")
    
    
    # detect blank parent folder
    del_parent_folder_if_blank <- function(f_path){
      par_fn <- dirname(f_path)
      par_info <- hdfs.ls( par_fn)
      if(is.null(par_info)){
        hdfs.delete(par_fn)
        Recall(par_fn)
      }
      return(invisible(0))
    }
    
    # added for supressing hdfs.delete console outputs
    sink("/dev/null")
    
    try({
      
      for( fn in unique(dat_now$file)){
        hdfs.delete(fn)
        del_parent_folder_if_blank(fn)
      }
      
    })
    
    # added for supressing hdfs.delete console outputs
    sink()
  }
  
  return(invisible(0))
}


# work flow
work_flow <- function(){
  
  is_partition_added <- add_partition()
  
  if(is_partition_added){
    insert_into_structurized_db()
  }
  
  clean_up <- check_data_load(info_out = T)
  
  if(clean_up$is_clean_required){
    clear_old_unstructured_data(selective_partitions = clean_up$data$done_partitions)
  }else{
    cat("\nData not fully loaded or completed.\n")
  }
  # return when nothing more to load
  return(invisible(!clean_up$is_loaded))
}


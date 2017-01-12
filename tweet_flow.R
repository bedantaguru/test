
rm(list = ls())
source("lib/tweet_flow.lib.R")


# This code will be runing in Hadoop integrated environment only
init_flow <- function(){
  
  # config
  load.config(pt_col_name = "part_col_str", 
              db_stg = "rbi_unstructured_data", 
              db_final = "rbi_structurized_data",
              hdfs_base_path_final = "/project/rbi/structurized/", 
              # for usual tweets : partition_format_string = "/user/flume/tweets/%Y/%m/%d/",
              # for Gov related tweets : 
              partition_format_string = "/user/flume/tweets_modi/%Y/%m/%d/%H/",
              slow_update_partitions = 10,
              template_sql_file_path = "config/twitter_create_table_part.sql")
  
  begin_flow()
  
  return(invisible(0))
}

flow_controller <- function(){
  
  init_flow()
  
  # sequential loading untill all is done
  repeat{
    load_stat <- work_flow()
    if(load_stat){
      break
    }
    Sys.sleep(2)
  }
  
}
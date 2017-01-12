
# to get function call sequence 
# https://www.r-bloggers.com/r-function-of-the-day-foodweb-2/
# library(mvbutils) 
# then source
# then foodweb(border = T)

rm(list = ls())
source("ref/WorkFlow_Generics.R")
source("lib/large_data_set_separation.lib.R")

# take functions from main data flow
main_data_flow <- load_flow_source("lib/tweet_flow.lib.R")

init_flow <- function(){
  
  load.config(run_in_automated_mode = T, # this property determines which mode it will run 
              # automated mode is simpler as it takes main process arguments and proceed from there
              # only parent_partition_format_string is required
              parent_partition_format_string = "/user/flume/tweets_modi/%Y/%m/%d/%H/", # For usual tweets "/user/flume/tweets/%Y/%m/%d/" and for Gov "/user/flume/tweets_modi/%Y/%m/%d/%H/"
              # manual mode all three below is required to be mentioned
              # table_name = "tweets_modi",
              # large_folder_hdfs_path = "/user/flume/tweets_modi/2016/11/09/00",
              # parent_partition_string = "2016-11-09 00:00:00",
              hdfs_intermediate_staging_path = "/project/rbi/large_folder_processed",
              db_raw_staging = "rbi_dev",
              db_intermediate_staging = "rbi_dev_clean",
              db_final = "rbi_structurized_data",
              folder_group_aprox_size_in_MB = 200, 
              folder_group_file_num_limit = 3000,
              template_sql_file_path = "config/twitter_create_table_part.sql",
              safe_mode = F)
  
  
}

flow_controller <- function(){
  
  init_flow()
  
  work_flow()
  
}
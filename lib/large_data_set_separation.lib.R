

#ref http://blog.cloudera.com/blog/2009/02/the-small-files-problem/

require(plyr)
require(dplyr)


load.config <- function(run_in_automated_mode = F,
                        parent_partition_format_string,
                        table_name,
                        large_folder_hdfs_path,
                        parent_partition_string,
                        hdfs_intermediate_staging_path,
                        db_raw_staging,
                        db_intermediate_staging,
                        db_final,
                        folder_group_aprox_size_in_MB = 500,
                        folder_group_file_num_limit = 3000,
                        template_sql_file_path,
                        safe_mode = F){
  config <- list()
  
  config$run_in_automated_mode <- run_in_automated_mode
  
  if(config$run_in_automated_mode){
    
    config$parent_partition_format_string <- parent_partition_format_string
    
  }else{
    
    config$table_name <- table_name
    config$large_folder_hdfs_path <- large_folder_hdfs_path
    config$parent_partition_string <- parent_partition_string
    
  }
  
  config$db_raw_staging <- db_raw_staging
  config$db_intermediate_staging <- db_intermediate_staging
  config$db_final <- db_final
  config$folder_group_aprox_size_in_MB <- folder_group_aprox_size_in_MB
  config$folder_group_file_num_limit <- folder_group_file_num_limit
  config$hdfs_intermediate_staging_path <- hdfs_intermediate_staging_path
  config$template_sql_file_path <- template_sql_file_path
  
  config$safe_mode <- safe_mode
  
  # flow_control may have to design differently
  # LDS : Large Data Separation
  options(flow_control_LDS = config)
  return(invisible(config))
}

rebuild_config <- function(){
  
  hdfs.init()
  # clean options 
  options(flow_hdfs_folder_sizes_stat = NULL)
  
  flow_control <- options("flow_control_LDS")[[1]]
  
  if(flow_control$run_in_automated_mode){
    # re configure for manual runs
    
    # load dummy config to get folder stats
    main_data_flow$load.config(pt_col_name = "part_col_str", 
                               db_stg = "dummy", 
                               db_final = "dummy",
                               hdfs_base_path_final = "dummy", 
                               partition_format_string = flow_control$parent_partition_format_string,
                               template_sql_file_path = "dummy")
    
    info_parent_folder <- main_data_flow$get_staging_folder_information(folder_filter_by_size = T, 
                                                                        return_detailed_info = T)
    
    if(!is.null(info_parent_folder$data_filtered)){
      info_dat <- info_parent_folder$data_filtered
      info_dat <- info_dat[ info_dat$is_folder_size_present,]
      
      if(nrow(info_dat)>0){
        info_dat$is_large_folder <- (info_dat$size_in_HDFS_in_bytes > flow_control$folder_group_aprox_size_in_MB*1024*1024 | 
                                       info_dat$num_files > flow_control$folder_group_file_num_limit)
        if(any(!info_dat$is_large_folder)){
          cat("\nThere are small folders in the path discarding them. These are:-\n",paste(info_dat$file[!info_dat$is_large_folder],collapse = "\n"),"\n",sep="")
          info_dat <- info_dat[info_dat$is_large_folder,]
        }
        
        if(nrow(info_dat)>0){
          cat("\nTotal paths to process\n",paste(info_dat$file,collapse = "\n"),"\n",sep="")
          
          table_name <- options("flow_control")[[1]]$table_name
          
          config_list<- dlply(info_dat, "file", function(d){list(table_name = table_name, large_folder_hdfs_path = d$file, parent_partition_string = d$partition_option_str)}) 
        }
        
      }
      
    }
    
  }else{
    config_list <- list(flow_control[c("table_name","large_folder_hdfs_path","parent_partition_string")])
  }
  return(invisible(config_list))
}

after_rebuild_load.config <- function(manual_config_part){
  
  flow_control <- options("flow_control_LDS")[[1]]
  
  load.config(run_in_automated_mode = F,
              # only following three parameters are taken from argument
              table_name = manual_config_part$table_name,
              large_folder_hdfs_path = manual_config_part$large_folder_hdfs_path,
              parent_partition_string = manual_config_part$parent_partition_string,
              # rest remains unchanged
              hdfs_intermediate_staging_path = flow_control$hdfs_intermediate_staging_path,
              db_raw_staging = flow_control$db_raw_staging,
              db_intermediate_staging = flow_control$db_intermediate_staging,
              db_final = flow_control$db_final,
              folder_group_aprox_size_in_MB = flow_control$folder_group_aprox_size_in_MB, 
              folder_group_file_num_limit = flow_control$folder_group_file_num_limit,
              template_sql_file_path = flow_control$template_sql_file_path)
  
}

begin_flow <- function(delete_old_files = T){
  
  flow_control <- options("flow_control_LDS")[[1]]
  
  # have separate functions for conversion
  # config
  # slow_update_partitions = 1 only ensures no duplicates
  # Note : at this tage it is mandatory to give same name for pt_col in both main data flow and LDS
  main_data_flow$load.config(table_name = flow_control$table_name,
                             pt_col_name = "part_col_str", 
                             db_stg = flow_control$db_raw_staging, 
                             db_final = flow_control$db_intermediate_staging,
                             hdfs_base_path_final = flow_control$hdfs_intermediate_staging_path,
                             partition_format_string = paste0(flow_control$large_folder_hdfs_path,"/Large_Folder_Split_%Y"),
                             partition_str_option = "func",
                             partition_str_option_function = function(val, str){
                               paste(options("flow_control_LDS")[[1]]$parent_partition_string,format(val, "%Y"))
                             },
                             omit_last_partition = F, 
                             use_max_id_while_inserting = F,
                             no_interrupt_clean_on_partial_loading = T,
                             slow_update_partitions=ifelse(flow_control$safe_mode, 1, 10),
                             set_uniform_pt_col_val = T, 
                             uniform_pt_col_val = flow_control$parent_partition_string,
                             template_sql_file_path = flow_control$template_sql_file_path,
                             # disable folder_filter
                             folder_filter_by_size_and_file_num = F)
  
  main_data_flow$begin_flow()
  
  # clean 
  if(delete_old_files){
    # permission is required to be formalized (below is example)
    # groupadd supergroup
    # usermod -a -G supergroup ruser
    old_data_path <- paste0(flow_control$hdfs_intermediate_staging_path,"/",flow_control$table_name)
    if(hdfs.exists(old_data_path)){
      hdfs.delete(old_data_path)
      hdfs.mkdir(old_data_path)
    }
  }
  
  return(invisible(0))
  
}

create_chrono_file_buckets <- function(path, aprox_size_in_MB = 500, max_tolerance_frac = 1.2, dir_tag = "folder_split_", file_num_limit = 4000, debug=F ){
  
  dinfo <- hdfs.ls(path, recurse = T)
  
  # chronological ordering (mod time)
  dinfo <- dinfo[ order(dinfo$modtime), ]
  dinfo$cum_size_in_MB <- cumsum(dinfo$size) / 1024^2
  
  # naive approach for grouping based on size only works if each file size is small enough
  dinfo$size_groups <- floor(dinfo$cum_size_in_MB/aprox_size_in_MB)
  dchk <- dinfo %>% group_by(size_groups) %>% dplyr::summarise(
    size = sum(size),
    num_file = n()
  )
  dchk$size_in_MB <- dchk$size / 1024^2
  
  if(max(dchk$size_in_MB) / aprox_size_in_MB > max_tolerance_frac | max(dchk$num_file) / file_num_limit > max_tolerance_frac ){
    # do fine grouping
    dinfo$size_groups_approx <- dinfo$size_groups
    dinfo$custom_cum_size_in_MB <- dinfo$size / 1024^2
    dinfo$sum_count <- 1
    dinfo$size_groups <- 0
    j <- 0
    
    for( i in 2:(nrow(dinfo))){
      if(dinfo$custom_cum_size_in_MB[i] + dinfo$custom_cum_size_in_MB[i-1] < aprox_size_in_MB &
         dinfo$sum_count[i] + dinfo$sum_count[i-1] < file_num_limit){
        dinfo$custom_cum_size_in_MB[i] <- dinfo$custom_cum_size_in_MB[i] + dinfo$custom_cum_size_in_MB[i-1]
        dinfo$sum_count[i] <- dinfo$sum_count[i] + dinfo$sum_count[i-1]
      }else{
        j <- j+1
      }
      dinfo$size_groups[i] <- j
    }
    
    dchk <- dinfo %>% group_by(size_groups) %>% dplyr::summarise(
      size = sum(size),
      num_file = n()
    )
    dchk$size_in_MB <- dchk$size / 1024^2
    
    if(max(dchk$size_in_MB / aprox_size_in_MB) > max_tolerance_frac){
      cat("\nSome file group size is more than the tolerance limit.\n")
    }
    
  }
  
  cat("\nTotal File Groups:", nrow(dchk),"\n")
  
  if(!debug){
    
    dlply(dinfo, "size_groups", function(d){
      gr_dir <- file.path(paste0(path,"/",dir_tag,d$size_groups[1]))
      hdfs.mkdir(paths = gr_dir)
      hdfs.move(src = d$file, dest = gr_dir)
    })
    
  }
  
  u <- list(info = dinfo, report = dchk)
  return(invisible(u))
}

regroup_files <- function(){
  
  flow_control <- options("flow_control_LDS")[[1]]
  
  size_chk <- main_data_flow$hdfs.get_folder_depth(flow_control$large_folder_hdfs_path)
  
  is_required_to_regroup <- T
  
  if(size_chk$depth>=1){
    avg_size_in_MB     <- mean(size_chk$folder_size_info$size_in_HDFS_in_bytes/1024^2)
    avg_num_in_folders <- mean(size_chk$folder_size_info$num_files)
    if(abs(flow_control$folder_group_aprox_size_in_MB - avg_size_in_MB) < 10 & avg_num_in_folders < flow_control$folder_group_file_num_limit*1.2 ){
      is_required_to_regroup<- F
    }
  }
  
  
  if( is_required_to_regroup ){
    # below command takes a lot time
    create_chrono_file_buckets(path = flow_control$large_folder_hdfs_path, 
                               aprox_size_in_MB = flow_control$folder_group_aprox_size_in_MB, 
                               dir_tag = "Large_Folder_Split_", 
                               file_num_limit = flow_control$folder_group_file_num_limit)
    main_data_flow$hdfs.clean_size_zero_folders(path = flow_control$large_folder_hdfs_path)
    
  }
  
  return(invisible(0))
  
}

patch_main_data_flow <- function(...){
  list_of_args <- list(...)
  current_args <- options("flow_control")[[1]]
  matched <- intersect(names(current_args), names(list_of_args))
  if(length(matched)>0){
    list_of_args <- list_of_args[ matched ]
    for(nm in matched){
      current_args[[nm]] <- list_of_args[[nm]]
    }
    cat("\nChanged", length(matched), "arguments\n")
    options(flow_control = current_args)
  }
}

insert_data_into_final_db <- function(){
  flow_control <- options("flow_control_LDS")[[1]]
  patch_main_data_flow( final_db = flow_control$db_final, 
                        staging_db = flow_control$db_intermediate_staging,
                        use_max_id_while_inserting = T, 
                        ignore_partition_check_while_inserting = T,
                        recommendation_of_direct_insert_by_test_for_common_ids = T)
  # verify and deploy insert
  main_data_flow_config <- options("flow_control")[[1]]
  if(main_data_flow_config$final_db == flow_control$db_final & main_data_flow_config$staging_db == flow_control$db_intermediate_staging){
    main_data_flow$insert_into_structurized_db()
  }
}

work_flow_unit <- function(config_part){
  
  # rebuild config
  after_rebuild_load.config(manual_config_part = config_part)
  
  # start flow
  begin_flow()
  
  # re-group if required only
  regroup_files()
  
  # sequential loading untill all is done
  it <- 1 
  repeat{
    
    cat(paste0("\n\nIteration number : ", it,"\n\n"))
    it <- it +1
    
    load_stat <- try(main_data_flow$work_flow())
    
    if(!is.logical(load_stat)){
      break
    }
    
    if(load_stat){
      break
    }
    Sys.sleep(2)
  }
  
  insert_data_into_final_db()
  
}

work_flow <- function(){
  
  part_configs <- rebuild_config()
  
  for(config in part_configs){
    try(work_flow_unit(config))
  }
  
}

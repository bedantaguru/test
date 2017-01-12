
load_flow_source <- function(source_file){
  
  handle <- list()
  
  if(file.exists(source_file)){
    source(file = source_file, local = T)
  }
  
  handle <- environment()
  
  return(invisible(handle))
}

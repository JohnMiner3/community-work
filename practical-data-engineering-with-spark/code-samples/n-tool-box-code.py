# Databricks notebook source
#
# Define function to find matching files
# 

# import libraries
import fnmatch

# define function
def get_file_list(path_txt, pattern_txt):
  
  # list of file info objects
  fs_lst = dbutils.fs.ls(path_txt)
  
  # create list of file names
  dir_lst = list()
  for f in fs_lst:
      dir_lst.append(f[1])
      
  # filter file names by pattern
  files_lst = fnmatch.filter(dir_lst, pattern_txt)
  
  # return list
  return(files_lst)

# COMMAND ----------

# 
#  Keep only the single delimited file
#

# Define function
def unwanted_file_cleanup(folder_name, file_name, file_ext):
  try:
    
    # define tmp dir
    tmp_dir = folder_name
    
    # find new file
    tmp_lst = get_file_list(tmp_dir, "part*." + file_ext)
    tmpfile_txt = tmp_dir + "/" + tmp_lst[0]

    # remove old file
    dbutils.fs.rm(file_name, recurse=False)
    
    # copy new file
    dbutils.fs.cp(tmpfile_txt, file_name)
    
    # remove tmp dir, clean up step
    dbutils.fs.rm(tmp_dir, recurse=True)
    
    return True
  
  except Exception as err:
    raise(err)


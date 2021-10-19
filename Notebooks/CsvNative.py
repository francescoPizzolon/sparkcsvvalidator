# Databricks notebook source
# debug flag
debug = False

dbutils.widgets.text("filename","sftp/feed/2020/01/01/feed_20200101.csv","filename")
filename=dbutils.widgets.get("filename")
if debug:
  print("filename: '"+str(filename)+"'")

raw_filename = "/mnt/raw/"+filename
if debug:
  print("raw_filename: '"+str(raw_filename)+"'")

dbfs_mount_folder = 'mnt'
raw_filesystem = 'raw'
cleaned_filesystem = 'cleaned'

# COMMAND ----------

# MAGIC %run ../Helpers/HelpersFunctions

# COMMAND ----------

# MAGIC %run ../Helpers/ConnectToDataLakeGen2

# COMMAND ----------

# MAGIC %run ../Helpers/MountFileSystem $file_system="raw" $mount_path="mnt"

# COMMAND ----------

# MAGIC %run ../Helpers/MountFileSystem $file_system="cleaned" $mount_path="mnt"

# COMMAND ----------

# MAGIC %run ../Helpers/MountFileSystem $file_system="elt" $mount_path="mnt"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

# clear any cache
spark.catalog.clearCache()

# parse input file
path_separator = "/"
file_extension_separator = "."
bad_records_folder = "bad"
bad_records_extension = "bad"
validation_error_col_name = "CLEAN_VALIDATION_ERROR"
rdd_cleaned_folder = "rddTmp"
rdd_bad_records_folder = "rddBadTmp"

source_system = raw_filename.split(path_separator)[3]
feed_name = raw_filename.split(path_separator)[4]
feed_year = raw_filename.split(path_separator)[5]
feed_month = raw_filename.split(path_separator)[6]
feed_day = raw_filename.split(path_separator)[7]
feed_filename = raw_filename.split(path_separator)[-1]
feed_filename_no_extenstion = feed_filename.split(file_extension_separator)[0]
raw_file_path = raw_filename.replace(feed_filename, "")

# construct cleaned records filename and path
cleaned_path = path_separator + dbfs_mount_folder + path_separator + cleaned_filesystem + path_separator + source_system + path_separator + feed_name + path_separator + feed_year + path_separator + feed_month + path_separator + feed_day
cleaned_filename = feed_filename_no_extenstion+".parquet"

# construct rdd cleaned records filename and path
rdd_cleaned_path = cleaned_path + path_separator + rdd_cleaned_folder

# construct bad records filename and path
bad_records_path = cleaned_path + path_separator + bad_records_folder
bad_records_filename = feed_filename + file_extension_separator + bad_records_extension

# construct rdd bad records filename and path
rdd_bad_records_path = cleaned_path + path_separator + rdd_bad_records_folder

# remove nullable and bad folders
dbutils.fs.rm(bad_records_path, True)
# remove cleaned csv (if any) -> for re-runs
dbutils.fs.rm(cleaned_path + path_separator + cleaned_filename)
# remove rdd folders
dbutils.fs.rm(rdd_cleaned_path, True)
dbutils.fs.rm(rdd_bad_records_path, True)

csv_schema = StructType([
  StructField("FIELD_1", StringType(), True),
  StructField("FIELD_2", StringType(), True),
  StructField("FIELD_3", IntegerType(), True),
  StructField("FIELD_4", IntegerType(), True),
  StructField("FIELD_5", DoubleType(), True),
  StructField("FIELD_6", DoubleType(), True),
  StructField("FIELD_7", TimestampType(), True),
  StructField("FIELD_8", TimestampType(), True)])

#### READ THE CSV IN PERMISSIVE MODE #####
permissive_df = sqlContext.read.format('csv').options(header='true', mode='PERMISSIVE', timestampFormat='yyyy/MM/dd HH:mm:ss').schema(csv_schema).load(raw_filename)
#display(permissive_df)

#### READ THE CSV IN DROPMALFORMED MODE #####
dropmalformed_df = sqlContext.read.format('csv').options(header='true', mode='DROPMALFORMED', timestampFormat='yyyy/MM/dd HH:mm:ss').schema(csv_schema).load(raw_filename)
display(dropmalformed_df)

#### READ THE CSV IN FAILFAST MODE #####
failfast_df = sqlContext.read.format('csv').options(header='true', mode='FAILFAST', timestampFormat='yyyy/MM/dd HH:mm:ss').schema(csv_schema).load(raw_filename)
#display(failfast_df)


# COMMAND ----------



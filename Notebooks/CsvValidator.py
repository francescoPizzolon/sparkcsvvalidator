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

# get feed schema from raw/elt/schema/*
csv_string_schema = None
csv_typed_schema = None
try:
  csv_string_schema_code, expected_header, not_nullable_cols, cols_data_types, csv_typed_schema_code, datetime_py_format, datetime_spark_format = GenerateDynamicSchema(feed_name)
  exec(csv_string_schema_code)
  exec(csv_typed_schema_code)
except:
  raise Exception("Failed to generate dynamic schema...")

if(debug):
  print("csv_string_schema: "+str(csv_string_schema))
  print("expected_header: "+str(expected_header))
  print("not_nullable_cols: "+str(not_nullable_cols))
  print("cols_data_types: "+str(cols_data_types))
  print("csv_typed_schema: "+str(csv_typed_schema))

expected_columns_no = len(expected_header.split(','))
columns = expected_header.split(',')

#### READ THE CSV IN PERMISSIVE MODE #####
csv_no_schema = sqlContext.read.format('csv').options(inferSchema='false', header='false', mode='PERMISSIVE').load(raw_filename)
csv_no_schema_count = csv_no_schema.count()
print("["+str(csv_no_schema_count)+"] total number of rows found in " + str(feed_filename) + "...")
actual_row_count = csv_no_schema.count()-1


#### VALIDATE HEADER #####
try:
  header_compliance_flag, actual_columns_no, first_row, actual_header = validateHeader(csv_no_schema, expected_header)
except:
  raise Exception("Error in validating header for " + str(feed_filename))
if(not(header_compliance_flag)):
  print("Expecting "+str(expected_columns_no)+" columns in header but "+str(actual_columns_no)+" were found, header is invalid...")
  print("Expected header: "+str(expected_header))
  print("Actual header: "+str(actual_header))
  raise Exception("Invalid header found in " + str(feed_filename))
else:
  print("["+str(actual_columns_no)+"/"+str(len(columns))+"] columns found in " + str(feed_filename) + ", header is valid...")

# ASSUMPTION HERE: HEADER IS VALID
data_df = sqlContext.read.format('csv').options(header='true', mode='PERMISSIVE').schema(csv_string_schema).load(raw_filename)

##### CLEAN THE CSV FEED, HOLD YOUR HATS [|:/ #####
# cleanCsvRow is a map function that adds a column (validation_error_col_name) at the end of the rdd record with the parse error, if any
clean_map_rdd = data_df.rdd.map(cleanCsvRow)

# get valid records
valid_rdd = clean_map_rdd.filter(lambda x: x[-1] == '').map(lambda x: x[:-1])
valid_data_count = valid_rdd.count()
print("valid record count: "+str(valid_data_count))
if(valid_data_count != 0):
  valid_rdd.map(toCSVLine).saveAsTextFile(rdd_cleaned_path)

# optimise: there is no need to compute bad records if we don't have any
bad_data_count = 0
if (valid_data_count != actual_row_count):
  # get bad records
  bad_rdd = clean_map_rdd.filter(lambda x: x[-1] != '')
  bad_data_count = bad_rdd.count()
  print("bad record count: "+str(bad_data_count))
  if(bad_data_count != 0):
    bad_rdd.map(toCSVLine).saveAsTextFile(rdd_bad_records_path)

# COMMAND ----------

if(valid_data_count != 0):
  if debug:
    print("rdd_cleaned_path: "+rdd_cleaned_path)
    print("cleaned_path: "+cleaned_path)
    print("cleaned_filename: "+cleaned_filename)
  # we are going to enforce a schema here, so that we can be confident in the clean to aligned notebook that these records actually have the right schema
  writeValidCsvRecordsToFileSystem(rdd_cleaned_path, cleaned_path, cleaned_filename, csv_typed_schema, datetime_spark_format, path_separator, valid_data_count)

# COMMAND ----------

if(bad_data_count != 0):
  csv_bad_schema = csv_string_schema
  csv_bad_schema.add(validation_error_col_name, StringType())
  if debug:
    print("rdd_bad_records_path: "+rdd_bad_records_path)
    print("bad_records_path: "+bad_records_path)
    print("bad_records_filename: "+bad_records_filename)
  writeBadCsvRecordsToFileSystem(rdd_bad_records_path, bad_records_path, bad_records_filename, csv_string_schema, path_separator, bad_data_count, validation_error_col_name)

# COMMAND ----------

if (bad_data_count != 0):
  raise Exception(str(feed_filename) + " has not passed cleaned validation...")
print(str(feed_filename)+" has passed cleaned validation...")

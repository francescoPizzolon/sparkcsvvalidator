# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

def writeCsvDataframeToFileSystem(csv_df, destination_path, filename, header_flag):
  """Writes a CSV dataframe to a destination.

      Parameters:
      csv_df (pyspark.sql.DataFrame): The CSV pyspark dataframe
      destination_path (string): The destination path from dbfs:/, must be valid
      filename (string): the filename
      quote (string): quote character for writing the CSV

      Returns:
      None
     """
  path_separator = '/'
  tmp_folder = 'tmp'
  tmp_folder_path = destination_path + path_separator + tmp_folder

  # save csv df with coalesce to generate only one file - this might affect performance for big files
  csv_df.coalesce(1).write.save(path=tmp_folder_path, format='csv', mode='overwrite', header=header_flag, nullValue=None)

  # remove spark job files
  fileList = dbutils.fs.ls(tmp_folder_path)
  for files in fileList:
    if files.name.startswith("_"):
      dbutils.fs.rm(tmp_folder_path + path_separator + files.name)

  # rename file
  dbutils.fs.mv(dbutils.fs.ls(tmp_folder_path)[0].path, destination_path + path_separator + filename)
  dbutils.fs.rm(tmp_folder_path)

def validateHeader(csv_df_no_schema, expected_header):
  """Validates the header of a CSV dataframe with no schema.

        Parameters:
        csv_df_no_schema (pyspark.sql.DataFrame): The CSV pyspark dataframe with **NO** schema
        expected_header (string): The expected header

        Returns:
        bool: True if header is as expected, otherwise False
        columns_no: The actual header length
        first_row: First row of csv_df_no_schema
        csv_header: The actual header of csv_df_no_schema
       
       """
  first_row = csv_df_no_schema.first()
  columns_no = len(first_row)
  csv_header = ','.join(first_row)
  if (expected_header == csv_header):
    return True, columns_no, first_row, csv_header
  else:
    return False, columns_no, csv_header
  return False, columns_no, first_row, csv_header

def writeValidCsvRecordsToFileSystem(rdd_folder, destination_folder, destination_filename, csv_typed_schema, timestamp_format, path_separator, valid_data_count):
  """Writes only valid records of a dataframe generated from reading all files from rdd_folder, applying csv_typed_schema. Triggers error if the dataframe valid records do not match valid_data_count.

        Parameters:
        rdd_folder (string): The folder where the RDD files to merge are.
        destination_folder (string): The destination folder path.
        destination_filename (string): The destination filename.
        csv_typed_schema (string): The csv typed schema [currently supports IntegerType, TimestampType and FloatType]
        timestamp_format (string): The timestamp format to read and write TimestampType columns.
        path_separator (string): folder path separator.
        valid_data_count (string): initial valid data count (tipycall from the RDD).
       
       """
  tmp_folder_name = "tmp"
  
  # remove spark operational files from rdd folder
  fileList = dbutils.fs.ls(rdd_folder)
  for files in fileList:
    if files.name.startswith("_"):
      dbutils.fs.rm(rdd_folder + path_separator + files.name)

  # read RDD files
  df = sqlContext.read.format('csv').options(header='false', mode='DROPMALFORMED', timestampFormat=timestamp_format).schema(csv_typed_schema).load(rdd_folder+"/*")
  
  # check rowcount
  df_count = df.count()
  print("["+str(df_count)+"/"+str(valid_data_count)+"] casted from RDD to dataframe...")
  if df_count != valid_data_count:
    raise Exception("Something went wrong in applying the typed schema...")
    
  # write a single csv file
  tmp_destination_folder = destination_folder + path_separator + tmp_folder_name
  df.coalesce(1).write.save(path=tmp_destination_folder, format='parquet', mode='overwrite', header='true', timestampFormat='yyyy/MM/dd HH:mm:ss', nullValue=None)

  # remove spark job files
  fileList = dbutils.fs.ls(tmp_destination_folder)
  for files in fileList:
    if files.name.startswith("_"):
      dbutils.fs.rm(tmp_destination_folder + path_separator + files.name)

  # rename csv file
  dbutils.fs.mv(dbutils.fs.ls(tmp_destination_folder)[0].path, destination_folder + path_separator + destination_filename)
  dbutils.fs.rm(tmp_destination_folder, True)
  dbutils.fs.rm(rdd_folder, True)

def writeBadCsvRecordsToFileSystem(rdd_bad_folder, destination_bad_folder, destination_bad_filename, csv_string_schema, path_separator, bad_data_count, validation_error_column_name):
  """Writes only bad records of a dataframe generated from reading all files from rdd_bad_folder, applying csv_string_schema. Triggers error if the dataframe valid records do not match bad_data_count.

        Parameters:
        rdd_bad_folder (string): The folder where the RDD files containing the bad records are.
        destination_bad_folder (string): The bad destination folder path.
        destination_bad_filename (string): The bad destination filename.
        csv_string_schema (string): The csv string schema, does not support any types (obviously).
        path_separator (string): folder path separator.
        bad_data_count (string): initial bad data count (tipycall from the bad RDD).
        validation_error_column_name(string): the validation error column name.
       
       """
  tmp_folder_name = "tmp"
  
  # remove spark operational files from rdd folder
  fileList = dbutils.fs.ls(rdd_bad_folder)
  for files in fileList:
    if files.name.startswith("_"):
      dbutils.fs.rm(rdd_bad_folder + path_separator + files.name)

  # read RDD files
  df = sqlContext.read.format('csv').options(inferSchema='false', header='false', mode='PERMISSIVE').schema(csv_string_schema).load(rdd_bad_folder+"/*")
  # check rowcount
  df_count = df.count()
  print("["+str(df_count)+"/"+str(bad_data_count)+"] bad records copied from RDD to dataframe...")
  if df_count != bad_data_count:
    raise Exception("Something went wrong in writing the bad records...")
    
  # write a single csv file
  tmp_destination_bad_folder = destination_bad_folder + path_separator + tmp_folder_name
  df.coalesce(1).write.save(path=tmp_destination_bad_folder, format='csv', mode='overwrite', header='true')
  
  # remove spark job files
  fileList = dbutils.fs.ls(tmp_destination_bad_folder)
  for files in fileList:
    if files.name.startswith("_"):
      dbutils.fs.rm(tmp_destination_bad_folder + path_separator + files.name)

  # rename csv file
  dbutils.fs.mv(dbutils.fs.ls(tmp_destination_bad_folder)[0].path, destination_bad_folder + path_separator + destination_bad_filename)
  dbutils.fs.rm(tmp_destination_bad_folder, True)
  dbutils.fs.rm(rdd_bad_folder, True)

def toCSVLine(data):
  """Merges all element of data into a string, the separator is a comma.

        Parameters:
        data (list): The list to join.
        
        Returns:
        All casted element of data to string, concatenated with a comma
       """
  return ','.join(str(d) for d in data)

def cleanCsvRow(row): 
  """Cleans a CSV Row.

        Parameters:
        row (pyspark.sql.Row): The RDD row.
        
        Pre-req:
        1. not_nullable_cols is a list of not nullables field names
        2. cols_data_types is a list of python data types corresponding to the csv columns
        
        Returns:
        tempRow: the new row with a validation_error column containig a comprehensive error as to why the record is valid or not
       """
  tempRow = []
  i = 0
  validation_error = ''
  for field_value in row:
    field_name = columns[i]
    field_data_type = cols_data_types[i]
    # check nullables
    if(field_name in not_nullable_cols and field_value is None):
      validation_error = validation_error + field_name +" is null|"
      tempRow.append("")
    else:
      # try to cast only for not strings
      if (field_name not in not_nullable_cols and field_value is None):
        tempRow.append("")
      else:
        if field_data_type != "str":
          try:
            if field_data_type == 'datetime':
              datetype_cast = str(datetime.datetime.strptime((row[field_name]), datetime_py_format))
            else:
              dynamic_cast_code = "type_cast = "+field_data_type+"(row[field_name])"
              exec(dynamic_cast_code)
          except:
            validation_error = validation_error + field_name + " is not " + field_data_type + "|"
        if row[field_name] is not None:
          if field_data_type == 'str' or field_data_type == 'datetime':
            tempRow.append('"'+str(row[field_name]).replace('"','\\"')+'"')
          else:
            tempRow.append(row[field_name])
        else:
          tempRow.append("")
    i += 1
  validation_error = validation_error[:-1]
  tempRow.append(validation_error)
  return tempRow

def GenerateDynamicSchema(feed_name):
  """Generates string schema, typed, schema, header and nullable columns list for a given feedname. The scheman must be available in json format from '/mnt/raw/etl/schema/feed_name.schema.json'. The data types supported are string, int, float and datetime. The json schema has to have the following format:
    {
     feed_name: "feed_name",  # the feed name
     datetime_py_format: "%Y/%M/%d %H:%M:%S" # the date time columns timestamp format
     datetime_spark_format: # the datetime_py_format correspondent spark timestamp format
     "schema":[
       {
         "name": "COLUMN_0",
         "position": 0,
         "type": "str",
         "is_nullable": 0
       },
       {
         "name": "COLUMN_1",
         "position": 1,
         "type": "int",
         "is_nullable": 1
       }
     ]
    }

      Parameters:
        feed_name (string): The feed name.

      Returns:
        csv_string_schema_code: the dynamic code to execute to compute the feed's string schema.
        expected_header: the expected header, comma separated.
        not_nullable_cols: the list of not nullable cols.
        cols_data_types: the list of columnd data types in python.
        csv_typed_schema_code: the dynamic code to execute to compute the feed's typed schema.
        datetime_py_format: the datetime in python format.
        datetime_spark_format: the datetime in spark format.
       """
  map_py_spark_types = {'str': 'StringType', 'int': 'IntegerType', 'float': 'FloatType','datetime': 'TimestampType', 'double': 'DoubleType'}

  col_schema = StructType([
    StructField("feed_name", StringType()),
    StructField("datetime_py_format", StringType()),
    StructField("datetime_spark_format", StringType()),
    StructField("schema", ArrayType(
        StructType([
          StructField('name', StringType(), True),
          StructField('position', IntegerType(), True),
          StructField('type', StringType(), True),
          StructField('is_nullable', IntegerType(), True)
        ])
     ))
  ])

  df = sqlContext.read.json("/mnt/elt/schemas/"+feed_name+".schema.json", col_schema)

  json_feed_name = str(df.first().feed_name)
  datetime_py_format = str(df.first().datetime_py_format)
  datetime_spark_format = str(df.first().datetime_spark_format)

  # VALIDATE FEED NAME
  if(json_feed_name != feed_name):
    raise Exception("["+json_feed_name+" != "+str(feed_name)+"]: json feed name mismatch with input feed")

  # GET COLUMNS
  columns_df = (df.withColumn("schema", explode(col("schema"))).select(col('schema')))
  columns_list = columns_df.select("schema").rdd.flatMap(lambda x: x).collect()

  # ORDER COLUMNS
  sorted_column_list = [None] * len(columns_list)
  for row in columns_list:
    sorted_column_list[row.position] = [row.name, row.type, row.is_nullable]

  # GENERATE STRING SCHEMA, EXPECTED HEADER, NOT NULLABLE COLUMNS
  csv_string_schema_code = "csv_string_schema = StructType(["
  expected_header = ""
  not_nullable_cols = []
  cols_data_types = []
  csv_typed_schema_code = "csv_typed_schema = StructType(["

  for i in range(0,len(columns_list)):
    original_column_name = str(sorted_column_list[i][0])
    column_name = original_column_name.replace(" ", "_")
    column_type = sorted_column_list[i][1]
    is_nullable = sorted_column_list[i][2]

    csv_string_schema_code = csv_string_schema_code + "StructField(\""+column_name+"\", StringType(), True), "
    expected_header = expected_header + original_column_name + ","
    if is_nullable == 0:
      not_nullable_cols.append(column_name)
    if column_type not in map_py_spark_types:
      raise Exception(column_type+" is not yet supported!")
    else:
      cols_data_types.append(column_type)
      csv_typed_schema_code = csv_typed_schema_code + "StructField(\""+column_name+"\", "+str(map_py_spark_types[column_type])+"(), True), "

  # csv_string_schema
  csv_string_schema_code = csv_string_schema_code[:-2] + "])"

  # expected_header
  expected_header = expected_header[:-1]

  # csv_typed_schema
  csv_typed_schema_code = csv_typed_schema_code[:-2] + "])"
  
  return csv_string_schema_code, expected_header, not_nullable_cols, cols_data_types, csv_typed_schema_code, datetime_py_format, datetime_spark_format

# COMMAND ----------

# when adding a function, add a description and print it here
print(writeCsvDataframeToFileSystem.__name__ + ": \n"+writeCsvDataframeToFileSystem.__doc__)
print(validateHeader.__name__ + ": \n"+validateHeader.__doc__)
print(writeValidCsvRecordsToFileSystem.__name__ + ": \n"+writeValidCsvRecordsToFileSystem.__doc__)
print(writeBadCsvRecordsToFileSystem.__name__ + ": \n"+writeBadCsvRecordsToFileSystem.__doc__)
print(toCSVLine.__name__ + ": \n"+toCSVLine.__doc__)
print(cleanCsvRow.__name__ + ": \n"+cleanCsvRow.__doc__)
print(GenerateDynamicSchema.__name__ + ": \n"+GenerateDynamicSchema.__doc__)

import sys
from awsglue.transforms import *

from datetime import datetime
import boto3
import botocore

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','target_databasename','target_tablename','target_bucketname'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def table_exists(target_database,table_name):
    try:
        glue_client=boto3.client("glue")
        glue_client.get_table(DatabaseName=target_database,Name=table_name)
        return True
    except:
        return False

def create_database():
    response = None
    table_response = None
    
    glue_client = boto3.client('glue')
    database_name = args['target_databasename']
    
    try:
        # global response
        response = glue_client.get_database(
            Name=database_name
        )
        print(response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            print('The requested database: {0} was not found.'.format(database_name))
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))

    if response is None:
        print('I am going to create a database')
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print("create_database_response: ", response) 

def upsert_catalog_table(df,target_database,table_name,classification,storage_location):
    
    create_database()
    df_schema=df.dtypes
    schema=[]
    for s in df_schema:
        if s[1]=="decimal(10,0)":
            v={"Name": s[0], "Type": "int" }
        elif s[1]=="null":
            v={"Name": s[0], "Type": "string" }
        else:
            v={"Name": s[0], "Type": s[1] }
        schema.append(v)
    
    print(schema)
    #schema = transform.source_schema
    input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    serde_info = {'Parameters': {'serialization.format': '1'},
                  'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}
    storeage_descriptor={'Columns': schema, 'InputFormat': input_format, 'OutputFormat': output_format, 'SerdeInfo': serde_info,
                'Location': storage_location}
    #print(storeage_descriptor)
    
    #partition_key=[{'Name':'year', 'Type':'string'},{'Name':'month', 'Type':'string'},{'Name':'day', 'Type':'string'}]
    table_input = {'Name': table_name,
                  'StorageDescriptor' : storeage_descriptor, #self.get_storage_descriptor(target.output_format, schema, target.table_name),
                  'Parameters': {'classification': classification,
                                  'SourceType': "s3",
                                  'SourceTableName': table_name,#[len(source.source_prefix):],
                                #   'CreatedByJob': target.job_name,
                                #   'CreatedByJobRun': target.job_run_id,
                                  'TableVersion': "0"},
                  'TableType': 'EXTERNAL_TABLE'
                  #'PartitionKeys':partition_key
                  
                  }
    #print(table_input)
    
    glue_client=boto3.client('glue')
    
    
    if not table_exists(target_database,table_name):
        print("[INFO] Target Table name: {} does not exist.".format(table_name))
        glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
    else:
        print("[INFO] Trying to update: TargetTable: {}".format(table_name))
        glue_client.update_table(DatabaseName=target_database, TableInput=table_input) 

def main():
    
    year=datetime.today().year
    month=datetime.today().month
    day=datetime.today().day
        

    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "datalake_blog_nyc_stage", table_name = "nyc_taxi_data", transformation_ctx = "datasource0")
    datasource0.show(5)
    

    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vendorid", "long", "vendorid", "long"), ("tpep_pickup_datetime", "string", "tpep_pickup_datetime", "string"), ("tpep_dropoff_datetime", "string", "tpep_dropoff_datetime", "string"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("pulocationid", "long", "pulocationid", "long"), ("dolocationid", "long", "dolocationid", "long"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("congestion_surcharge", "double", "congestion_surcharge", "double")], transformation_ctx = "applymapping1")

    resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")

    dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
    dropnullfields3.show(5)

    
    df=dropnullfields3.toDF()
    partition_path= "year={}/".format(year) + "month={}/".format(month) + "day={}/".format(day)
    target_s3_location="s3://" + args['target_bucketname']+"/datalake_blog/"
    storage_location=target_s3_location + args['target_tablename'] + "/" + partition_path
    upsert_catalog_table(df,args['target_databasename'],args['target_tablename'],'PARQUET',storage_location)
    
    
    
    dynamic_df=DynamicFrame.fromDF(df,glueContext,"table_df")
    
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": storage_location}, format = "parquet", transformation_ctx = "datasink4")
    job.commit()
    
if __name__=="__main__":
    main()
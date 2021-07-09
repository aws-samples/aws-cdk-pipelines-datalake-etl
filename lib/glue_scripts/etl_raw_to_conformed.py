# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

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


# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'target_databasename',
        'target_bucketname',
        'source_bucketname',
        'source_key',
        'base_file_name',
        'p_year',
        'p_month',
        'p_day',
        'table_name'
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def table_exists(target_database, table_name):
    """
    Function to check if table exists returns true/false

    @param target_database:
    @param table_name:
    """
    try:
        glue_client = boto3.client('glue')
        glue_client.get_table(DatabaseName=target_database, Name=table_name)

        return True
    except:
        return False


def create_database():
    """
    Function to create catalog database if does not exists
    @return:
    """
    response = None

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
        print('create_database_response: ', response)


def upsert_catalog_table(df, target_database, table_name, classification, storage_location):
    """
    Function to upsert catalog table
    @param df:
    @param target_database:
    @param table_name:
    @param classification:
    @param storage_location:
    """
    create_database()
    df_schema = df.dtypes
    schema = []
    for s in df_schema:
        if s[1] == 'decimal(10,0)':
            print('converting decimal(10,0) to int')
            v = {'Name': s[0], 'Type': 'int'}
        elif s[1] == 'null':
            v = {'Name': s[0], 'Type': 'string'}
        else:
            v = {'Name': s[0], 'Type': s[1]}
        schema.append(v)

    print(schema)
    input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    serde_info = {
        'Parameters': {
            'serialization.format': '1'
        },
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    }
    storage_descriptor = {
        'Columns': schema,
        'InputFormat': input_format,
        'OutputFormat': output_format,
        'SerdeInfo': serde_info,
        'Location': storage_location
    }

    partition_key = [
        {'Name': 'year', 'Type': 'string'},
        {'Name': 'month', 'Type': 'string'},
        {'Name': 'day', 'Type': 'string'},
    ]
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'Parameters': {
            'classification': classification,
            'SourceType': 's3',
            'SourceTableName': table_name,
            'TableVersion': '0'
        },
        'TableType': 'EXTERNAL_TABLE',
        'PartitionKeys': partition_key
    }

    try:
        glue_client = boto3.client('glue')
        if not table_exists(target_database, table_name):
            print('[INFO] Target Table name: {} does not exist.'.format(table_name))
            glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
        else:
            print('[INFO] Trying to update: TargetTable: {}'.format(table_name))
            glue_client.update_table(DatabaseName=target_database, TableInput=table_input)
    except botocore.exceptions.ClientError as error:
        print('[ERROR] Glue job client process failed:{}'.format(error))
        raise error
    except Exception as e:
        print('[ERROR] Glue job function call failed:{}'.format(e))
        raise e


def add_partition(rec):
    """
    Function to add partition
    """
    partition_path = '{}/'.format(args['p_year']) + '{}/'.format(args['p_month']) + '{}/'.format(args['p_day'])
    rec['year'] = args['p_year']
    rec['month'] = args['p_month']
    rec['day'] = args['p_day']
    print(partition_path)

    return rec


def main():
    source_path = 's3://' + args['source_bucketname'] + '/' + args['source_key'] + '/' + args['base_file_name']
    print(source_path)

    df = spark.read.format('csv') \
        .option('header', 'true') \
        .option('delimiter', ',') \
        .option('inferSchema', 'true') \
        .option('mode', 'DROPMALFORMED') \
        .load(source_path)

    target_s3_location = 's3://' + args['target_bucketname'] + '/datalake_blog/'
    storage_location = target_s3_location + args['table_name']
    upsert_catalog_table(df, args['target_databasename'], args['table_name'], 'PARQUET', storage_location)

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('hive.exec.dynamic.partition', 'true')
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

    dynamic_df = DynamicFrame.fromDF(df, glueContext, 'table_df')
    dynamic_df.show(5)
    mapped_dyF = Map.apply(frame=dynamic_df, f=add_partition)
    df_final = mapped_dyF.toDF()
    df_final.show(5)
    # get dataframe schema
    my_schema = list(df_final.schema)
    print(my_schema)
    null_cols = []

    # iterate over schema list to filter for NullType columns
    for st in my_schema:
        if str(st.dataType) == 'NullType':
            null_cols.append(st)

    # cast null type columns to string (or whatever you'd like)
    for ncol in null_cols:
        mycolname = str(ncol.name)
        df_final = df_final.withColumn(mycolname, df_final[mycolname].cast('string'))

    df_final.show(5)
    df_final.write.partitionBy('year', 'month', 'day').format('parquet').save(storage_location, mode='overwrite')

    target_table_name = args['target_databasename'] + '.' + args['table_name']
    spark.sql(f'ALTER TABLE {target_table_name} RECOVER PARTITIONS')

    job.commit()


if __name__ == '__main__':
    main()

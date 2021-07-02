# AWS CDK Pipelines for Data Lake ETL Deployment

This solution helps you deploy ETL processes on data lake using [AWS CDK Pipelines](https://docs.aws.amazon.com/cdk/latest/guide/cdk_pipeline.html).

[CDK Pipelines](https://docs.aws.amazon.com/cdk/api/latest/docs/pipelines-readme.html) is a construct library module for painless continuous delivery of CDK applications. CDK stands for Cloud Development Kit. It is an open source software development framework to define your cloud application resources using familiar programming languages.

This solution helps you to:

1. deploy ETL workloads on data lake
1. build CDK applications for your ETL workloads
1. deploy ETL workloads from a central deployment account to multiple AWS environments such as dev, test, and prod
1. leverage the benefit of self-mutating feature of CDK Pipelines. For example, whenever you check your CDK app's source code in to your version control system, CDK Pipelines can automatically build, test, and deploy your new version
1. increase the speed of prototyping, testing, and deployment of new ETL workloads

---

## Contents

* [Conceptual Data Lake](#conceptual-data-lake)
* [Engineering the Data Lake](#engineering-the-data-lake)
* [Data Lake Infrastructure](#data-lake-infrastructure)
* [ETL Use Case](#etl-use-case)
* [Centralized Deployment](#centralized-deployment)
* [Continuous Delivery of ETL Jobs using CDK Pipelines](#continuous-delivery-of-etl-jobs-using-cdk-pipelines)
* [Source Code Structure](#source-code-structure)
* [Deployment Overview](#deployment-overview)
* [Before Deployment](#before-deployment)
  * [AWS Environment Bootstrapping](#aws-environment-bootstrapping)
  * [Application Configuration](#application-configuration)
  * [Integration of AWS CodePipeline and GitHub.com](#integration-of-aws-codepipeline-and-github.com)
* [Deployment](#deployment)
  * [Deploying for the first time](#deploying-for-the-first-time)
  * [Iterative Deployment](#iterative-deployment)
* [Clean Up](#clean-up)
* [AWS CDK](#aws-cdk)
* [Developer Instructions](#developer-instructions)
* [Contributors](#contributors)
* [License Summary](#license-summary)

---

## Conceptual Data Lake

To level set, let us design a data lake. As shown in the figure below, we use Amazon S3 for storage. We use three S3 buckets - 1) raw bucket to store raw data in its original format 2) conformed bucket to store the data that meets the quality requirements of the lake 3) purpose-built data that is used by analysts and data consumers of the lake.

The Data Lake has one producer which ingests files into the raw bucket. We use AWS Lambda and AWS Step Functions for orchestration and scheduling of ETL workloads.

We use AWS Glue for ETL and data cataloging, Amazon Athena for interactive queries and analysis. We use various AWS services for logging, monitoring, security, authentication, authorization, notification, build, and deployment.

**Note:** [AWS Lake Formation](https://aws.amazon.com/lake-formation/) is a service that makes it easy to set up a secure data lake in days. [Amazon QuickSight](https://aws.amazon.com/quicksight/) is a scalable, serverless, embeddable, machine learning-powered business intelligence (BI) service built for the cloud. These two services are not used in this solution.

![Conceptual Data Lake](./resources/Aws-cdk-pipelines-blog-datalake-data_lake.png)

---

## Engineering the Data Lake

To engineer the data lake, we will create two source code repositories. They are:

1. source code repository for data lake infrastructure
1. source code repository for ETL process

In a general sense, one code repo is sufficient for the whole data lake infrastructure and each ETL process has its own code repo. We will apply this principle to this solution.

---

## Data Lake Infrastructure

Now we have the Data Lake design, let's deploy its infrastructure. You can use [AWS CDK Pipelines for Data Lake Infrastructure Deployment](https://github.com/aws-samples/aws-cdk-pipelines-datalake-infrastructure) for this purpose.

---

## ETL Use Case

To demonstrate the above benefits, we will use [NYC Taxi and Limousine Commission Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and build a sample ETL process for this. In our Data Lake, we have three S3 buckets - Raw, Conformed, and Purpose-built.

Figure below represents the infrastructure resources we provision for Data Lake.

1. A file server uploads files to S3 raw bucket of the data lake. Here file server is a data producer/source for the data lake. Assumption is the data will be pushed to the raw bucket
1. Amazon S3 triggers an event notification to AWS Lambda Function
1. AWS Lambda function inserts an item in DynamoDB table
1. AWS Lambda function Starts an execution of AWS Step Functions State machine
1. Runs a Glue Job – Initiate glue job in sync mode
1. Glue job – Spark glue job will process the data from raw to conform. Source data is provided in csv format and will be converted to the parquet format
1. After creating parquet data will update the Glue Data Catalog table
1. Runs a Glue Job – initiates data processing from conform to purpose-built in sync mode
1. Glue Job – conformed to purpose-built fetches data transformation rules from DynamoDB table 
1. Stores the result in parquet format within purpose-built zone
1. Glue job updates the Data Catalog table
1. Updates DynamoDB table with job status
1. Sends SNS notification
1. Data engineers or analysts analyze data using Amazon Athena


![Data Lake Infrastructure Architecture](./resources/Aws-cdk-pipelines-blog-datalake-etl.png)

---

## Centralized Deployment

Let us see how we deploy data lake ETL workloads from a central deployment account to multiple AWS environments such as dev, test, and prod. As shown in the figure below, we organize **Data Lake ETL source code** into three branches - dev, test, and production. We use a dedicated AWS account to create CDK Pipelines. Each branch is mapped to a CDK pipeline and it turn mapped to a target environment. This way, code changes made to the branches are deployed iteratively to their respective target environment.

![Alt](./resources/Aws-cdk-pipelines-blog-datalake-branch_strategy_etl.png)

## Continuous Delivery of ETL Jobs using CDK Pipelines

Figure below illustrates the continuous delivery of ETL jobs on Data Lake.

![Alt](./resources/Aws-cdk-pipelines-blog-datalake-continuous_delivery_data_lake_etl.png)

There are few interesting details to point out here:

1. DevOps administrator checks in the initial code
1. DevOps administrator performs a one-time `cdk deploy --all` to deploy the data lake infrastructure to all target environments
1. CDK pipelines executes a multi-stage pipeline that includes, cloning the source code from GitHub repo, build the code, publish artifacts to S3 bucket, executes one or more stages/ Deployment of infrastructure resources is one of the stages
1. CDK pipelines deploy the resources to dev, test, and prod environments

---

## Source Code Structure

Table below explains how this source ode structured:

  | File / Folder    | Description  |
  |------------------| -------------|
  | [app.py](aws_cdk_pipelines_blog_datalake_infrastructure/app.py) | Application entry point |
  | [pipeline_stack](aws_cdk_pipelines_blog_datalake_infrastructure/pipeline_stack.py) | Pipeline stack entry point |
  | [pipeline_deploy_stage](aws_cdk_pipelines_blog_datalake_infrastructure/pipeline_deploy_stage.py) | Pipeline deploy stage entry point |
  | [glue_stack](aws_cdk_pipelines_blog_datalake_infrastructure/glue_stack.py) | Stack creates Glue Jobs and supporting resources such as Connections, S3 Buckets - script and temporary - and an IAM execution Role |
  | [step_functions_stack](aws_cdk_pipelines_blog_datalake_infrastructure/step_functions_stack.py) | Stack creates an ETL State machine which triggers Glue Jobs and supporting Lambdas - trigger and notification. |
  | [dynamodb_stack](aws_cdk_pipelines_blog_datalake_infrastructure/dynamodb_stack.py)) | Stack creates DynamoDB Tables for Job Auditing and ETL transformation rules. |
  | *lib/glue_scripts*| Glue spark job data processing logic for conform and purposebuilt layers |
  | *lib/datalake_blog_failure_status_update* | lambda script to update dynamodb in case of glue job failure  |
  | *lib/datalake_blog_success_status_update* | lambda script to update dynamodb for successful glue job execution |
  *lib/datalake_blog_trigger_load* | lambda script to trigger step function and intiate dynamodb |
  | *lib/dynamodb_config* | Transformation logic to be used for data processing from conform to purpose built |
  | *resources*| This folder has architecture and process flow diagrams for Infrastructure and CDK Pipelines |

---

## Before Deployment

This project is dependent on the [AWS CDK Pipelines for Data Lake Infrastructure Deployment](https://github.com/aws-samples/aws-cdk-pipelines-datalake-infrastructure). Please reference the [Prerequisites section in README](https://github.com/aws-samples/aws-cdk-pipelines-datalake-infrastructure#prerequisites).

---

## Deployment

### Deploying for the first time

Configure your AWS profile to target the central Deployment account as an Administrator and perform the following steps:

1. Open command line (terminal)
1. Go to project root directory where ```cdk.json``` and ```app.py``` exist
1. Run the command ```cdk ls```
1. Expected output: It lists the name of the CDK Pipeline on the console
1. Run the command ```cdk deploy --all```
1. Expected output:
    1. It creates new Pipelines in CodePipeline in the Deployment account
    1. It creates one or more CloudFormation Stacks in target AWS Account
    1. In the target AWS account, all AWS resources constituted by the CloudFormation stack will be provisioned

---

### Iterative Deployment

Pipeline you have created using CDK Pipelines module is self mutating. That means, code checked to GitHub repository branch will kick off CDK Pipeline mapped to that branch.

---

## Testing

### Prerequisites

Below lists steps are required before starting the job testing:

1. For ETL job testing TLC Trip Record Data public data is being used, https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    On the mentioned link multiple years of data is available, you can download data from 2020

1. Download Yellow Taxi Trip Records for August month from 2020. [Download data](https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2020-08.csv)

1. Make sure the transformation logic is entered in dynamodb for <> table. As part of job creation mentioned transformation logic will be used to transform data from raw to conform:

   ```sql
   SELECT count(*) count, coalesce(vendorid,-1) vendorid, day, month, year, pulocationid, dolocationid, payment_type, sum(passenger_count) passenger_count, sum(trip_distance) total_trip_distance, sum(fare_amount) total_fare_amount, sum(extra) total_extra, sum(tip_amount) total_tip_amount, sum(tolls_amount) total_tolls_amount, sum(total_amount) total_amount
   FROM datalake_raw_source.yellow_taxi_trip_record
   GROUP BY vendorid, day, month, year, day, month, year, pulocationid, dolocationid, payment_type;
   ```

1. Create a folder under raw bucket `{target_environment.lower()}-{resource_name_prefix}-{self.account}-{self.region}-raw` root path, this folder name will be used as source_system_name. You can use `tlc_taxi_data` or name of your choice.

1. Go to the created folder and create child folder named `yellow_taxi_trip_record` or you can name it per your choice

1. Make sure Athena has workgroup to execute queries for data validations. [Setting up Athena Workgroups](https://docs.aws.amazon.com/athena/latest/ug/workgroups-procedure.html) 

1. Make sure Athena has S3 buckets for query results [Getting started](https://docs.aws.amazon.com/athena/latest/ug/getting-started.html)
   
### Steps for ETL testing

1. Upload downloaded file `yellow_tripdata_2020-01.csv` to Raw bucket `s3://{target_environment.lower()}-{resource_name_prefix}-{self.account}-{self.region}-raw/tlc_taxi_data/yellow_taxi_trip_record/`

1. Upon successful load of file S3 event notification will trigger the lambda

1. Lambda will insert record into the dynamodb table `{target_environment.lower()}-{resource_name_prefix}-etl-job-audit` to track job start status

1. Lambda function will trigger the step function. Step function name will be `<filename>-<YYYYMMDDHHMMSSxxxxxx>` and provided the required metadata input

1. Step function will trigger the glue job for Raw to Conformed data processing.

1. Glue job will load the data into conformed bucket using the provided metadata and data will be loaded to `s3://{target_environment.lower()}-{resource_name_prefix}-{self.account}-{self.region}-conformed/tlc_taxi_data/yellow_taxi_trip_record/year=YYYY/month=MM/day=DD` in parquet format

1. Glue job will create/update the catalog table using the tablename passed as parameter based on folder name `yellow_taxi_trip_record` as being mentioned in prerequisites

1. After raw to conform job completion purpose-built glue job will get triggered in step function

1. Purpose built glue job will use the transformation logic being provided in dynamodb as part of prerequisites for data transformation

1. Purpose built glue job will store the result set in S3 bucket under `s3://{target_environment.lower()}-{resource_name_prefix}-{self.account}-{self.region}-purposebuilt/tlc_taxi_data/yellow_taxi_trip_record/year=YYYY/month=MM/day=DD`

1. Purpose built glue job will create/update the catalog table

1. After completion of glue job lambda will get triggered in step function to update the dynamodb table `{target_environment.lower()}-{resource_name_prefix}-etl-job-audit` with latest status

1. SNS notification will be sent to the subscribed users

1. To validate the data, please open Athena service and execute query. For testing purpose below mentioned query is being used 


```sql
SELECT * FROM "datablog_arg"."yellow_taxi_trip_record" limit 10;
```

For testing of `second data source`, download the Green Taxi Trip data [Data source](https://nyc-tlc.s3.amazonaws.com/trip+data/green_tripdata_2020-08.csv)

Perform the prerequisites for second source, where create child folder `yellow_taxi_trip_record` under could be `tlc_taxi_data` in `s3://{target_environment.lower()}-{resource_name_prefix}-{self.account}-{self.region}-raw`

For dynamodb transformation logic you can use the below mentioned query:

```sql
SELECT count(*) count, coalesce(vendorid,-1) vendorid, day, month, year, pulocationid, dolocationid, payment_type, sum(passenger_count) passenger_count, sum(trip_distance) total_trip_distance, sum(fare_amount) total_fare_amount, sum(extra) total_extra, sum(tip_amount) total_tip_amount, sum(tolls_amount) total_tolls_amount, sum(total_amount) total_amount
FROM datalake_raw_source.green_taxi_record_data
GROUP BY vendorid, day, month, year, day, month, year, pulocationid, dolocationid, payment_type
```

---

## Clean up

1. Delete stacks using the command ```cdk destroy --all```. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: TestDataLakeCDKBlogEtlPipeline, ProdDataLakeCDKBlogEtlPipeline, DevDataLakeCDKBlogEtlPipeline (y/n)?
   ```

   Note: This operation deletes stacks only in central deployment account

1. To delete stacks in **development** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Dev-DevDataLakeCDKBlogEtlDynamoDb
   1. Dev-DevDataLakeCDKBlogEtlGlue
   1. Dev-DevDataLakeCDKBlogEtlStepFunctions

1. To delete stacks in **test** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Test-TestDataLakeCDKBlogEtlDynamoDb
   1. Test-TestDataLakeCDKBlogEtlGlue
   1. Test-TestDataLakeCDKBlogEtlStepFunctions

1. To delete stacks in **prod** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Prod-ProdDataLakeCDKBlogEtlDynamoDb
   1. Prod-ProdDataLakeCDKBlogEtlGlue
   1. Prod-ProdDataLakeCDKBlogEtlStepFunctions

1. For more details refer to [AWS CDK Toolkit](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)

---

## AWS CDK

Refer to [cdk_instructions.md](./resources/cdk_instructions.md) for detailed instructions

---

## Developer Instructions

Refer to [developer_instructions.md](./resources/developer_instructions.md) for notes to Developers on this project

---

## Contributors

1. **Ravi Itha**, Senior Data Architect, Amazon Web Services
1. **Muhammad Zahid Ali**, Data Architect, Amazon Web Services
1. **Isaiah Grant**, Cloud Consultant, 2nd Watch, Inc.

---

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.

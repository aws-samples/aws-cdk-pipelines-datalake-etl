# Developer Guide

## Set up Instructions

Reference the [CDK Instructions](./cdk_instructions.md) for standard CDK project setup.

## Code Quality

We now follow [PEP8](https://www.python.org/dev/peps/pep-0008/) enforced through [flake8](https://flake8.pycqa.org/en/latest/) and [pre-commit](https://pre-commit.com/)

Please install and setup pre-commit before making any commits against this project. Example:

```{bash}
pip install pre-commit
pre-commit install
```

The above will create a git hook which will validate code prior to commits. Configuration for standards can be found in:

* [.flake8](../.flake8)
* [.pre-commit-config.yaml](../.pre-commit-config.yaml)


## ETL Process:
Here are the details about ETL process being used for job execution:

1. Data formats:
For data source, we will be using [New York City TLC Trip Record Data.](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Its a CSV(comma seperated) file providing information about New York City cab trips.
1. Data Zones:
We are using three different data zones for Data Lake processing:
   1. Raw zone serves the purpose of sourcing where source system can upload/push the source data
   1. Conformed zone standarized the data format, different source format can be conformed to a standard format, in this case parquet format. Glue supports different format [Glue Formats](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format.html)
   1. Purposebuilt zone in data lake serves the business requirments for data analysis
1. Transformation Logic Raw to Conform:
Once the data file uploaded/pushed to Raw zone, raw glue job transforms csv format to parquet format and uploads data to the conform zone. As part of standardization, csv files will be converted to parquet format. 
1. Transformation from Conform to Purposebuilt:
We are producing aggregated passenger_count,total_trip_distance,total_amount and other aggregations for each vendoar, day, month, year, pickup, drop off location to be used as data analysis
1. Data Cataloge: 
Once data transformation gets completed so as part of glue job we are cataloging the data in glue catalog. Both Conformed and Purposebuilt data can be later accessed via Athena for data analysis purpose
1. S3 event
For data flow as soon as data lands on Raw zone S3 event triggers the lambda and intiate data procssing. Lambda prepare metadata for SFN and intiate SFN execution. 
1. Integration between SFN and Glue
Once SFN gets intiated, it orchestrato the glue jobs and pass paramters to the Glue jobs. These parameters are used in glue job to get filename, source paths.
1. SNS notification:
Upon successful or failure of job SNS notification will be send to subscribed users


## Testing

TODO

## Known Issues

   ```bash
   Action execution failed
   Error calling startBuild: Cannot have more than 1 builds in queue for the account (Service: AWSCodeBuild; Status Code: 400; Error Code: AccountLimitExceededException; Request ID: e123456-d617-40d5-abcd-9b92307d238c; Proxy: null)
   ```

   [Quotas for AWS CodeBuild](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html)

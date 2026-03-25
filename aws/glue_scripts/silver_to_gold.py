####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
# Description:  AWS Glue job responsible for processing data from
#               the Silver layer (trusted/clean) to the Gold layer
#               (aggregated/analytics). It reads a SQL file from S3,
#               executes it against the Iceberg/Glue catalog, and
#               writes the result to the target Athena table.
#               Supports both 'overwrite' and 'merge' write modes.
#
# Job Arguments:
#
#   --JOB_NAME      : Glue job name (injected automatically by Glue)
#
#   --target_table  : DynamoDB item key identifying the target table
#                     and its refined/quality configuration
#                     (e.g. 'gold_tb_ft_breweries_agg')
#
#   --env           : Deployment environment (e.g. 'dev', 'prod')
####################################################################


########## Imports ##########
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf

### Custom Modules
from logs import Logs
from utils import Dynamo, AwsManager
from support import split_target_table
#############################


########## Job Arguments ##########
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'target_table', 'env'])

trgt_tbl = args['target_table']
job_name = args['JOB_NAME']
env      = args['env']

####################################


########## Logger & DynamoDB Setup ##########
logger = Logs(
    job_name=job_name,
    layer='gold',
    target_table=trgt_tbl,
    env=env,
)

dynamo_instance = Dynamo(job_name=job_name, logger=logger)

# Fetch notification preferences from DynamoDB
dynamo_notif_params = dynamo_instance.get_dynamo_records(
    dynamo_table='notification_params',
    id_value=trgt_tbl,
    id_column='trgt_tbl',
)

email_on_failure, email_on_warning, email_on_success = dynamo_instance.get_email_notif(
    dynamo_notif_params,
    layer='refined',
)

critical = dynamo_notif_params.get('critical', False)
logger.add_info(critical=critical)
#############################################


########## AWS Manager & Refined Parameters ##########
manager = AwsManager(
    job_name=job_name,
    logger=logger,
    destination=email_on_failure,
    target_table=trgt_tbl,
)

# Fetch job-specific parameters from the refined_params DynamoDB table
refined_params = manager.dynamo.get_dynamo_records(
    dynamo_table='refined_params',
    id_value=trgt_tbl,
    id_column='target_table',
)

# 'layer' determines which catalog/path to target (currently only 'gold' is supported)
# 'mode' controls how data is written: 'overwrite' replaces existing data,
#        'merge' delegates the write to the SQL itself (e.g. MERGE INTO statement)
layer = refined_params.get('layer', 'gold')
mode  = refined_params.get('mode', 'overwrite')

logger.add_info(mode=mode, layer=layer)

# S3 bucket where SQL query files are stored
bucket_sql = f'bws-artifacts-sae1-{env}'
######################################################


########## Job Arguments Validation ##########

# Validate that required string arguments are not empty

_required_args = {'target_table': trgt_tbl, 'env': env}
_missing = [k for k, v in _required_args.items() if not v or not v.strip()]
if _missing:
    msg = f"The following required job arguments are empty: {_missing}"
    logger.error(msg)
    manager.ses.send_email_on_failure(
        target_table=trgt_tbl,
        description=msg,
        destination=email_on_failure,  # Custom SES method to send alert email on failure
    )
    raise ValueError(msg)
###############################################


########## Layer Routing ##########
# Resolve target Athena table name and SQL file path based on the configured layer
if layer == 'gold':
    table, _        = split_target_table(trgt_tbl)
    trgt_tbl_athena = f'{layer}.{table}'
    key_sql         = f'sql/gold/{table}.sql'

    logger.add_info(athena_table=trgt_tbl_athena, target_table=trgt_tbl, refined_layer=layer)

else:
    msg = f"Layer '{layer}' is not supported. Only 'gold' is currently accepted."
    logger.error(msg)
    manager.ses.send_email_on_failure(
        target_table=trgt_tbl,
        description=msg,
        destination=email_on_failure,  # Custom SES method to send alert email on failure
    )
    raise ValueError(msg)
###################################


########## Spark / Glue Context (Iceberg) ##########
spark_config = (
    SparkConf()
    # Increase max RPC message size to handle large query plans
    .set('spark.rpc.message.maxSize', '1024')
    # Rebase parquet datetime values to avoid corruption between legacy and modern Spark
    .set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite',  'CORRECTED')
    .set('spark.sql.legacy.parquet.int96RebaseModeInWrite',     'CORRECTED')
    .set('spark.sql.legacy.parquet.int96RebaseModeInRead',      'CORRECTED')
    .set('spark.sql.legacy.parquet.datetimeRebaseModeInRead',   'CORRECTED')
    # Use legacy time parser to maintain compatibility with existing SQL date formats
    .set('spark.sql.legacy.timeParserPolicy',                   'LEGACY')
    # Iceberg catalog backed by AWS Glue Data Catalog
    .set('spark.sql.catalog.glue_catalog',                      'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.glue_catalog.catalog-impl',         'org.apache.iceberg.aws.glue.GlueCatalog')
    .set('spark.sql.catalog.glue_catalog.io-impl',              'org.apache.iceberg.aws.s3.S3FileIO')
    # Iceberg warehouse location on S3
    .set('spark.sql.catalog.glue_catalog.warehouse',            f's3://bws-dl-gold-sae1-{env}/')
    # Treat timestamps without timezone info as UTC to avoid ambiguity
    .set('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
)

sc          = SparkContext(conf=spark_config)
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(job_name, args)
####################################################


########## Fetch SQL Query from S3 ##########
# Retrieve the SQL file that defines the transformation for this target table
sql_query = manager.s3.get_s3_file(bucket=bucket_sql, key=key_sql)  # Custom S3 method from AwsManager

if not sql_query or not sql_query.strip():
    msg = f"SQL file '{key_sql}' retrieved from bucket '{bucket_sql}' is empty or could not be read."
    logger.error(msg)
    manager.ses.send_email_on_failure(
        target_table=trgt_tbl,
        description=msg,
        destination=email_on_failure,  # Custom SES method to send alert email on failure
    )
    raise ValueError(msg)
#############################################


########## Execute SQL Query ##########
try:
    logger.add_info(sql_key=key_sql)

    df = spark.sql(sql_query)
    # Count rows to log volume; treat failures as empty result and warn
    
    try:
        count = df.count()
        logger.add_info(count=count)
    except Exception:
        logger.add_info(count=0)
        if email_on_warning:
            manager.ses.send_email_on_warning(
                target_table=trgt_tbl,
                description='The SQL query returned an empty result set.',
                destination=email_on_warning,
                logger=logger,
            )

    logger.time_execution_step('sql_execution')

except Exception as e:
    msg = f"Failed to execute SQL file '{key_sql}'."
    logger.error(msg, error_desc=str(e))
    manager.ses.send_email_on_failure(
        target_table=trgt_tbl,
        description=str(e),
        destination=email_on_failure,  # Custom SES method to send alert email on failure
    )
    raise RuntimeError(msg) from e
#######################################


########## Write to Target Table ##########
try:
    # Enable dynamic partition overwrite so only affected partitions are replaced
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Disable int96 timestamp and vectorized reader to ensure Iceberg compatibility
    spark.conf.set("spark.sql.parquet.int96AsTimestamp",       "false")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    if mode != 'merge':
        # Write the DataFrame to the Iceberg table registered in the Glue catalog
        logger.add_info(target_athena_table=trgt_tbl_athena, write_mode=mode)
        (
            df.write
            .mode(mode)
            .option("overwriteSchema", "true")
            .insertInto(f"glue_catalog.{trgt_tbl_athena}")
        )
        logger.time_execution_step('table_insert')

except Exception as e:
    msg = f"Failed to insert data into '{trgt_tbl_athena}' (layer: {layer}, mode: {mode})."
    logger.error(msg, error_desc=str(e))
    manager.ses.send_email_on_failure(
        target_table=trgt_tbl,
        description=str(e),
        destination=email_on_failure,  # Custom SES method to send alert email on failure
    )
    raise RuntimeError(msg) from e
##########################################


########## Finalise — Write Log & Commit Job ##########
if email_on_success:
    manager.ses.send_email_on_success(
        target_table=trgt_tbl,
        destination=email_on_success,
    )

logger.write_log()
job.commit()
#######################################################
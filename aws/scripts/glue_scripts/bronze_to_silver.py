####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
# Description:  AWS Glue job responsible for processing data from
#               the Bronze layer (raw ingestion) to the Silver layer
#               (trusted/clean data). It reads ingestion configuration
#               from DynamoDB, loads files from S3 (CSV, TXT, JSON),
#               applies schema casting, optional row filtering,
#               optional data quality checks (BDQ), and writes the
#               result to the corresponding Athena/Iceberg Silver table.
#
# Job Arguments:
#
#   --JOB_NAME      : Glue job name (injected automatically by Glue)
#
#   --dt_ref        : Reference date used to locate the S3 partition
#                     (e.g. '2026-03-03')
#
#   --target_table  : DynamoDB item key identifying the target table
#                     and its ingestion/quality configuration
#
#   --file_name     : Specific file to read within the partition.
#                     Pass 'none' to read the entire partition folder.
#
#   --env           : Deployment environment (e.g. 'dev', 'prod')
####################################################################

########### Imports ############
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf


### Custom Modules 
from pyspark_utils import Pyspark
from logs import Logs
from utils import Dynamo, AwsManager
from support import eval_values
from quality import Quality

####################################


########## Job Arguments ##########

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'dt_ref', 'target_table', 'file_name', 'env']
)

trgt_tbl  = args['target_table']
env       = args['env']
file_name = args['file_name']
dt_ref    = args['dt_ref']
job_name  = 'bronze_to_silver'


########## Spark / Glue Context Configuration ##########

# S3 warehouse path used by the Iceberg catalog
_warehouse_bucket = f'bws-dl-silver-sae1-{env}'

spark_config = (
    SparkConf()
    # Allow Spark to write to dynamic partitions without specifying them explicitly
    .set('hive.exec.dynamic.partition.mode', 'nonstrict')
    # Iceberg catalog backed by AWS Glue Data Catalog
    .set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    .set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    # Increase S3 connection pool to avoid throttling on large jobs
    .set('spark.hadoop.fs.s3.connection.maximum', '10000')
    .set('spark.hadoop.fs.s3a.connection.maximum', '10000')
    # Iceberg warehouse location on S3
    .set('spark.sql.catalog.glue_catalog.warehouse', _warehouse_bucket)
    # Treat timestamps without timezone info as UTC to avoid ambiguity
    .set('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
)

sc          = SparkContext(conf=spark_config)
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)


########## Logger, Dynamo & AWS Manager Initialisation ##########

logger = Logs(
    job_name=job_name,
    layer='silver',
    target_table=trgt_tbl,
    env=env,
)

dynamo = Dynamo(
    job_name=job_name,
    logger=logger,
    trgt_tbl=trgt_tbl,
)

# Retrieve notification configuration for this target table
notification_params = dynamo.get_dynamo_records(
    dynamo_table='notification_params',
    id_value=trgt_tbl,
    id_column='trgt_tbl',
)

is_critical = notification_params.get('critical', False)
logger.add_info(critical=is_critical)

email_on_failure, email_on_warning, email_on_success = dynamo.get_email_notif(
    notification_params,
    layer='ingestion',
)

manager = AwsManager(
    job_name=job_name,
    logger=logger,
    destination=email_on_failure,
    env=env,
    target_table=trgt_tbl,
)

# Retrieve ingestion-specific configuration for this target table
ingestion_params = manager.dynamo.get_dynamo_records(
    dynamo_table='ingestion_params',
    id_value=trgt_tbl,
    id_column='trgt_tbl',
)

pyspark = Pyspark(
    job_name=job_name,
    spark=spark,
    env=env,
    logger=logger,
    destination=email_on_failure,
    trgt_tbl=trgt_tbl,
)

# Validate that required string arguments are not empty
_required_args = {'target_table': trgt_tbl, 'env': env, 'dt_ref': dt_ref}
_missing = [k for k, v in _required_args.items() if not v or not v.strip()]
if _missing:
    msg = f"The following required job arguments are empty: {_missing}"
    logger.error(msg)
    manager.ses.send_email_on_failure(
            target_table=trgt_tbl,
            description=msg,
            destination=email_on_failure) # Custom SES method to send alert email on failure
    
    raise ValueError(f"The following required job arguments are empty: {_missing}")


########## Ingestion Parameters ##########

ext              = ingestion_params.get('ext')
bronze_path      = ingestion_params.get('s3_bronze_path')
schema           = ingestion_params.get('table_schema', None)
has_bdq          = ingestion_params.get('has_bdq', None)
header           = ingestion_params.get('header', True)
encoding         = ingestion_params.get('encoding', 'UTF-8')
sep              = ingestion_params.get('sep', None)
options_params   = ingestion_params.get('options_params', {})
explode_column   = ingestion_params.get('explode_column', None)
skip_header      = ingestion_params.get('skip_header', None)
skip_footer      = ingestion_params.get('skip_footer', None)
filter_column    = ingestion_params.get('filter_column', None)
filter_value     = ingestion_params.get('filter_value', None)
positional_column = ingestion_params.get('positional_column', [])
silver_table     = ingestion_params.get('silver_table')
mode             = ingestion_params.get('mode', 'overwrite')
partition_column = ingestion_params.get('partition_column')
iceberg_query    = ingestion_params.get('iceberg_query', None)
lit_values       = ingestion_params.get('lit_values', None)


# Guard against missing critical ingestion parameters before going further
_missing_params = {
    'ext': ext,
    's3_bronze_path': bronze_path,
    'silver_table': silver_table,
}
for param_name, param_value in _missing_params.items():
    if not param_value:
        _msg = f"'{param_name}' is missing in ingestion_params for table '{trgt_tbl}'."
        logger.error(_msg)
        manager.ses.send_email_on_failure(
            target_table=trgt_tbl,
            description=_msg,
            destination=email_on_failure,  # Custom SES method to send alert email on failure
        )
        raise ValueError(_msg)


########## Build S3 Source Path ##########

# Append the date partition to the base bronze path
bronze_path += f"ingestion_date={dt_ref}/"

# If a specific file was provided, append it; otherwise the whole partition is read
if file_name.lower() != 'none':
    bronze_path += file_name

logger.add_info(file_name=file_name)


########## Parameter Evaluation (string → native type) ##########

# Evaluate lit_values and resolve variable references (e.g. dt_ref)
if lit_values:
    lit_values = eval_values(
        lit_values,
        target_tbl=trgt_tbl,
        logger=logger,
        manager=manager,
        destination=email_on_failure,
    )
    # If the value is a variable name, resolve it in the current scope
    if lit_values.get('variable'):
        raw_value = lit_values['value']
        try:
            lit_values['value'] = eval(raw_value)  # noqa: S307 — controlled internal usage
        except NameError:
            msg = f"lit_values 'value' refers to undefined variable '{raw_value}'. Ensure the variable is available in the job scope."
            logger.error(msg)
            manager.ses.send_email_on_failure(
                    target_table=trgt_tbl,
                    description=msg,
                    destination=email_on_failure) # Custom SES method to send alert email on failure
            raise NameError(msg)

# Convert schema from string representation to dict/StructType if needed
schema = eval_values(
    schema,
    target_tbl=trgt_tbl,
    logger=logger,
    manager=manager,
    destination=email_on_failure,
)

# Convert header from string (e.g. "True"/"False") to boolean if needed
header = eval_values(
    header,
    target_tbl=trgt_tbl,
    logger=logger,
    manager=manager,
    destination=email_on_failure,
)


########## Read File from Bronze Layer (S3) ##########

SUPPORTED_EXTENSIONS = ['csv', 'txt', 'json']

if ext in ['csv', 'txt']:
    df = pyspark.read_csv_file_from_s3(
        s3_path=bronze_path,
        ext=ext,
        header=header,
        sep=sep,
        encoding=encoding,
        schema=schema,
        kwargs=options_params,
    )

elif ext == 'json':
    df = pyspark.read_json_file_from_s3(
        s3_path=bronze_path,
        encoding=encoding,
        explode_column=explode_column,
        kwargs=options_params,
    )

else:
    msg = f"Unsupported file extension '{ext}' for table '{trgt_tbl}'. Supported extensions are: {SUPPORTED_EXTENSIONS}."
    logger.error(msg)
    manager.ses.send_email_on_failure(
                    target_table=trgt_tbl,
                    description=msg,
                    destination=email_on_failure) # Custom SES method to send alert email on failure
    raise ValueError(msg)



########## Schema Casting ##########

df = pyspark.cast_df(
    df,
    schema=schema,
    ext=ext,
    positional_column=positional_column,
    lit_values=lit_values,
    partition_column=partition_column,
)


########## Optional Filtering ##########

# Apply column filter if configured (supports plain values and regex via '*re' prefix)
if filter_column and filter_value:
    use_regex = False
    if filter_value.startswith('*re'):
        filter_value = filter_value.split('*re', 1)[1]
        use_regex = True

    df = pyspark.filter_df(
        df=df,
        filter_column=filter_column,
        filter_value=filter_value,
        is_re=use_regex,
    )

# Skip header/footer rows if configured (e.g. fixed-width files with metadata rows)
if skip_header or skip_footer:
    df = pyspark.skip_rows(
        df=df,
        skip_footer=skip_footer,
        skip_header=skip_header,
    )


########## Row Count Logging ##########

row_count = df.count()
logger.add_info(count=row_count)

if row_count == 0:
    logger.add_info(warning=f"DataFrame is empty after reading from '{bronze_path}'. "
                             "The Silver table will be written with zero rows.")


########## Data Quality Checks (BDQ) ##########

if has_bdq:
    quality_records = manager.dynamo.get_dynamo_records(
        dynamo_table='quality_params',
        id_value=trgt_tbl,
        id_column='trgt_tbl',
    )

    quality_params = quality_records.get('quality_params')
    stop_job       = quality_records.get('stop_job', False)

    if not quality_params:
        
        msg = f"'has_bdq' is True for table '{trgt_tbl}' but no 'quality_params' were found in the 'quality_params' DynamoDB table."
        logger.error(msg)
        manager.ses.send_email_on_failure(
                    target_table=trgt_tbl,
                    description=msg,
                    destination=email_on_failure) # Custom SES method to send alert email on failure
        raise ValueError(msg)

    quality = Quality(
        job_name=job_name,
        quality_params=quality_params,
        target_table=trgt_tbl,
        df=df,
        stop_job=stop_job,
        destination_on_failure=email_on_failure,
        destination_on_success=email_on_success,
        spark=spark,
        logger=logger,
    )

    quality.run_quality_checks()


########## Write to Silver Layer (Athena / Iceberg) ##########

pyspark.insert_into_at_tbl(
    df=df,
    athena_tbl=silver_table,
    mode=mode,
    query_iceberg=iceberg_query,
)


########## Success Notification & Log Finalisation ##########

if email_on_success:
    manager.ses.send_email_on_success(
        target_table=trgt_tbl,
        destination=email_on_success,
    )

logger.write_log()
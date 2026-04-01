####################################################################
# Author:       Bruno William da Silva
# Last Updated: 03/03/2026
# Description:  AWS Lambda function triggered by S3 events to
#               automatically remove Hadoop/EMR-generated folder
#               marker objects (files ending with '_$folder$').
#               These artifacts are created by Spark/Hadoop jobs
#               and serve no purpose in S3 — this function
#               keeps the bucket clean by deleting them on arrival.
# Trigger:      S3 Event Notification (s3:ObjectCreated:*)
# Usage example:
#   Deployed as an AWS Lambda with an S3 trigger configured
#   on the target bucket for ObjectCreated events.
####################################################################

########## Imports ##########

import json
import boto3
import urllib.parse

########## Constants ##########

FOLDER_MARKER_SUFFIX = "_$folder$"
HTTP_OK             = 200

########## AWS Clients ##########

s3_client = boto3.client("s3")

########## Handler ##########

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point for the Lambda function.

    Processes an S3 event notification and deletes the triggering
    object if its key ends with the Hadoop folder-marker suffix
    '_$folder$'.

    Args:
        event   (dict): S3 event payload provided by AWS Lambda.
        context       : Lambda context object (unused).

    Returns:
        dict: HTTP-style response with statusCode and body.
    """
    record     = event["Records"][0]
    bucket     = record["s3"]["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    print(f"[INFO] Event received  | bucket: {bucket} | key: {object_key}")

    try:
        _handle_object(bucket, object_key)

    except Exception as exc:
        print(f"[ERROR] Failed to process object '{object_key}' in bucket '{bucket}': {exc}")
        raise

    return {
        "statusCode": HTTP_OK,
        "body": json.dumps("Processing completed successfully."),
    }


########## Helpers ##########

def _handle_object(bucket: str, object_key: str) -> None:
    """
    Evaluates the S3 object key and deletes it when it matches
    the Hadoop folder-marker suffix.

    Args:
        bucket     (str): Name of the S3 bucket.
        object_key (str): Key (path) of the S3 object.
    """
    if object_key.endswith(FOLDER_MARKER_SUFFIX):
        _delete_object(bucket, object_key)
    else:
        print(
            f"[INFO] Skipping '{object_key}' — "
            f"does not match the suffix '{FOLDER_MARKER_SUFFIX}'."
        )


def _delete_object(bucket: str, object_key: str) -> None:
    """
    Deletes the specified object from S3.

    Args:
        bucket     (str): Name of the S3 bucket.
        object_key (str): Key (path) of the object to delete.
    """
    s3_client.delete_object(Bucket=bucket, Key=object_key)
    print(f"[INFO] Deleted '{object_key}' from bucket '{bucket}' successfully.")
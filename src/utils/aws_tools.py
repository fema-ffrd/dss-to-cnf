import json
import logging
from pathlib import Path


def check_file_exists(s3_client, bucket, key):
    """
    Verify that a file exists in an S3 bucket and can be accessed.
    """
    if Path(key).suffix != ".dss":  
        raise ValueError(f"{key} is not a DSS file")
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        raise KeyError(f"cannot access file {key} in bucket {bucket}. Verify key exists and you have access")


def read_json_from_s3(s3_client, bucket, key):
    """
    Read a JSON file from an S3 bucket and parse it.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = response["Body"].read()
        return json.loads(json_data)

    except Exception as e:
        return KeyError(f"Error reading s3://{bucket}/{key}: {e}")


def list_keys(s3_client, bucket, prefix, suffix=""):
    keys = []
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        keys += [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(suffix)]
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return keys

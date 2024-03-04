import logging
import os
import re
import warnings
import boto3
import s3fs
import xarray as xr

import numpy as np

from datatree import DataTree
from dotenv import find_dotenv, load_dotenv
from pydsstools.heclib.utils import dss_logging
from papipyplug import plugin_logger
from utils.aws_tools import read_json_from_s3, list_keys, check_file_exists
from utils.dss_tools import extract_data_from_dss, custom_f_part_parser

# quietly set dss_logging to level ERROR
# without setting the root logger to ERROR will write warning when changing
logging.root.setLevel(logging.ERROR)
dss_logging.config(level="Error")
logging.root.setLevel(logging.INFO)

# prevent pydsstools from printing
# text_trap = io.StringIO()
# sys.stdout = text_trap

"""NOTES

metadata per event in event loop
        for k in list_keys(client, input_dss_bucket, prefix):
            if re.search(r, k):
                meta = k


"""

warnings.filterwarnings("ignore")
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)


PLUGIN_PARAMS = {
    "required": [
        "dss_input_bucket",
        "dss_input_key",
        "zarr_output_bucket",
        "zarr_output_prefix",
    ],
    "optional": ["group_id", "c_part_variable", "sub_group_name", "use_custom_parser"],
}


def main(params: dict):
    # params = papipyplug.parse_input(sys.argv, PLUGIN_PARAMS)
    """
    add description....sub_group_name
    """
    # Required
    dss_input_bucket = params.get("dss_input_bucket", "")
    dss_input_key = params.get("dss_input_key", "")
    zarr_output_bucket = params.get("zarr_output_bucket", "")
    zarr_output_prefix = params.get("zarr_output_prefix", [])

    # Optional
    c_part_variable = params.get("simplify", "FLOW")
    group_id = params.get("group_id", "main_group")
    use_custom_parser = params.get("use_custom_parser", False)
    sub_group_name = params.get("sub_group_name", "sub_group")

    if params["dss_input_bucket"] != params["zarr_output_bucket"]:
        raise NotImplementedError(
            "multi-bucket support not implemented! please use the same bucket for input and output"
        )

    # setup s3 session
    load_dotenv(find_dotenv())

    if "AWS_ACCESS_KEY_ID" not in os.environ:
        raise ValueError("AWS_ACCESS_KEY_ID not found in environment variables")

    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        raise ValueError("AWS_SECRET_ACCESS_KEY not found in environment variables")

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])

    fs = s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    client = session.client("s3")

    try:
        check_file_exists(client, dss_input_bucket, dss_input_key)
    except (ValueError, KeyError) as e:
        logging.error(e)
        return

    # construct output key
    zarr_output = f"s3://{zarr_output_bucket}/{zarr_output_prefix}"

    # construct pathname pattern
    dss_internal_pathname_pattern = f"/*/*/{c_part_variable}/*/*/*/"

    # initiate datatree
    dt = DataTree(name=group_id)
    group = DataTree(name=group_id, data=xr.Dataset(), parent=dt)

    successes, failures = [], []

    logging.info(f"reading from dss file: {dss_input_key}")

    logging.info(f"looking for {dss_input_bucket}/{dss_input_key}")
    if use_custom_parser:
        try:
            data_dict = extract_data_from_dss(
                f"s3://{dss_input_bucket}/{dss_input_key}",
                dss_internal_pathname_pattern,
                fs,
                sub_group_name,
                custom_parser=custom_f_part_parser,
            )
        except ValueError as e:
            logging.error(e)
            return
    else:
        data_dict = extract_data_from_dss(
            f"s3://{dss_input_bucket}/{dss_input_key}",
            dss_internal_pathname_pattern,
            fs,
        )

    # Write collected data to zarr using xarray
    arrays = []
    metadata = {}

    # event is either the fpart or a parsed fpart
    for event, elements_data in data_dict.items():
        logging.info(event, elements_data)
        arr = np.array(list(elements_data.values()))

        logging.debug(f"event: {event} | min: {arr.min()} | max: {arr.max()} | shape: {arr.shape}")

        data = np.reshape(arr, (1, *arr.shape))
        arrays.append(
            xr.DataArray(
                data=data,
                dims=["event", "element", "time_index"],
                coords=[
                    [event],
                    list(elements_data.keys()),
                    range(len(data[0][0])),
                ],
            )
        )

        # TODO: Add metadata
        # metadata["event"] = {"a":"b"}

    # create data array
    da = xr.concat(arrays, dim="event")

    # add data array to a dataset
    ds = xr.Dataset({c_part_variable: da}, attrs=metadata)

    # add dataset to datatree
    DataTree(name=sub_group_name, parent=group, data=ds)
    logging.info(f"finished reading dss file: {dss_input_key}")

    # write datatree to s3
    logging.info(f"started writing {zarr_output}")
    s3_reader = s3fs.S3FileSystem(
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    dt.to_zarr(s3fs.S3Map(zarr_output, s3=s3_reader), "a")
    logging.info(f"finished writing {zarr_output}")

    return {"zarr_s3_key": zarr_output, "success": successes, "failures": failures}


if __name__ == "__main__":
    # main()
    results = main(
        {
            "dss_input_bucket": "wy-ble",
            "dss_input_key": "realizations/realization-1/hms_output/Y001/HUC14040102/Y001.dss",
            "zarr_output_bucket": "wy-ble",
            "zarr_output_prefix": "realizations/realization-1/post_processing/test22.zarr",
            # "group_id": "HUC14040102",
            # "c_part_variable": "FLOW",
            # "sub_group_name": "Y001",
            # "use_custom_parser": "False"
        }
    )

    # plugin_logger()
    # # main(params)

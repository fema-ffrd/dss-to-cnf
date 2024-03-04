from __future__ import annotations
import logging
import re
import tempfile
import numpy as np
import s3fs
import io
import sys
from typing import Callable
from pydsstools.heclib.dss import HecDss
from pydsstools.heclib.utils import dss_logging

logging.root.handlers = []
logging.basicConfig(
    level=logging.INFO,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def read_dss_data(dss_file: str, dss_pathname: str) -> np.array:
    """Reads flow data from a given .dss file"""

    # quietly set dss_logging to level ERROR
    # without setting the root logger to ERROR will write warning when changing
    logging.root.setLevel(logging.ERROR)
    dss_logging.config(level="Error")
    logging.root.setLevel(logging.INFO)

    # prevent pydsstools from printing
    text_trap = io.StringIO()
    sys.stdout = text_trap

    # read dss path
    with HecDss.Open(dss_file) as fid:
        ts = fid.read_ts(dss_pathname)  # , trim_missing=False)
        values = ts.values

    # restore standard out
    sys.stdout = sys.__stdout__

    return values.copy()


def get_path_part(pathname: str, part: str) -> str:
    """Extracts value from given pathname and part of pathname with '/' as the delimiter

    example 1: "C" selects 'FLOW' from pathname:
             //Alderson/FLOW/14Jan1996 - 07Feb1996/1Hour/RUN:Jan 1996 - Calibration/

    example 2: "F" selects 'hms-runner' from pathname:
             //hms-bucket/kanawha/kanawha/5/hms-runner/Jan_1996___Calibration.dss
    """
    path_parts = ["A", "B", "C", "D", "E", "F", "G", "H"]
    if part not in path_parts:
        raise ValueError(
            f"`part` must be in {path_parts}, replace `{part}` with a valid part"
        )
    position = path_parts.index(part)
    return pathname.split("/")[position + 1].replace(" ", "_")


def remove_date(pathnames: list[str]):
    """removes time stamps (part d) and returns a simplified list of pathnames

    Args:
        pathnames (list[str]): pathnames to remove part d from

    Returns:s
        _type_: list of unique pathnames after removing part d
    """
    no_part_d_pathnames = []
    for pathname in pathnames:
        no_part_d_pathnames.append(pathname.replace(pathname.split("/")[4], ""))

    return list(set(no_part_d_pathnames))


def custom_f_part_parser(f_part:str, string_part:str):
    """
    Custom parser takes a patterned pathname and splits based on simualtion
    logic. 
    
    In this parser, the F pattern is assumed to be: "RUN-{year}-{event}"
    
    Example:

        FPART = "RUN_Y001-E0001"

    Args:
        f_part (str)
        expected_string (str)

    Raises:
        ValueError: _description_
    """     
    parts = f_part.lstrip("RUN:").split("-")
    if len(parts) != 2:
        raise ValueError(f"Expected 2 parts [year,event] in the FPART, got {parts}")
    
    part_1, part_2 = parts
    if string_part != part_1:
        raise ValueError(f"Year {part_1} does not match expected year {string_part}")
    
    return part_2

    
def extract_data_from_dss(
    dss_key: str, dss_internal_pathname_pattern: str, fs: s3fs.S3FileSystem, parser_string_part:str=None, custom_parser: Callable[str, str]= None
)-> dict:
    """read dss data from s3 for the provided dss file and pathname pattern

    Args:
        dss_key (str): dss key on s3
        dss_internal_pathname_pattern (str): path name pattern to filter dss records to
        s3 (s3fs.S3FileSystem):

    Returns:
        tuple: flow data and year
    """

    with tempfile.NamedTemporaryFile(suffix=".dss", delete=True) as tmpfile:
        local_dss_file_path = tmpfile.name
        fs.get(dss_key, local_dss_file_path)

        # open dss file and get pathnames that follow pathname pattern
        with HecDss.Open(local_dss_file_path) as hec_dss:
            dss_internal_pathnames = hec_dss.getPathnameList(
                dss_internal_pathname_pattern
            )

        pathnames = remove_date(dss_internal_pathnames)    

        # data_dict = custom_dss_path_parser(local_dss_file_path, dss_internal_pathnames, dss_year)
        dss_data = {}
        for pathname in pathnames:
            f_part = get_path_part(pathname, "F")
            b_part = get_path_part(pathname, "B")
            data = read_dss_data(local_dss_file_path, pathname)

            if custom_parser:
                new_entry = custom_parser(f_part, parser_string_part)
            else:
                new_entry = f_part

            if new_entry not in dss_data.keys():
                    dss_data[new_entry] = {}
            dss_data[new_entry][b_part] = data

    return dss_data


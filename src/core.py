# %% Setting up

import os
import requests
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from typing import get_args, Literal


# %% Defining supported extensions

# Extensions for pandas data frames
DataFrameExt = Literal[
    "csv", "xlsx", "parquet", "ods", "stata", "hdf", "f", "feather", "pkl", "pickle"
]


# %% Function to load credentials from .ENV file


def load_credentials(
    zone_name: str = "BUNNY_STORAGE_ZONE",
    password_read: str | None = "BUNNY_PASS_READ",
    password_write: str | None = "BUNNY_PASS_WRITE",
) -> dict:
    """
    Loads the name of a given Bunny.net storage zone as
    well as either read or write access password from a local
    .ENV file (at least one of the two kinds of passwords
    needs to be provided).

    Args:
        zone_name (str): name of the ENV variable that hasof the
        Bunny.net storage zone (can be found in the storage zone's
        settings)
        password_read (str | None): password for read-only access
        password_write (str | None): password for both reading
        and writing data.

    Returns:
        dict: all the credentials for accessing a given Bunny.net
        storage zone
    """

    # Reading credentials from .ENV file
    _ = load_dotenv()
    zone = os.getenv(zone_name)
    p_read = os.getenv(password_read) if password_read else None
    p_write = os.getenv(password_write) if password_write else None

    # Checking if the Storage Zone name itself is missing
    if not zone:
        raise ValueError(f"Environment variable '{zone_name}' is not set.")

    # Checking whether there is at least one valid password
    if not p_read and not p_write:
        raise KeyError(
            f"Could not find passwords in '{password_read}' or '{password_write}'. "
            "At least one must be defined in your .env file."
        )

    # Building a dictionary and returning
    credentials = {
        "zone_name": zone,
        "password_read": p_read,
        "password_write": p_write,
    }
    return credentials


# %% Function to download a file from Bunny.net


def download_file(
    remote_file_path: str,
    local_destination_path: str,
    storage_zone: str,
    password_read: str,
    print_status: bool = True,
):
    url = f"https://storage.bunnycdn.com/{storage_zone}/{remote_file_path}"
    headers = {"accept": "*/*", "AccessKey": password_read}

    # Starting the request
    response = requests.get(url, headers=headers, stream=True)

    # Checking for HTTP errors
    # This will stop the script if it's NOT a 200-level code
    response.raise_for_status()

    # Creating folders if they don't exist
    os.makedirs(os.path.dirname(local_destination_path), exist_ok=True)

    # Streaming the file to disk
    with open(local_destination_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    if print_status:
        print(f"Successfully downloaded: '{local_destination_path}'.")


# %% Function to upload a file to Bunny.net


def upload_file(
    local_file_path: str,
    remote_file_path: str,
    storage_zone: str,
    password_write: str,
    region: str = "",
    print_status: bool = True,
) -> None:
    """
    Uploads a local file to a given Bunny.net storage zone.

    Args:
        local_file_path (str): full path and file name, including
        extension, as stored on the local device.
        remote_file_path (str, optional): the path in the storage
        zone to which to save the file.
        storage_zone (str): name of the storage zone where we want
        to upload the file.
        password_write (str): password with write access
        region (str, optional): region to write the data to.
        Defaults to "", which is the same as Germany, the default
        region in the service.
        print_status (bool, optional): whether to print a confirmation
        when the file has been uploaded or not. Defaults to True.
    """

    # Checking if the local file exists
    if not os.path.isfile(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    # Building the base URL
    base_url = "storage.bunnycdn.com"
    if region:
        base_url = f"{region}.{base_url}"

    # Determining the path in the storage zone to save to
    if remote_file_path:
        remote_file_path = f"{remote_file_path}"
        storage_url = f"https://{base_url}/{storage_zone}/{remote_file_path}"
    else:
        storage_url = f"https://{base_url}/{storage_zone}/{local_file_path}"

    # Preparing the headers, including credentials
    headers = {
        "AccessKey": password_write,
        "Content-Type": "application/octet-stream",
        "accept": "application/json",
    }

    # Executing the request
    with open(local_file_path, "rb") as file_data:
        response = requests.put(storage_url, headers=headers, data=file_data)

    # Raising an error if the upload failed (4xx or 5xx codes)
    response.raise_for_status()

    # Printing an optional confirmation
    if print_status:
        print(f"Successfully uploaded to '{remote_file_path}'.")


# %% Function to delete a remote file stored on Bunny.net


def delete_file(
    remote_file_path: str,
    storage_zone: str,
    password_write: str,
    print_status: bool = True,
):

    # Building the URL for the request
    url = f"https://storage.bunnycdn.com/{storage_zone}/{remote_file_path}"

    # Preparing the headers, including a read/write password
    headers = {"accept": "*/*", "AccessKey": password_write}

    # Executing the request
    response = requests.delete(url, headers=headers)

    # Printing an optional confirmation
    if print_status:
        if response.status_code == 200:
            print(f"The remote file '{remote_file_path}' was deleted successfully.")
        elif response.status_code == 404:
            print(
                f"'{remote_file_path}' not found in the remote storage zone. No deletion has been performed."
            )
        else:
            print(response.text + f" (HTTP Code: {response.status_code})")


# %% Function to write a df to a local temp file


def write_tmp_df(df: pd.DataFrame, format: DataFrameExt, **kwargs) -> str:
    """
    Writes a pandas df to a local temporary file as step 1
    of writing a df to Bunny.net storage.

    Args:
        df (pd.DataFrame): pandas df to write
        format (DataFrameExt): file format for the df

    Returns:
        str: local file path
    """

    # Notes: delete=False ensures the file stays on disk after
    # we close it. The suffix helps the OS and other apps
    # with recognizing the file type.
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as tmp:
        temp_path = tmp.name

        if format == "csv":
            df.to_csv(temp_path, **kwargs)
        elif format in ["xls", "xlsx", "xlsm"]:
            df.to_excel(temp_path, **kwargs)
        elif format == "ods":
            df.to_excel(temp_path, engine="odf", **kwargs)
        elif format == "stata":
            df.to_stata(temp_path, **kwargs)
        elif format == "hdf":
            df.to_hdf(temp_path, **kwargs)
        elif format == "parquet":
            df.to_parquet(temp_path, **kwargs)
        elif format in ["f", "feather"]:
            df.to_feather(temp_path, **kwargs)
        elif format in ["pkl", "pickle"]:
            df.to_pickle(temp_path, **kwargs)
        else:
            raise ValueError(f"Unsupported file extension: '{format}'.")

    return temp_path


# %% Function to read a df from a local (temp) file


def read_tmp_df(local_file_path: str, **kwargs) -> pd.DataFrame:
    """
    Reads a pandas df from a local (temporary) file as step 1
    of reading a df from Bunny.net storage.

    Args:
        local_file_path (str): path to a local file where
        a data frame-compatible file is stored

    Returns:
        pd.DataFrame: pandas df ready for use
    """

    # Detecting the file format from the extension
    format = Path(local_file_path).suffix.lower().lstrip(".")

    # Reading a df using standard pandas
    if format == "csv":
        df = pd.read_csv(local_file_path, **kwargs)
    elif format in ["xls", "xlsx", "xlsm"]:
        df = pd.read_excel(local_file_path, **kwargs)
    elif format == "ods":
        df = pd.read_excel(local_file_path, engine="odf", **kwargs)
    elif format == "stata":
        df = pd.read_stata(local_file_path, **kwargs)
    elif format == "hdf":
        df = pd.read_hdf(local_file_path, **kwargs)
    elif format == "parquet":
        df = pd.read_parquet(local_file_path, **kwargs)
    elif format in ["f", "feather"]:
        df = pd.read_feather(local_file_path, **kwargs)
    elif format in ["pkl", "pickle"]:
        df = pd.read_pickle(local_file_path, **kwargs)
    else:
        raise ValueError(f"Unsupported file extension: '{format}'.")

    return df


# %% Function to delete a local (temp) file


def delete_local_file(local_file: str) -> None:
    """
    Deletes a local file. Useful to do this clean-up
    after a local temp file containing a pandas df
    has been uploaded to Bunny.net.

    Args:
        local_file (str): path to the local file to be
        deleted. Typically, this should be the output of
        the write_tmp_file() function.
    """
    if os.path.exists(local_file):
        os.remove(local_file)


# %% Function to write a df to Bunny.net


def write_bunny_df(
    df: pd.DataFrame,
    remote_file_path: str,
    storage_zone: str,
    password_write: str,
    region: str = "",
    print_status: bool = True,
    **kwargs,
):

    # Detecting the file format from the extension
    format = Path(remote_file_path).suffix.lower().lstrip(".")

    # Validating the chosen format
    if format not in get_args(DataFrameExt):
        raise ValueError(
            f"Unsupported file extension '{format}'. Must be one of {get_args(DataFrameExt)}"
        )

    # Writing the sample df to a local temp file
    local_file_path = write_tmp_df(df, format)

    # Uploading the local file to Bunny.net storage
    upload_file(
        local_file_path,
        remote_file_path,
        storage_zone,
        password_write,
        region,
        print_status=False,
        **kwargs,
    )

    # Deleting the local temp file
    delete_local_file(local_file_path)

    # Confirming that writing the df was successful
    print(
        f"Data frame successfully written to Bunny.net: '{storage_zone}/{remote_file_path}'."
    )


# %% Function to read a df from Bunny.net


def read_bunny_df(
    remote_file_path: str,
    storage_zone: str,
    password_read: str,
    print_status: bool = True,
    **kwargs,
) -> pd.DataFrame:

    # Getting the file extension from remote
    format = Path(remote_file_path).suffix
    format_clean = format.lstrip(".")

    # Confirming that the file extension is supported
    if format_clean not in get_args(DataFrameExt):
        raise ValueError(
            f"Unsupported file extension '{format}'. Must be one of {get_args(DataFrameExt)}"
        )

    # Creating a temporary file for the download
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=format)
    local_file_path = tmp.name

    try:
        # Closing the file handle immediately; some downloaders
        # prefer to open the path themselves.
        tmp.close()

        # Downloading the actual file from Bunny.net
        download_file(
            remote_file_path,
            local_file_path,
            storage_zone,
            password_read,
            print_status=False,
        )

        # Importing data from the local file
        df = read_tmp_df(local_file_path)

        if print_status:
            print(
                f"Data frame successfully loaded from Bunny.net: '{storage_zone}/{remote_file_path}'."
            )

        return df

    finally:
        # Cleaning up: the below code guaranttes that even if
        # the download or the read fails, deletion will happen
        if os.path.exists(local_file_path):
            os.remove(local_file_path)


# %% Examples

if __name__ == "__main__":

    # Getting credentials from .ENV file
    bunny = load_credentials()

    # Uploading a local file
    upload_file(
        "sample_text.txt",
        "sample_text.txt",
        bunny["zone_name"],
        bunny["password_write"],
        print_status=True,
    )

    # Downloading a remote file
    download_file(
        "sample_text.txt",
        "downloaded_sample_text.txt",
        bunny["zone_name"],
        bunny["password_read"],
        print_status=True,
    )

    # Deleting a remote file
    delete_file(
        "Christmas.png",
        bunny["zone_name"],
        bunny["password_write"],
        print_status=True,
    )

    # Creating a sample df
    df = pd.DataFrame(np.random.randint(0, 100, size=(10000, 3)), columns=list("ABC"))

    # Writing the sample df to Bunny.net
    write_bunny_df(df, "test_df.csv", bunny["zone_name"], bunny["password_write"])

    # Reading a df from Bunny.net
    df_read = read_bunny_df("test_df.csv", bunny["zone_name"], bunny["password_read"])


# %%

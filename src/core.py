# %% Setting up

import os
import requests
from dotenv import load_dotenv  # noqa


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


# %% Function to download a file


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


# %% Function to upload a file


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
        print(
            f"Successfully uploaded '{local_file_path}' to '{storage_zone}' (HTTP 201)."
        )


# %% Function to delete a remote file


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


# %%

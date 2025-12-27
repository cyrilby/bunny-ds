# BunnyDF: Reading & writing data frames from Bunny.net

This Python package allows for easily reading and writing `pandas` data frames and other data science-related objects to Bunny.net cloud storage.

## Requirements

- Python 3.10 or higher
- Python packages as specified in the `pyproject.toml` file

## Functionalities

The package contains the following functions, which are all callable by the end user:

- `load_credentials()`: allows for easy import of credentials stored in a local .ENV file
- `download_file()`: downloads any kind of file stored in a remote storage zone on Bunny.net to a local directory
- `upload_file()`: uplodas any kind of file from a local directory to a remote storage zone on Bunny.net
- `delete_file()`: deletes any kind of file stored on a remote storage zone on Bunny.net
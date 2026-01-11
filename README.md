# BunnyDS: Reading & writing data frames from Bunny.net

- **Author**: github.com/cyrilby
- **Last meaningful update**: 29-12-2025

This Python package allows for easily reading and writing `pandas` data frames and other data science-related objects to Bunny.net cloud storage.

## Requirements

- Python 3.10 or higher
- Python packages as specified in the `pyproject.toml` file
- Additional packages may be needed to enable reading/writing specific file formats such as Excel, Parquet, etc. (please check the official [pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html) for more information)

## Functionalities

### Supported file formats

For reading/writing tabular data (`pandas` data frames):

- CSV
- Excel (xls, xlsx, xlsm)
- ODS
- Parquet
- Feather (f, feather)
- HDF
- Pickle (pkl, pickle)

Other formats [WIP as of 29-12-2025]:

- JSON for storing dictionaries (yet to be added)
- TXT for storing strings (yet to be added)
- Pickle for storing any other Pythonic objects (yet to be added)

### Main functions

The package contains the following main functions which are intended to be called by the end user:

- `load_credentials()`: allows for easy import of credentials stored in a local .ENV file
- `read_bunny_df()`: reads a `pandas` data frame from a remote Bunny.net file
- `write_bunny_df()`: writes a `pandas` data frame to a remote Bunny.net file

### Supporting functions

To enable the above functionalities, the following functions have been added, though they are not intended to be called by the end user as such:

- `download_file()`: downloads any kind of file stored in a remote storage zone on Bunny.net to a local directory
- `upload_file()`: uplodas any kind of file from a local directory to a remote storage zone on Bunny.net
- `delete_file()`: deletes any kind of file stored on a remote storage zone on Bunny.net
- `write_tmp_df()`: creates a local temporary file where a data frame is written before being uploaded to Bunny.net
- `read_tmp_df()`: imports a data frame from a local temporary file that serves as a copy of a file stored remotely on Bunny.net
- `delete_local_file()`: used to do some clean-up in relation to the local temp files mentioned above

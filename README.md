# FTP Mirroring Tool

A simple Python script to mirror a remote FTP directory to a local one.

## Usage

1. Install the required packages with `pip install -r requirements.txt`
2. Run the script with `python main.py`
3. Follow the prompts to enter the remote FTP server details and the local directory to mirror to

## Download

You can download the latest version of the .exe file from the [GitHub Releases page](https://github.com/khangklj/auto_sync_ftp_client/releases).

## Features

- Downloads new files from the remote server
- Updates existing files if they have changed
- Deletes local files that are no longer present on the remote server
- Shows progress during downloads

## Known Issues

- The script does not currently handle symbolic links
- The script does not currently handle subdirectories with the same name as files

## Contributing

Pull requests are welcome! Please open an issue on GitHub to discuss any changes before submitting a pull request.

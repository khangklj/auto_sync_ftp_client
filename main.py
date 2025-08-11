import os
import ftplib
import logging
import tabulate
import sys
import json
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_remote_file_list(ftp_client: ftplib.FTP, remote_path):
    """Recursively gets all file paths and sizes from the remote FTP directory."""
    remote_files = {}
    try:
        ftp_client.cwd(remote_path)
    except ftplib.error_perm as e:
        logging.error(f"Could not access remote path {remote_path}: {e}")
        sys.exit(1)

    items = ftp_client.nlst()

    for item in items:
        # Check if it's a directory
        try:
            ftp_client.cwd(item)
            # It's a directory, recurse
            remote_files.update(
                get_remote_file_list(
                    ftp_client, os.path.normpath(os.path.join(remote_path, item))
                )
            )
            ftp_client.cwd("..")
        except ftplib.error_perm:
            # It's a file
            file_path = os.path.normpath(os.path.join(remote_path, item))
            ftp_client.voidcmd("TYPE I")
            file_size = ftp_client.size(item)
            remote_files[file_path] = file_size

    return remote_files


def get_local_file_list(local_path):
    """Recursively gets all local file paths and sizes."""
    local_files = {}
    if not os.path.exists(local_path):
        return local_files

    for root, _, files in os.walk(local_path):
        for file in files:
            file_path = os.path.normpath(os.path.join(root, file))
            local_files[file_path] = os.path.getsize(file_path)

    return local_files


def preview_changes(remote_files, local_files, local_base_path):
    """Compares file lists and prints the planned changes in a table."""
    to_download = []
    to_delete = []

    # Identify files to download or update
    for remote_path, remote_size in remote_files.items():
        local_path = os.path.join(
            local_base_path, os.path.relpath(remote_path, REMOTE_DIR)
        )
        if local_path not in local_files:
            to_download.append(["Copy", remote_path, local_path])
        elif local_files[local_path] != remote_size:
            to_download.append(["Updated", remote_path, local_path])

    # Identify files to delete
    for local_path, _ in local_files.items():
        remote_path = os.path.join(
            REMOTE_DIR, os.path.relpath(local_path, local_base_path)
        )
        if remote_path not in remote_files:
            to_delete.append([local_path])

    if len(to_download) > 0 or len(to_delete) > 0:
        # Print current time
        print("\n")
        logging.info("Detect changes")

    # Print the download/update table
    if to_download:
        headers_download = ["Action", "Remote Path", "Local Path"]
        print("Files to be downloaded/updated:")
        print(
            tabulate.tabulate(
                to_download, headers=headers_download, tablefmt="fancy_grid"
            )
        )
    else:
        print("\nNo new or updated files to download.") if PREVIEW_MODE else None

    # Print the deletion table
    if to_delete:
        headers_delete = ["Local Path"]
        print("\nFiles to be deleted:")
        print(
            tabulate.tabulate(to_delete, headers=headers_delete, tablefmt="fancy_grid")
        )
    else:
        print("\nNo files to be deleted.") if PREVIEW_MODE else None
    print("===============================") if PREVIEW_MODE else None

    return to_download, to_delete


def mirror_ftp_directory(ftp_client: ftplib.FTP, to_download, to_delete):
    """
    Mirrors the remote directory to the local one, showing progress during downloads.
    """

    total_files = len(to_download)
    files_processed = 0

    # Helper function to track download progress
    def handle_binary(block):
        nonlocal bytes_so_far
        local_file.write(block)
        bytes_so_far += len(block)
        percent = (bytes_so_far / total_size) * 100
        sys.stdout.write(
            f"\r[{files_processed}/{total_files}] {remote_path}: {percent:.2f}% complete"
        )
        sys.stdout.flush()

    # Download or update files
    for action, remote_path, local_path in to_download:
        print(f"{action.upper()}: {remote_path} -----> {local_path}")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        try:
            # Get the file size for progress calculation
            ftp_client.voidcmd("TYPE I")
            total_size = ftp_client.size(remote_path)
            if total_size is None:
                # Handle cases where size can't be determined
                logging.warning(
                    f"Could not get size for {remote_path}. Downloading without progress display."
                )
                with open(local_path, "wb") as local_file:
                    ftp_client.retrbinary(f"RETR {remote_path}", local_file.write)
                continue
            files_processed += 1
            bytes_so_far = 0
            with open(local_path, "wb") as local_file:
                # Use retrbinary with the custom callback for progress
                ftp_client.retrbinary(f"RETR {remote_path}", handle_binary)

            sys.stdout.write("\n")  # Newline after a completed download progress line
        except ftplib.all_errors as e:
            logging.error(f"Failed to download {remote_path}: {e}")
            # Consider cleaning up the partially downloaded file here

    # Delete files in local
    for local_path_list in to_delete:
        local_path = local_path_list[0]
        try:
            os.remove(local_path)
            print(f"DELETE: {local_path}")
        except OSError as e:
            logging.error(f"Error deleting file {local_path}: {e}")


if __name__ == "__main__":
    # os.system("mode CON: COLS=200 LINES=40")

    if not os.path.exists("config.json"):
        # Create a template config file
        with open("config.json", "w") as f:
            f.write(
                "{\n"
                '"FTP_HOST": "127.0.0.1",\n '
                '"FTP_USER": "anonymous",\n'
                ' "FTP_PASSWORD": "anonymous",\n'
                ' "REMOTE_DIR": "\\\\MXF",\n'
                ' "LOCAL_DIR": "D:\\\\",\n'
                ' "PREVIEW_MODE": true,\n'
                ' "INTERVAL_TIME": 120\n'
                "}"
            )
        print("Created config.json template. Please edit and run again.")
        os.system("pause")
        sys.exit(0)

    with open("config.json") as f:
        config = json.load(f)

    FTP_HOST = config["FTP_HOST"]
    FTP_USER = config["FTP_USER"]
    FTP_PASSWORD = config["FTP_PASSWORD"]
    REMOTE_DIR = os.path.normpath(config["REMOTE_DIR"])
    LOCAL_DIR = os.path.normpath(config["LOCAL_DIR"])
    PREVIEW_MODE = config["PREVIEW_MODE"]
    INTERVAL_TIME = config["INTERVAL_TIME"]

    print(f"Connecting to FTP host {FTP_HOST}")
    print(f"Watch remote folder {REMOTE_DIR}")
    print(f"Interval time: {INTERVAL_TIME} seconds")
    if PREVIEW_MODE:
        print("Preview mode enabled. Turn off to monitor changes.")
    print(f"Press Ctrl+C to exit.")
    try:
        while True:
            ftp = None
            try:
                ftp = ftplib.FTP(FTP_HOST)
                ftp.login(FTP_USER, FTP_PASSWORD)
                if PREVIEW_MODE:
                    print("Logged in to FTP server successfully.")
                ftp.set_pasv(True)

                remote_files = get_remote_file_list(ftp, REMOTE_DIR)
                local_files = get_local_file_list(LOCAL_DIR)

                to_download, to_delete = preview_changes(
                    remote_files, local_files, LOCAL_DIR
                )

                if PREVIEW_MODE:
                    action = input("Do you want to commit?[Y/n] ")
                    if action.lower() != "y":
                        sys.exit(0)

                mirror_ftp_directory(ftp, to_download, to_delete)
                if PREVIEW_MODE:
                    break
            except ftplib.all_errors as e:
                logging.error(f"FTP Error: {e}")
                break
            finally:
                if ftp:
                    ftp.quit()
                    if PREVIEW_MODE:
                        print("Disconnected from FTP server.")
            time.sleep(INTERVAL_TIME)
    except KeyboardInterrupt:
        print("Program interrupted by user. Exiting...")
    finally:
        os.system("pause")

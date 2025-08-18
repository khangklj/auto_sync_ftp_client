import os
import ftplib
import logging
import tabulate
import sys
import json
import time
import sqlite3

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Enum class
# 0 = Not downloaded (in remote not in local or incompleted file in local),
# 1 = Downloaded (in local and remote),
# 2 = Deleted (in local but not in remote)
class VideoStatus:
    NOT_DOWNLOADED = 0
    DOWNLOADED = 1
    UPDATED = 2
    DELETED = 3


def scan_remote(ftp_client: ftplib.FTP, remote_dir: str):
    try:
        remote_files = ftp_client.nlst()

        for remote_file in remote_files:
            ftp_client.voidcmd("TYPE I")
            remote_file_size = ftp_client.size(remote_file)
            remote_dir = os.path.normpath(os.path.join(remote_dir, remote_file))
            cur.execute("SELECT * FROM videos WHERE video_id = ?", (remote_file,))
            row: sqlite3.Row = cur.fetchone()
            if row is None:
                cur.execute(
                    "INSERT INTO videos (video_id, video_status, video_remote_size) VALUES (?, ?, ?)",
                    (remote_file, VideoStatus.NOT_DOWNLOADED, remote_file_size),
                )
            elif remote_file_size != row["video_remote_size"]:
                cur.execute(
                    "UPDATE videos SET video_status = ?, video_remote_size = ? WHERE video_id = ?",
                    (VideoStatus.UPDATED, remote_file_size, remote_file),
                )
        conn.commit()

        cur.execute(
            "SELECT * FROM videos WHERE video_status = ?", (VideoStatus.DOWNLOADED,)
        )
        rows: list[sqlite3.Row] = cur.fetchall()
        for row in rows:
            if row["video_id"] not in remote_files:
                cur.execute(
                    "UPDATE videos SET video_status = ? WHERE video_id = ?",
                    (VideoStatus.DELETED, row["video_id"]),
                )
        conn.commit()
    except Exception as e:
        logging.error(f"Error scanning remote directory {remote_dir}: {e}")
        sys.exit(1)


def scan_local():
    try:
        local_paths = get_local_paths()
        cur.execute(
            "SELECT * FROM videos WHERE video_status = ?",
            (VideoStatus.DOWNLOADED,),
        )
        rows: list[sqlite3.Row] = cur.fetchall()
        local_file_ids = []
        for local_path in local_paths:
            local_file_id = os.path.basename(local_path)
            local_file_ids.append(local_file_id)

        for row in rows:
            if row["video_id"] not in local_file_ids:
                cur.execute(
                    "UPDATE videos SET video_status = ? WHERE video_id = ?",
                    (VideoStatus.NOT_DOWNLOADED, row["video_id"]),
                )
    except Exception as e:
        logging.error(f"Error scanning local directory: {e}")
        sys.exit(1)


def get_local_paths():
    """Recursively gets all local file paths and sizes."""
    local_files = []
    if not os.path.exists(LOCAL_DIR):
        return local_files

    for root, _, files in os.walk(LOCAL_DIR):
        for file in files:
            file_path = os.path.normpath(os.path.join(root, file))
            local_files.append(file_path)

    return local_files


def preview_changes():
    """Compares file lists and prints the planned changes in a table."""
    table = []
    download_count = 0
    delete_count = 0
    update_count = 0

    rows: list[sqlite3.Row] = cur.execute(
        "SELECT * FROM videos ORDER BY video_id ASC"
    ).fetchall()
    for row in rows:
        video_id = row["video_id"]
        video_status = row["video_status"]
        if video_status == VideoStatus.NOT_DOWNLOADED:
            table.append(["Download", video_id])
            download_count += 1
        elif video_status == VideoStatus.DELETED:
            table.append(["Delete", video_id])
            delete_count += 1
        elif video_status == VideoStatus.UPDATED:
            table.append(["Update", video_id])
            update_count += 1

    if download_count == 0 and delete_count == 0 and update_count == 0:
        if PREVIEW_MODE:
            print("No changes detected")
        return

    # Print the table
    headers = ["Action", "Video ID"]
    print("Preview of changes:")
    print(tabulate.tabulate(table, headers=headers, tablefmt="fancy_grid"))
    print(f"Total {download_count} files to download.")
    print(f"Total {update_count} files to update.")
    print(f"Total {delete_count} files to delete.")
    print("===============================")


def mirror_ftp_directory(ftp_client: ftplib.FTP):
    cur.execute(
        "SELECT * FROM videos WHERE video_status = ? or video_status = ? or video_status = ? ORDER BY video_id ASC",
        (VideoStatus.NOT_DOWNLOADED, VideoStatus.DELETED, VideoStatus.UPDATED),
    )
    rows: list[sqlite3.Row] = cur.fetchall()
    total_files = len(rows)
    count = 1
    # Delete files in local
    for row in rows:
        if row["video_status"] == VideoStatus.DELETED:
            local_path = os.path.join(LOCAL_DIR, row["video_id"])
            try:
                os.remove(local_path)
                print(f"[{count}/{total_files}] DELETE: {local_path}")
                cur.execute("DELETE FROM videos WHERE video_id = ?", (row["video_id"],))
                conn.commit()
                count += 1
            except OSError as e:
                logging.error(f"Error deleting file {local_path}: {e}")

    def callback(block):
        # Print total bytes downloaded
        nonlocal total_bytes
        total_bytes += len(block)
        sys.stdout.write(
            f"\r[{count}/{total_files}] {"DOWNLOAD" if row['video_status'] == VideoStatus.NOT_DOWNLOADED else "UPDATE"}: {remote_path} -----> {local_path} ---- {total_bytes / 1024 / 1024 / 1024:.2f} GB"
        )
        sys.stdout.flush()

    # Download or update files
    for row in rows:
        if (
            row["video_status"] == VideoStatus.NOT_DOWNLOADED
            or row["video_status"] == VideoStatus.UPDATED
        ):
            remote_path = os.path.join(REMOTE_DIR, row["video_id"])
            local_path = os.path.join(LOCAL_DIR, row["video_id"])
            try:
                total_bytes = 0
                with open(local_path, "wb") as local_file:
                    ftp_client.retrbinary(
                        f"RETR {row["video_id"]}",
                        callback=callback,
                    )
                cur.execute(
                    "UPDATE videos SET video_status = ? WHERE video_id = ?",
                    (VideoStatus.DOWNLOADED, row["video_id"]),
                )
                conn.commit()
                sys.stdout.write(" DONE")
                sys.stdout.write("\n")
                count += 1
            except ftplib.all_errors as e:
                logging.error(f"Error downloading file {remote_path}: {e}")


if __name__ == "__main__":
    os.system("mode CON: COLS=200")

    # Prepare database
    conn = None
    try:
        if not os.path.exists("database"):
            os.mkdir("database")
        conn = sqlite3.connect("database/qlps.db")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        create_table = """
            CREATE TABLE IF NOT EXISTS videos (
                video_id STRING PRIMARY KEY,
                video_status INTEGER,
                video_remote_size INTEGER                
            );
        """
        cur.execute(create_table)
        conn.commit()

    except Exception as e:
        logging.error(f"Failed during setup database: {e}")
        sys.exit(1)

    if not os.path.exists("config.json"):
        # Create a template config file
        with open("config.json", "w") as f:
            f.write(
                "{\n"
                ' "FTP_HOST": "127.0.0.1",\n'
                ' "FTP_PORT": 21,\n'
                ' "FTP_USER": "anonymous",\n'
                ' "FTP_PASSWORD": "anonymous",\n'
                ' "REMOTE_DIR": "MXF",\n'
                ' "LOCAL_DIR": "D:\\\\TestFolder",\n'
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
    FTP_PORT = config["FTP_PORT"]
    FTP_USER = config["FTP_USER"]
    FTP_PASSWORD = config["FTP_PASSWORD"]
    REMOTE_DIR = os.path.normpath(config["REMOTE_DIR"])
    LOCAL_DIR = os.path.normpath(config["LOCAL_DIR"])
    PREVIEW_MODE = config["PREVIEW_MODE"]
    INTERVAL_TIME = config["INTERVAL_TIME"]

    print(f"Connecting to FTP {FTP_HOST}:{FTP_PORT}")
    print(f"Watch remote folder {REMOTE_DIR}")
    print(f"Interval time: {INTERVAL_TIME} seconds")
    if PREVIEW_MODE:
        print("Preview mode enabled. Turn off to monitor changes.")
    print(f"Press Ctrl+C to exit.")
    try:
        while True:
            ftp = None
            try:
                ftp = ftplib.FTP()
                ftp.connect(FTP_HOST, FTP_PORT)
                ftp.login(FTP_USER, FTP_PASSWORD)
                ftp.cwd(REMOTE_DIR)
                if PREVIEW_MODE:
                    print("Logged in to FTP server successfully.")
                ftp.set_pasv(True)

                scan_remote(ftp, REMOTE_DIR)
                scan_local()
                preview_changes()

                if PREVIEW_MODE:
                    action = input("Do you want to commit?[Y/n] ")
                    if action.lower() != "y":
                        sys.exit(0)

                mirror_ftp_directory(ftp)
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
                conn.close()
            time.sleep(INTERVAL_TIME)
    except KeyboardInterrupt:
        print("Program interrupted by user. Exiting...")
    finally:
        os.system("pause")

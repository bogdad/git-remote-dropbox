import os
from typing import TextIO

APP_KEY: str = "h7d8z1irmz0r7lp"
APP_FOLDER_APP_KEY: str = "w966iez5k027tcl"
DEVNULL: TextIO = open(os.devnull, "w")  # noqa: SIM115
PROCESSES: int = 8
MAX_RETRIES: int = 3
# during fetch, after this many objects have been downloaded via the object
# graph walk, switch to bulk downloading all missing remote objects
BULK_FETCH_THRESHOLD: int = 50
# during fetch, give up if no download completes for this many seconds in a row,
# STALL_LIMIT times, instead of waiting forever
STALL_TIMEOUT: int = 60
STALL_LIMIT: int = 3
CHUNK_SIZE: int = 50 * 1024 * 1024  # 50 megabytes

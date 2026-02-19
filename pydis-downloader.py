#!/usr/bin/env python3
import os
import shutil
import platform
import requests
import argparse
import json
from datetime import datetime, time as dt_time
from pathlib import Path
import time
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from functools import wraps
import threading
import socket
from tqdm import tqdm
import random
from urllib.parse import urlparse, urlunparse, parse_qsl
import math
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def log_debug_point(label, url=None):
    """Write a timestamped debug message to tqdm for live tracing."""
    now = datetime.now().strftime("%H:%M:%S")
    tqdm.write(f"[{now}] DEBUG: {label}{' | ' + url if url else ''}")

# ----------------- New / Updated: download config & shared session -----------------
# Tune these values to your needs
DOWNLOAD_MAX_CONCURRENT = 4          # how many concurrent downloads at once
DOWNLOAD_MAX_RETRIES = 3
DOWNLOAD_BACKOFF_FACTOR = 0.5
DOWNLOAD_CONNECT_TIMEOUT = 10        # seconds (TCP connect)
DOWNLOAD_READ_TIMEOUT = 20           # seconds (per-socket-read)
DOWNLOAD_STALL_TIMEOUT = 45          # seconds without progress -> consider stalled
DOWNLOAD_CHUNK_SIZE = 8192

# Shared semaphore to limit concurrent streaming downloads (prevents resource exhaustion)
DOWNLOAD_SEMAPHORE = threading.Semaphore(DOWNLOAD_MAX_CONCURRENT)

# Storage check configuration (set by main() from CLI args)
_STORAGE_PATH = None
_STORAGE_THRESHOLD_GB = 5.0
_STORAGE_WAIT_SECONDS = 600

# Shared requests.Session with a simple adapter for connection pooling
def _make_download_session():
    s = requests.Session()
    # keep pool reasonably sized; we won't rely on Retry here because we handle retries manually
    adapter = requests.adapters.HTTPAdapter(pool_maxsize=20, max_retries=0)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# One session instance reused by download functions
_DOWNLOAD_SESSION = _make_download_session()
# ------------------------------------------------------------------------------------

class DownloadManager:
    def __init__(
        self,
        max_concurrent=4,
        max_retries=3,
        backoff_factor=0.5,
        connect_timeout=10,
        read_timeout=20,
        stall_timeout=45,
        chunk_size=8192,
    ):
        """
        :param max_concurrent: max parallel downloads (Semaphore)
        :param max_retries: number of attempts per file (network errors / stalls)
        :param backoff_factor: used for exponential backoff between retries
        :param connect_timeout: seconds for TCP connect timeout
        :param read_timeout: seconds for socket read timeout (applies per-socket-read)
        :param stall_timeout: custom stall detector: if no chunk written for this many seconds -> treat as stall
        :param chunk_size: bytes per iter_content chunk
        """
        self.semaphore = threading.Semaphore(max_concurrent)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.stall_timeout = stall_timeout
        self.chunk_size = chunk_size

        # Retry config for connection-level failures (idempotent-ish GETs)
        self._retry_strategy = Retry(
            total=0,  # we will handle retries manually to control behavior during streaming
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )

    def _make_session(self):
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=self._retry_strategy, pool_maxsize=20)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def download(self, task):
        """
        :param task: tuple (url, dest_folder, prefix_or_filename, blacklist_set, progress_data_dict_optional)
                     - prefix_or_filename can be None (will be inferred from URL)
        :returns: (success: bool, error or final_path_or_message)
        """
        url, dest_folder, prefix, blacklist, progress_data = task

        # quick skip if blacklisted
        if blacklist is not None and url in blacklist:
            return False, "blacklisted"

        # ensure dest folder exists
        os.makedirs(dest_folder, exist_ok=True)

        # infer filename
        parsed = urlparse(url)
        filename = prefix if prefix else os.path.basename(parsed.path) or "download"
        safe_name = filename.replace("/", "_")
        final_path = os.path.join(dest_folder, safe_name)
        temp_path = final_path + ".part"

        # If file already exists completely, skip
        if os.path.exists(final_path):
            return True, final_path

        # Acquire semaphore (limits concurrent downloads)
        with self.semaphore:
            attempt = 0
            while attempt <= self.max_retries:
                attempt += 1
                session = self._make_session()
                try:
                    # streaming GET with timeouts: (connect_timeout, read_timeout)
                    with session.get(url, stream=True, timeout=(self.connect_timeout, self.read_timeout)) as resp:
                        # Accept 200-299 as success; otherwise raise to trigger retry
                        resp.raise_for_status()

                        # Write to temporary file and monitor progress
                        last_progress = time.time()
                        bytes_written = 0
                        with open(temp_path, "wb") as fh:
                            for chunk in resp.iter_content(chunk_size=self.chunk_size):
                                # chunk can be None or b''
                                if chunk:
                                    fh.write(chunk)
                                    bytes_written += len(chunk)
                                    last_progress = time.time()

                                    # optional: update progress_data if provided (thread-safe-ish)
                                    if isinstance(progress_data, dict):
                                        # keep only a few fields to avoid heavy I/O
                                        progress_data[url] = {
                                            "bytes": bytes_written,
                                            "last_update": last_progress,
                                        }

                                # stall detector (in addition to requests read timeout)
                                if time.time() - last_progress > self.stall_timeout:
                                    raise IOError(f"Stalled for {self.stall_timeout}s")

                        # Finished writing successfully -> atomically move to final path
                        os.replace(temp_path, final_path)

                        # optionally add to blacklist set (if provided and mutable)
                        if isinstance(blacklist, set):
                            blacklist.add(url)

                        return True, final_path

                except requests.exceptions.RequestException as re:
                    err = f"request error: {re}"
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"

                # CLEAN UP partial file if it exists (so next attempt starts fresh)
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass

                # If we've exhausted retries, return failure
                if attempt > self.max_retries:
                    return False, f"failed after {self.max_retries} attempts: {err}"

                # Exponential backoff (jittered)
                backoff = self.backoff_factor * (2 ** (attempt - 1))
                # Add small jitter so many threads don't retry in lock-step
                jitter = backoff * (0.1 * (0.5 - random.random()))
                sleep_time = max(0.1, backoff + jitter)
                time.sleep(sleep_time)

            # fallback (should not reach)
            return False, "unknown failure"

def rate_limiter(max_calls, per_seconds):
    """
    Decorator to limit how many times a function can be started
    within a given time window, across threads.
    If a skip predicate is attached to the wrapper (via wrapper.set_skip_pred),
    the predicate is invoked before any rate-limiter waiting. If the predicate
    returns True, the wrapped function is called immediately (no rate-limit wait).
    """
    lock = threading.Lock()
    call_times = deque()

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Check for an attached skip predicate (fast-path)
            skip_pred = getattr(wrapper, "_skip_pred", None)
            if callable(skip_pred):
                try:
                    if skip_pred(*args, **kwargs):
                        # skip the rate-limit and run immediately
                        return func(*args, **kwargs)
                except Exception:
                    # If predicate errors, fall through to normal rate-limiting behavior
                    pass

            # Normal rate-limiter behavior
            nonlocal call_times
            while True:
                with lock:
                    now = time.time()
                    # Drop old calls outside the window
                    while call_times and now - call_times[0] > per_seconds:
                        call_times.popleft()

                    if len(call_times) < max_calls:
                        call_times.append(now)
                        break

                    # Too many calls: figure out how long to wait
                    sleep_time = per_seconds - (now - call_times[0])

                # sleep outside the lock
                if sleep_time > 0:
                    time.sleep(sleep_time)

            # actually run the function
            return func(*args, **kwargs)

        # Small helper to allow setting a skip predicate later:
        def set_skip_pred(predicate):
            """predicate is callable(*args, **kwargs) -> bool"""
            setattr(wrapper, "_skip_pred", predicate)

        wrapper.set_skip_pred = set_skip_pred
        return wrapper
    return decorator

# Where to store the list of already-seen URLs
DOWNLOAD_LINKS_FILE = os.path.join(os.getcwd(), "downloaded_links.txt")

def _load_link_index(path):
    s = set()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                u = line.strip()
                if u:
                    s.add(u)
    except FileNotFoundError:
        # no index yet
        pass
    return s

# in-memory set + lock (module-level, shared by threads)
_LINK_INDEX_LOCK = threading.Lock()
_LINK_INDEX_SET = _load_link_index(DOWNLOAD_LINKS_FILE)

def _record_link(url):
    """Append url to persistent index (thread-safe) and update in-memory set."""
    with _LINK_INDEX_LOCK:
        if url in _LINK_INDEX_SET:
            return
        with open(DOWNLOAD_LINKS_FILE, "a", encoding="utf-8") as fh:
            fh.write(url + "\n")
        _LINK_INDEX_SET.add(url)

def _normalize_url(url, drop_query_params=None):
    """
    Normalize URL to reduce duplicates due to tracking params.
    - For cdn.discordapp.com, strip ?ex= and everything after it.
    - Otherwise, remove common utm_* params (unless drop_query_params overrides).
    """
    try:
        p = urlparse(url)

        # Special case: Discord CDN links -> strip everything after ?ex=
        if "cdn.discordapp.com" in p.netloc:
            # keep only scheme, netloc, path
            return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

        # Default normalization (keep query but drop tracking params)
        if drop_query_params is None:
            drop_query_params = {
                "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"
            }
        qs = [(k, v) for k, v in parse_qsl(p.query) if k not in drop_query_params]
        new_q = "&".join(f"{k}={v}" for k, v in qs)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

    except Exception:
        # if parsing fails, return original
        return url
# --- END INSERT ---

# Helper stuff
def wait_for_wifi_connection(check_interval=600):
	"""
	Check if the device is connected to the internet via Wi-Fi.
	If not connected, wait for a specified interval before retrying.

	Args:
	- check_interval: Time in seconds between connection checks (default: 3600 seconds = 1 hour).
	"""
	while True:
		try:
			# Attempt to connect to a reliable host (Google DNS server)
			socket.create_connection(("8.8.8.8", 53), timeout=5)
			# tqdm.write("f\nWi-Fi connection detected. Continuing execution.")
			break  # Exit loop if connection is successful
		except (socket.timeout, OSError):
			tqdm.write(f"\nNo Wi-Fi connection detected. Retrying in {check_interval // 60} minutes...")
			time.sleep(check_interval)

# Example usage: Call this function anywhere to pause execution until Wi-Fi is connected
# wait_for_wifi_connection()

def check_storage(drive_path: str, threshold_gb: float, wait_seconds: int = 600):
    """
    Checks the available storage space on the specified drive.
    If the free space is less than the given threshold (in GB), it waits and checks again.
    Cross-platform: uses shutil.disk_usage() which works on Windows, macOS, and Linux.

    :param drive_path: Path to the drive (e.g., '/' for Linux/macOS, 'C:\\' for Windows, or any directory path)
    :param threshold_gb: The threshold in gigabytes
    :param wait_seconds: Seconds to wait before rechecking when below threshold (default: 600)
    """
    while True:
        # Get disk usage statistics (cross-platform)
        usage = shutil.disk_usage(drive_path)
        free_space_gb = usage.free / (1024 ** 3)  # Convert bytes to GB

        #tqdm.write(f"Free space on {drive_path}: {free_space_gb:.2f} GB")

        if free_space_gb < threshold_gb:
            wait_min = wait_seconds / 60
            tqdm.write(f"Warning: Free space ({free_space_gb:.2f} GB) below {threshold_gb} GB on '{drive_path}'! Checking again in {wait_min:.0f} minutes...")
            time.sleep(wait_seconds)
        else:
            #tqdm.write("Sufficient storage available. Exiting check.")
            break

# Example usage:
# check_storage('/mnt/external_drive', 10)  # Linux/macOS
# check_storage('C:\\', 10)  # Windows

def pause_during_time_range(start_time_str: str, end_time_str: str, check_interval: int = 3600):
    """
    Pauses code execution if the current time is between start_time and end_time.

    :param start_time_str: Start time in "HH:MM" 24-hour format
    :param end_time_str: End time in "HH:MM" 24-hour format
    :param check_interval: Time in seconds to wait before rechecking (default is 60 seconds)
    """
    # Convert strings to datetime.time objects
    start = datetime.strptime(start_time_str, "%H:%M").time()
    end = datetime.strptime(end_time_str, "%H:%M").time()

    while True:
        now = datetime.now().time()

        # Check if now is between start and end (handles overnight spans)
        if (start < end and start <= now < end) or (start > end and (now >= start or now < end)):
            tqdm.write(f"Current time {now.strftime('%H:%M:%S')} is within the pause range. Sleeping for {check_interval/60/60} hour(s)...")
            time.sleep(check_interval)
        else:
            #tqdm.write(f"Current time {now.strftime('%H:%M:%S')} is outside the pause range. Continuing execution.")
            break

# Example usage:
# pause_during_time_range("23:00", "01:00")  # Pauses between 11 PM and 1 AM

#============================================================================================ Sanitizing

def sanitize_filename(filename, max_length=255):
	"""Sanitize a file name by removing problematic characters and limiting its length."""
	# Remove invalid characters
	filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
	# Replace multiple spaces or underscores with a single underscore
	filename = re.sub(r'\s+', '_', filename)
	filename = re.sub(r'_+', '_', filename)
	# Truncate to max_length, preserving the file extension
	if len(filename) > max_length:
		name, ext = os.path.splitext(filename)
		filename = f"{name[:max_length - len(ext)]}{ext}"
	return filename
	tqdm.write("Sanitization complete.")


def sanitize_name_with_conflicts(name, existing_names):
	"""
	Sanitize a name and ensure uniqueness by appending numbers for conflicts.
	Args:
	- name: The name to sanitize.
	- existing_names: A set of already-used sanitized names.

	Returns:
	- A unique sanitized name.
	"""
	sanitized = sanitize_filename(name)
	original_sanitized = sanitized
	counter = 1

	# Resolve conflicts by appending a counter
	while sanitized in existing_names:
		sanitized = f"{original_sanitized}_{counter}"
		counter += 1

	existing_names.add(sanitized)
	return sanitized

def sanitize_path(path, existing_names):
	"""Sanitize a single path and rename it if necessary."""
	dirname, basename = os.path.split(path)
	sanitized_name = sanitize_name_with_conflicts(basename, existing_names)
	sanitized_path = os.path.join(dirname, sanitized_name)
	if path != sanitized_path:
		tqdm.write(f"Renaming: '{path}' -> '{sanitized_path}'")
		os.rename(path, sanitized_path)
	return sanitized_path

#============================================================================================ Helper functions (APIs and file loading/checking)

def api_request(url, headers, params=None):
    """
    Make an API request with special handling:
      - 429: Wait for the 'Retry-After' delay then retry.
      - 403: Raise an exception (do not retry).
      - 404: Raise an exception and ask to check wifi connection
      - 200: Return JSON.
      - Other errors: Wait briefly and retry.
    """
    wait_for_wifi_connection()

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 1))
            tqdm.write(f"Rate limited. Retrying in {retry_after} seconds...")
            time.sleep(retry_after)
        elif response.status_code == 403:
            raise Exception("Received 403 Unauthorized response; stopping retries.")
        elif response.status_code == 200:
            # tqdm.write(response.json())
            return response.json()
        elif response.status_code == 404:
            raise Exception("Received 404 Not found response; please check wifi connection")
        else:
            tqdm.write(f"Error {response.status_code} received; retrying in 1 second...")
            time.sleep(1)

def ensure_dir(path):
	"""Ensure directory exists."""
	Path(path).mkdir(parents=True, exist_ok=True)

NAME_INDEX_FILE = ".discord_name_index.json"
_NAME_INDEX_LOCK = threading.Lock()

def load_name_index(output_dir):
    path = os.path.join(output_dir, NAME_INDEX_FILE)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"servers": {}, "channels": {}}


def save_name_index(output_dir, index):
    path = os.path.join(output_dir, NAME_INDEX_FILE)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)


def rename_folder_if_needed(base_dir, old_name, new_name):
    if old_name == new_name:
        return new_name

    old_path = os.path.join(base_dir, old_name)
    new_path = os.path.join(base_dir, new_name)

    if os.path.exists(old_path) and not os.path.exists(new_path):
        tqdm.write(f"Renaming folder: '{old_name}' → '{new_name}'")
        os.rename(old_path, new_path)

    return new_name

def load_json(file_path):
	"""Load JSON data from a file."""
	if os.path.exists(file_path):
		with open(file_path, "r", encoding="utf-8") as f:
			return json.load(f)
	return []


def save_json(file_path, data):
	"""Save JSON data to a file."""
	with open(file_path, "w", encoding="utf-8") as f:
		json.dump(data, f, indent=4)


def parse_channels_file(file_path):
	"""Parse channels.txt formatted as channel_id per line."""
	with open(file_path, "r", encoding="utf-8") as f:
		lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
		return [line for line in lines if ":" in line]

def get_dm_folder_name(channel_metadata, output_dir):
	"""
	Generate or update a folder name for a DM channel, formatted as 'username_nickname'.
	Args:
	- channel_metadata: Metadata for the channel.
	- output_dir: Base output directory where folders are stored.
	Returns:
	- A sanitized folder name for the DM or group DM.
	"""
	if channel_metadata["type"] == 1:  # Direct Message
		recipient = channel_metadata.get("recipients", [{}])[0]
		username = sanitize_filename(recipient.get("username", f"DM-{channel_metadata['id']}"))
		nickname = sanitize_filename(recipient.get("global_name", ""))  # Nickname may be empty

		# Construct the expected folder name
		new_folder_name = f"{username}_({nickname})" if nickname else username

		# Search for existing folders with the username
		for folder in os.listdir(output_dir):
			if folder.startswith(username):
				# Check if the folder already has the correct nickname
				if folder == new_folder_name:
					return new_folder_name

				# If the nickname differs, rename the folder
				old_folder_path = os.path.join(output_dir, folder)
				new_folder_path = os.path.join(output_dir, new_folder_name)
				tqdm.write(f"Renaming folder '{old_folder_path}' to '{new_folder_path}' due to nickname change.")
				os.rename(old_folder_path, new_folder_path)
				return new_folder_name

		# No matching folder found, return the new folder name
		return new_folder_name

	elif channel_metadata["type"] == 3:  # Group DM
		return sanitize_filename(channel_metadata.get("name", f"GroupDM-{channel_metadata['id']}"))

	return None




#============================================================================================ Core functions (Downloading messages, media, embeds)

def fetch_channel_name(token, channel_id, server_dir, output_dir):
    headers = {"Authorization": token}
    url = f"https://discord.com/api/v9/channels/{channel_id}"
    channel_name = api_request(url, headers).get("name", f"channel_{channel_id}")

    sanitized = sanitize_filename(channel_name)

    with _NAME_INDEX_LOCK:
        index = load_name_index(output_dir)
        old_name = index["channels"].get(str(channel_id))

        if old_name:
            sanitized_old = sanitize_filename(old_name)
            sanitized = rename_folder_if_needed(
                server_dir,
                sanitized_old,
                sanitized
            )

        index["channels"][str(channel_id)] = channel_name
        save_name_index(os.path.dirname(server_dir), index)

    return sanitized

def fetch_server_name(token, guild_id, output_dir):
    headers = {"Authorization": token}
    url = f"https://discord.com/api/v9/guilds/{guild_id}"
    server_name = api_request(url, headers).get("name", f"server_{guild_id}")

    sanitized = sanitize_filename(server_name)

    with _NAME_INDEX_LOCK:
        index = load_name_index(output_dir)
        old_name = index["servers"].get(str(guild_id))

        if old_name:
            sanitized_old = sanitize_filename(old_name)
            sanitized = rename_folder_if_needed(
                output_dir,
                sanitized_old,
                sanitized
            )

        index["servers"][str(guild_id)] = server_name
        save_name_index(output_dir, index)

    return sanitized

def fetch_messages(token, channel_id, channel_dir, save_raw=False):
	"""Fetch messages and optionally save raw data."""
	headers = {"Authorization": token}
	url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
	params = {"limit": 100}

	raw_file = os.path.join(channel_dir, "messages_raw.json") if save_raw else None
	raw_data = load_json(raw_file) if save_raw else []
	existing_ids = {msg["id"] for msg in raw_data}

	messages_file = os.path.join(channel_dir, "messages.txt")
	existing_timestamps = {line.split(" - ", 1)[0] for line in open(messages_file, "r", encoding="utf-8")} if os.path.exists(messages_file) else set()

	new_messages = []
	message_count = 0
	last_timestamp = None

	while True:
		messages = api_request(url, headers, params)
		if not messages:
			break

		new_batch = [msg for msg in messages if msg["id"] not in existing_ids and msg["timestamp"] not in existing_timestamps]
		new_messages.extend(new_batch)
		existing_ids.update(msg["id"] for msg in new_batch)
		existing_timestamps.update(msg["timestamp"] for msg in new_batch)

		if new_batch:
			last_timestamp = new_batch[-1]["timestamp"]

		params["before"] = messages[-1]["id"]
		message_count += len(new_batch)

		if not new_batch:
			break

		tqdm.write(f"Downloaded {message_count} messages so far. Last timestamp: {last_timestamp}")

	# Save new raw data
	if save_raw and new_messages:
		save_json(raw_file, raw_data + new_messages)

	# Save formatted messages
	if new_messages:
		with open(messages_file, "a", encoding="utf-8") as f:
			for msg in reversed(new_messages):
				f.write(f"{msg['timestamp']} - {msg['author']['username']} (aka: {msg['author'].get('global_name', 'Unknown')}): {msg.get('content', '')}\n")

	return raw_data + new_messages if save_raw else new_messages

@rate_limiter(max_calls=20, per_seconds=1)
def download_attachment(attachment, media_dir, message_id, counter, total_attachments, lock):
    """
    Robust download: streaming with timeouts, stall detection, retries, .part temp file,
    atomic rename to final path, and blacklist/index handling. Keeps the same external interface.
    """
    url = attachment["url"]
    filename = attachment["filename"]
    base_name, ext = os.path.splitext(filename)
    # Use attachment ID if available, otherwise fallback to random number to prevent overwrites
    attachment_id = attachment.get("id", random.randint(1, 1000))
    unique_name = f"{message_id}_{attachment_id}_{base_name}"
    file_path = os.path.join(media_dir, f"{unique_name}{ext}")
    temp_path = file_path + ".part"

    # Normalize and quick-skip if we've already downloaded it
    _check_url = _normalize_url(url)
    with _LINK_INDEX_LOCK:
        if _check_url in _LINK_INDEX_SET:
            with lock:
                counter[0] += 1
                remaining = total_attachments - counter[0]
                tqdm.write(f"Skipping already-downloaded file: {filename} | Remaining: {remaining}")
            return

    # If final file already exists (race-safe check)
    if os.path.exists(file_path):
        with lock:
            counter[0] += 1
            remaining = total_attachments - counter[0]
            tqdm.write(f"Skipping already downloaded file (exists): {filename} | Remaining: {remaining}")
        # record normalized link for completeness
        _record_link(_check_url)
        return

    wait_for_wifi_connection()
    check_storage(_STORAGE_PATH, _STORAGE_THRESHOLD_GB, _STORAGE_WAIT_SECONDS)
    #pause_during_time_range("07:00", "11:00")

    # Acquire semaphore (limits how many streaming downloads run concurrently)
    with DOWNLOAD_SEMAPHORE:
        session = _DOWNLOAD_SESSION
        attempt = 0
        last_err = None
        while attempt < DOWNLOAD_MAX_RETRIES:
            attempt += 1
            try:
                with session.get(url, stream=True, timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get("Content-Length", 0) or 0)

                    # Ensure directory exists
                    ensure_dir(media_dir)

                    bytes_written = 0
                    last_progress = time.time()

                    # Ensure partial file removed from previous aborted attempt
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass

                    # Write streaming to .part file and show per-file tqdm
                    with open(temp_path, "wb") as fh, tqdm(
                        total=total_size if total_size > 0 else None,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=filename[:20],
                        leave=False
                    ) as pbar:
                        for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                            if chunk:
                                fh.write(chunk)
                                bytes_written += len(chunk)
                                last_progress = time.time()
                                pbar.update(len(chunk))

                            # Stall detection: no progress for a while
                            if time.time() - last_progress > DOWNLOAD_STALL_TIMEOUT:
                                raise IOError(f"Stalled for >{DOWNLOAD_STALL_TIMEOUT}s")

                    # Move .part -> final atomically
                    os.replace(temp_path, file_path)

                    # Record link and update counters
                    _record_link(_check_url)
                    with lock:
                        counter[0] += 1
                        remaining = total_attachments - counter[0]
                        tqdm.write(f" Remaining: {remaining} | [Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] | Successfully downloaded attachment: {file_path}")
                    return

            except requests.exceptions.RequestException as re:
                last_err = f"request error: {re}"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"

            # Cleanup partial after failure so next attempt starts fresh
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

            # If we've exhausted retries, record failure and continue (don't crash whole run)
            if attempt >= DOWNLOAD_MAX_RETRIES:
                with lock:
                    counter[0] += 1
                    remaining = total_attachments - counter[0]
                    tqdm.write(f" Remaining: {remaining} | [Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] | Failed to download {filename} from {url}: {last_err}")
                return

            # Backoff with a little jitter
            backoff = DOWNLOAD_BACKOFF_FACTOR * (2 ** (attempt - 1))
            jitter = backoff * (0.1 * (random.random() - 0.5))
            time.sleep(max(0.1, backoff + jitter))

def download_attachments_concurrently(messages, media_dir):
    """
    Download all attachments from messages concurrently with:
      - a global tqdm showing “files left”,
      - each attachment’s own tqdm for byte‐progress.
    (All other logic is unchanged—only how threads are submitted and tracked differs.)
    """
    ensure_dir(media_dir)
    attachments = [
        (attachment, message["id"])
        for message in messages
        for attachment in message.get("attachments", [])
    ]
    total_attachments = len(attachments)

    if total_attachments == 0:
        tqdm.write("No new attachments to download.")
        return

    tqdm.write(f"Starting download of {total_attachments} attachments.")
    counter = [0]
    lock = threading.Lock()

    futures = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        for attachment, message_id in attachments:
            futures.append(
                executor.submit(
                    download_attachment,
                    attachment,
                    media_dir,
                    message_id,
                    counter,
                    total_attachments,
                    lock
                )
            )

        # # Wrap as_completed in tqdm to show how many remain:
        # for _ in tqdm(
            # as_completed(futures),
            # total=total_attachments,
            # desc="Attachments",
            # unit="file"
        # ):
            # pass  # each future already tqdm.writes its own messages

@rate_limiter(max_calls=2, per_seconds=1)
def download_embed(embed_url, file_path, message_timestamp, counter, total_embeds, lock):
    """
    Robust embed downloader using same protections as attachments.
    """
    ensure_dir(os.path.dirname(file_path) or ".")
    _check_url = _normalize_url(embed_url)
    with _LINK_INDEX_LOCK:
        if _check_url in _LINK_INDEX_SET:
            with lock:
                counter[0] += 1
                remaining = total_embeds - counter[0]
                tqdm.write(f"Skipping already-downloaded embed : {embed_url} | Remaining: {remaining}")
            return

    temp_path = file_path + ".part"

    wait_for_wifi_connection()
    check_storage(_STORAGE_PATH, _STORAGE_THRESHOLD_GB, _STORAGE_WAIT_SECONDS)
    #pause_during_time_range("07:00", "11:00")

    with DOWNLOAD_SEMAPHORE:
        session = _DOWNLOAD_SESSION
        attempt = 0
        last_err = None
        while attempt < DOWNLOAD_MAX_RETRIES:
            attempt += 1
            try:
                with session.get(embed_url, stream=True, timeout=(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get("Content-Length", 0) or 0)
                    bytes_written = 0
                    last_progress = time.time()

                    # Remove previous partial if present
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass

                    with open(temp_path, "wb") as fh, tqdm(
                        total=total_size if total_size > 0 else None,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=os.path.basename(file_path)[:20],
                        leave=False
                    ) as pbar:
                        for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                            if chunk:
                                fh.write(chunk)
                                bytes_written += len(chunk)
                                last_progress = time.time()
                                pbar.update(len(chunk))

                            if time.time() - last_progress > DOWNLOAD_STALL_TIMEOUT:
                                raise IOError(f"Stalled for >{DOWNLOAD_STALL_TIMEOUT}s")

                    os.replace(temp_path, file_path)

                    with lock:
                        counter[0] += 1
                        remaining = total_embeds - counter[0]
                        tqdm.write(f" Remaining: {remaining} | [Current Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] | Successfully downloaded embed: {file_path}")
                        _record_link(_check_url)
                    return

            except requests.exceptions.RequestException as re:
                last_err = f"request error: {re}"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"

            # Cleanup & retry/backoff
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

            if attempt >= DOWNLOAD_MAX_RETRIES:
                with lock:
                    counter[0] += 1
                    remaining = total_embeds - counter[0]
                    tqdm.write(f" Remaining: {remaining} | [Current Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] | Failed to download embed {embed_url}: {last_err}")
                return

            backoff = DOWNLOAD_BACKOFF_FACTOR * (2 ** (attempt - 1))
            jitter = backoff * (0.1 * (random.random() - 0.5))
            time.sleep(max(0.1, backoff + jitter))

def download_embeds(messages, embed_dir):
    """
    Download all embeds from messages concurrently with:
      - a global tqdm showing “files left”,
      - each embed’s own tqdm for byte‐progress.
    (All other logic is unchanged—only the submission/tracking differs.)
    """
    ensure_dir(embed_dir)
    embeds = []
    for msg in messages:
        for embed in msg.get("embeds", []):
            video_url = embed.get("video", {}).get("url")
            image_url = embed.get("image", {}).get("url")
            thumbnail_url = embed.get("thumbnail", {}).get("url")

            urls_to_download = []
            if video_url:
                urls_to_download.append(video_url)
            if image_url:
                urls_to_download.append(image_url)
            if not video_url and not image_url and thumbnail_url:
                urls_to_download.append(thumbnail_url)

            for url in urls_to_download:
                embeds.append((url, msg["id"], msg["timestamp"]))

    total_embeds = len(embeds)

    if total_embeds == 0:
        tqdm.write("No new embeds to download.")
        return

    tqdm.write(f"Starting download of {total_embeds} embeds.")
    counter = [0]
    lock = threading.Lock()

    futures = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        for embed_url, message_id, timestamp in embeds:
            base_name = embed_url.split("/")[-1].split("?")[0]
            # Use hash of URL for unique identifier (deterministic), fallback to random if no URL
            embed_id = abs(hash(embed_url)) % 100000 if embed_url else random.randint(1, 100)
            file_path = os.path.join(embed_dir, f"{message_id}_{embed_id}_{base_name}")


            futures.append(
                executor.submit(
                    download_embed,
                    embed_url,
                    file_path,
                    timestamp,
                    counter,
                    total_embeds,
                    lock
                )
            )

        # for _ in tqdm(
            # #as_completed(futures),
            # total=total_embeds,
            # desc="Embeds",
            # unit="file"
        # ):
            # pass  # per‐embed tqdm.writes happen inside download_embed()

# --- Attach skip predicates to avoid waiting in rate_limiter when the item is already recorded ---

def _skip_if_attachment_already_downloaded(attachment, media_dir, message_id, counter, total_attachments, lock):
    try:
        url = attachment.get("url") if isinstance(attachment, dict) else None
        if not url:
            return False
        check_url = _normalize_url(url)
        with _LINK_INDEX_LOCK:
            return check_url in _LINK_INDEX_SET
    except Exception:
        return False

# For embeds: first arg is embed_url (string)
def _skip_if_embed_already_downloaded(embed_url, file_path, message_timestamp, counter, total_embeds, lock):
    try:
        if not isinstance(embed_url, str):
            return False
        check_url = _normalize_url(embed_url)
        with _LINK_INDEX_LOCK:
            return check_url in _LINK_INDEX_SET
    except Exception:
        return False

# Attach predicates to the decorated wrappers (download_attachment and download_embed are decorated with @rate_limiter)
try:
    download_attachment.set_skip_pred(_skip_if_attachment_already_downloaded)
except Exception:
    # If download_attachment wasn't defined yet for some reason, ignore silently
    pass

try:
    download_embed.set_skip_pred(_skip_if_embed_already_downloaded)
except Exception:
    pass

def fetch_channel_metadata(token, channel_id):
	"""
	Fetch metadata for a channel, including DMs.
	Args:
	- token: Discord API token.
	- channel_id: Channel ID to fetch.
	Returns:
	- A dictionary with metadata, including type (DM, group, or text channel).
	"""
	url = f"https://discord.com/api/v9/channels/{channel_id}"
	headers = {"Authorization": token}
	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
		channel_metadata = response.json()
		return channel_metadata
	except requests.exceptions.RequestException as e:
		tqdm.write(f"Failed to fetch metadata for channel {channel_id}: {e}")
		return {}

def fetch_category_channels(token, guild_id, category_id):
	"""
	Fetch all text channels in a specified category.
	Args:
	- token: Discord API token.
	- guild_id: ID of the guild (server) containing the category.
	- category_id: ID of the category channel.
	Returns:
	- List of child channel metadata for the category.
	"""
	headers = {"Authorization": token}
	url = f"https://discord.com/api/v9/guilds/{guild_id}/channels"
	try:
		all_channels = api_request(url, headers)

		# Filter channels that belong to the given category ID
		child_channels = [
			channel for channel in all_channels
			if channel.get("parent_id") == category_id and channel.get("type") == 0  # Type 0 is for text channels
		]

		if not child_channels:
			tqdm.write(f"No text channels found in category {category_id}.")
		return child_channels
	except Exception as e:
		tqdm.write(f"Failed to fetch child channels for category {category_id}: {e}")
		return []

def fetch_thread_channels(token, channel_id):
	"""
	Fetch all active and archived threads in a specified thread (forum/announcement) channel.
	Returns a list of thread-channel metadata dicts (each has its own 'id', 'name', etc.).
	"""
	headers = {"Authorization": token}
	urls = {
		# ~ "active":   f"https://discord.com/api/v9/channels/{channel_id}/threads/active",
		"public":   f"https://discord.com/api/v9/channels/{channel_id}/threads/archived/public",
		"private":  f"https://discord.com/api/v9/channels/{channel_id}/threads/archived/private",
	}

	all_threads = []
	for key, url in urls.items():
		try:
			data = api_request(url, headers)
			threads = data.get("threads", [])
			if threads:
				tqdm.write(f"Fetched {len(threads)} {key} threads from channel {channel_id}")
				all_threads.extend(threads)
		except Exception as e:
			tqdm.write(f"Could not fetch {key} threads for {channel_id}: {e}")
	return all_threads


def fetch_server_channels(token, server_id):
	"""
	Fetch all channels in a server (guild).
	Args:
	- token: Discord API token.
	- server_id: ID of the server (guild).
	Returns:
	- List of accessible channel metadata for the server.
	"""
	url = f"https://discord.com/api/v9/guilds/{server_id}/channels"
	headers = {"Authorization": token}
	try:
		all_channels = api_request(url, headers)

		# Filter out channels that return "403 Missing Access"
		accessible_channels = []
		for channel in all_channels:
			try:
				if channel["type"] in [0, 4]:  # Include text channels and categories
					accessible_channels.append(channel)
			except Exception as e:
				tqdm.write(f"Skipping channel {channel.get('id', 'unknown')} due to access issues: {e}")
		return accessible_channels
	except requests.exceptions.HTTPError as e:
		if e.response.status_code == 404:
			# Suppress 404 errors for invalid server IDs
			#tqdm.write(f"ID {server_id} is not a valid server ID (404 Not Found). Treating it as a channel/category.")
			return None
		else:
			# Re-raise other HTTP errors
			raise
	except Exception as e:
		tqdm.write(f"Unexpected error while fetching channels for server {server_id}: {e}")
		return None


def process_server_and_channel_names(token, server_id, channel_id, output_dir, existing_names):
	"""
	Sanitize and resolve conflicts for server and channel names.
	Args:
	- token: Discord API token.
	- server_id: Server ID.
	- channel_id: Channel ID.
	- existing_names: A set of already-used sanitized names.

	Returns:
	- A tuple of (sanitized_server_name, sanitized_channel_name).
	"""
	server_name = fetch_server_name(token, server_id, output_dir)
	sanitized_server_name = sanitize_name_with_conflicts(server_name, existing_names)

	server_dir = os.path.join(output_dir, server_name)
	ensure_dir(server_dir)

	channel_name = fetch_channel_name(token, channel_id, server_dir, output_dir)

	# Sanitize and resolve conflicts
	sanitized_channel_name = sanitize_name_with_conflicts(channel_name, existing_names)

	return sanitized_server_name, sanitized_channel_name

#============================================================================================ Main (CLI inputs and running the rest of the script)

def main():
	parser = argparse.ArgumentParser(description="Fetch Discord messages and media, sanitize directories, or scan for duplicates.")
	parser.add_argument("--token", help="Discord API token. Required for downloading messages and media.")
	parser.add_argument("--channels", "--channel", dest="channels", help="Channel, category, or server IDs (comma-separated or file path).")
	parser.add_argument("--blacklist", help="Channel or category IDs to skip (comma-separated or file path).")
	parser.add_argument("--output", default="output", help="Output directory for saved files.")
	parser.add_argument("--save-raw", action="store_true", help="Save raw message data to JSON.")
	parser.add_argument("--category", action="store_true", help="Treat provided IDs as category IDs.")
	parser.add_argument("--storage-path", default=None, help="Path to check for free disk space (default: auto-detect from output directory).")
	parser.add_argument("--storage-threshold", type=float, default=5.0, help="Minimum free disk space in GB before pausing downloads (default: 5).")
	parser.add_argument("--storage-wait", type=int, default=600, help="Seconds to wait when disk space is below threshold (default: 600).")

	args = parser.parse_args()

	# Ensure token is provided for download operations
	if not args.token:
		tqdm.write("Error: --token is required for downloading messages and media.")
		return

	# Load blacklist if provided
	blacklist = set()
	if args.blacklist:
		if os.path.isfile(args.blacklist):
			with open(args.blacklist, "r", encoding="utf-8") as file:
				blacklist = {line.strip() for line in file if line.strip()}
		else:
			blacklist = set(args.blacklist.split(","))

	# Determine input list (channels/categories/servers)
	if args.channels:
		if os.path.isfile(args.channels):
			with open(args.channels, "r", encoding="utf-8") as file:
				entries = [line.strip() for line in file if line.strip()]
		else:
			entries = args.channels.split(",")
	else:
		tqdm.write("Error: --channels is required")
		return

	ensure_dir(args.output)

	# Configure storage checks from CLI args
	global _STORAGE_PATH, _STORAGE_THRESHOLD_GB, _STORAGE_WAIT_SECONDS
	_STORAGE_THRESHOLD_GB = args.storage_threshold
	_STORAGE_WAIT_SECONDS = args.storage_wait
	if args.storage_path:
		_STORAGE_PATH = args.storage_path
	else:
		_STORAGE_PATH = os.path.realpath(args.output)

	# Platform detection logging
	tqdm.write(f"Platform: {platform.system()} {platform.release()} ({platform.machine()})")
	tqdm.write(f"Python: {platform.python_version()}")
	tqdm.write(f"Storage check path: {_STORAGE_PATH} (threshold: {_STORAGE_THRESHOLD_GB} GB, wait: {_STORAGE_WAIT_SECONDS}s)")

	# Track sanitized server names to avoid creating multiple folders
	server_name_cache = {}

	for entry in entries:
		if entry in blacklist:
			tqdm.write(f"Skipping blacklisted item: {entry}")
			continue

		# # Attempt to treat the entry as a server ID
		# all_channels = fetch_server_channels(args.token, entry)
		# if all_channels:
			# tqdm.write(f"Processing server: {entry}")

			# for channel_metadata in all_channels:
				# if channel_metadata["id"] in blacklist:
					# tqdm.write(f"Skipping blacklisted channel: {channel_metadata.get('name', 'Unknown')} ({channel_metadata['id']})")
					# continue
				# if channel_metadata["type"] == 0:  # Text channel
					# process_channel(args, channel_metadata)
				# elif channel_metadata["type"] == 4:  # Category
					# guild_id = channel_metadata["guild_id"]
					# child_channels = fetch_category_channels(args.token, guild_id, channel_metadata["id"])
					# for child_channel in child_channels:
						# if child_channel["id"] in blacklist:
							# tqdm.write(f"Skipping blacklisted channel: {child_channel.get('name', 'Unknown')} ({child_channel['id']})")
							# continue
						# process_channel(args, child_channel)
			# continue

		# If not a server ID, handle as channel/category ID
		channel_metadata = fetch_channel_metadata(args.token, entry)
		if not channel_metadata:
			tqdm.write(f"Failed to fetch metadata for channel {entry}. Skipping.")
			continue

		if channel_metadata["id"] in blacklist:
			tqdm.write(f"Skipping blacklisted item: {channel_metadata.get('name', 'Unknown')} ({channel_metadata['id']})")
			continue

		if channel_metadata["type"] == 15:  # 15 = GUILD_FORUM
			tqdm.write(f"Processing forum channel: {channel_metadata.get('name', f'Forum-{entry}')}")

			# Get server (guild) name
			server_id = channel_metadata.get("guild_id")
			if not server_id:
				tqdm.write(f"Forum {entry} does not belong to a guild. Skipping.")
				continue

			sanitized_server_name, sanitized_forum_name = process_server_and_channel_names(args.token, server_id, channel_metadata["id"], args.output, set())

			# Create Forum folder inside Server folder
			server_dir = os.path.join(args.output, sanitized_server_name)
			forum_dir = os.path.join(server_dir, sanitized_forum_name)
			ensure_dir(forum_dir)

			# Fetch threads inside the forum
			thread_list = fetch_thread_channels(args.token, channel_metadata["id"])

			for thread_meta in thread_list:
				tid = thread_meta["id"]
				if tid in blacklist:
					tqdm.write(f"Skipping blacklisted thread: {thread_meta.get('name', tid)} ({tid})")
					continue
				# Pass forum_dir as base_output_dir to process_channel
				process_channel(args, thread_meta, base_output_dir=forum_dir)
			continue

		if channel_metadata["type"] == 4:  # Category
			guild_id = channel_metadata.get("guild_id")
			if not guild_id:
				tqdm.write(f"Category {entry} does not belong to a guild. Skipping.")
				continue

			tqdm.write(f"Processing category: {channel_metadata.get('name', f'Category-{entry}')}")
			child_channels = fetch_category_channels(args.token, guild_id, entry)
			for child_channel in child_channels:
				if child_channel["id"] in blacklist:
					tqdm.write(f"Skipping blacklisted channel: {child_channel.get('name', 'Unknown')} ({child_channel['id']})")
					continue
				process_channel(args, child_channel)
			continue

		# Handle individual channels (DMs, Group DMs, or regular text channels)
		process_channel(args, channel_metadata)

def process_channel(args, channel_metadata, base_output_dir=None):
	"""
	Process a single channel for message and media downloads.
	Args:
	- args: Parsed command-line arguments.
	- channel_metadata: Metadata of the channel to process.
	- base_output_dir: Override the server directory (used for organizing threads).
	"""
	try:
		if channel_metadata["type"] in [1, 3]:  # DM or Group DM
			if channel_metadata["type"] == 1:  # Direct Message
				folder_name = get_dm_folder_name(channel_metadata, args.output)
			elif channel_metadata["type"] == 3:  # Group DM
				folder_name = sanitize_filename(channel_metadata.get("name") or f"GroupDM-{channel_metadata['id']}")

			channel_dir = os.path.join(args.output, folder_name)
			ensure_dir(channel_dir)

			tqdm.write(f"Processing DM or Group DM channel '{folder_name}'")
			messages = fetch_messages(args.token, channel_metadata["id"], channel_dir, save_raw=args.save_raw)
			download_attachments_concurrently(messages, os.path.join(channel_dir, "media"))
			download_embeds(messages, os.path.join(channel_dir, "embeds"))
			return

		# Process regular text channels
		server_id = channel_metadata.get("guild_id")
		if not server_id:
			tqdm.write(f"Channel {channel_metadata['id']} does not belong to a guild. Skipping.")
			return

		sanitized_server_name, sanitized_channel_name = process_server_and_channel_names(args.token, server_id, channel_metadata["id"], args.output, set())

		# If no custom base_output_dir was given, use the default server folder
		server_dir = base_output_dir if base_output_dir else os.path.join(args.output, sanitized_server_name)
		channel_dir = os.path.join(server_dir, sanitized_channel_name)
		ensure_dir(channel_dir)

		tqdm.write(f"Processing channel '{sanitized_channel_name}' in server '{sanitized_server_name}'")
		messages = fetch_messages(args.token, channel_metadata["id"], channel_dir, save_raw=args.save_raw)
		download_attachments_concurrently(messages, os.path.join(channel_dir, "media"))
		download_embeds(messages, os.path.join(channel_dir, "embeds"))
	except Exception as e:
		tqdm.write(f"Error processing channel {channel_metadata.get('id', 'unknown')}: {e}")


if __name__ == "__main__":
	main()

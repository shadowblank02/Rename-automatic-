import os
import re
import time
import shutil
import asyncio
import logging
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaDocument, InputMediaVideo, InputMediaAudio
from pyrogram.errors import FloodWait

# NOTE: Ensure these imports are correct for your project structure
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram
from helper.database import Botskingdom
from config import Config
from functools import wraps

ADMIN_URL = Config.ADMIN_URL


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Enhanced Semaphores for better concurrency ---
download_semaphore = asyncio.Semaphore(3)   # Allow 3 concurrent downloads
upload_semaphore = asyncio.Semaphore(3)     # Limit 3 concurrent uploads
ffmpeg_semaphore = asyncio.Semaphore(3)     # Limit FFmpeg processes
processing_semaphore = asyncio.Semaphore(3) # Overall processing limit

# Thread pool for CPU-intensive operations
thread_pool = ThreadPoolExecutor(max_workers=4)

# ========== Decorators ==========

def check_ban(func):
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        user = await Botskingdom.col.find_one({"_id": user_id})
        if user and user.get("ban_status", {}).get("is_banned", False):
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Cᴏɴᴛᴀᴄᴛ ʜᴇʀᴇ...!!", url=ADMIN_URL)]]
            )
            return await message.reply_text(
                "Wᴛғ ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ ғʀᴏᴍ ᴜsɪɴɢ ᴍᴇ ʙʏ ᴏᴜʀ ᴀᴅᴍɪɴ/ᴏᴡɴᴇʀ . Iғ ʏᴏᴜ ᴛʜɪɴᴋs ɪᴛ's ᴍɪsᴛᴀᴋᴇ ᴄʟɪᴄᴋ ᴏɴ **ᴄᴏɴᴛᴀᴄᴛ ʜᴇʀᴇ...!!**",
                reply_markup=keyboard
            )
        return await func(client, message, *args, **kwargs)
    return wrapper


# --- REVISED extract_episode_number ---
def extract_episode_number(filename):
    """
    Enhanced episode extraction with better pattern matching and validation.
    Improved negative lookaheads to prevent various quality numbers (like 480p, 720p, 1080p, 4K)
    and years from being misinterpreted as episode numbers.
    """
    if not filename:
        return None

    logger.debug(f"Extracting episode from: '{filename}'")

    # Define common quality and year indicators to exclude if they appear near a number.
    quality_and_year_indicators = [
        r'\d{2,4}[pP]',    # e.g., 480p, 720p, 1080p, 2160p (case-insensitive 'p')
        r'\dK',            # e.g., 4K, 2K
        r'HD(?:RIP)?',     # e.g., HD, HDRip
        r'WEB(?:-)?DL',    # e.g., WEB-DL, WEBDL
        r'BLURAY',         # e.g., BLURAY
        r'X264',           # e.g., X264
        r'X265',           # e.g., X265
        r'HEVC',           # e.g., HEVC
        r'FHD',            # e.g., FHD (Full HD)
        r'UHD',            # e.g., UHD (Ultra HD)
        r'HDR',            # e.g., HDR
        r'H\.264', r'H\.265', # common codec spellings
        r'(?:19|20)\d{2}', # Years like 19XX or 20XX
        r'Multi(?:audio)?', # Multi audio, as it can be near numbers
        r'Dual(?:audio)?', # Dual audio, as it can be near numbers
    ]
    # Create a single regex for negative lookaheads, allowing for various separators
    quality_pattern_for_exclusion = r'(?:' + '|'.join([f'(?:[\s._-]*{ind})' for ind in quality_and_year_indicators]) + r')'

    patterns = [
        # Pattern 1: S##E## format (most reliable)
        re.compile(r'S\d+[.-_]?E(\d+)', re.IGNORECASE),
        # Pattern 2: Episode XX, EP XX formats
        re.compile(r'(?:Episode|EP)[\s._-]*(\d+)', re.IGNORECASE),
        # Pattern 3: E## standalone (with word boundaries)
        re.compile(r'\bE(\d+)\b', re.IGNORECASE),
        # Pattern 4: [E##] or (E##) format
        re.compile(r'[\[\(]E(\d+)[\]\)]', re.IGNORECASE),
        # Pattern 5: X of Y format
        re.compile(r'\b(\d+)\s*of\s*\d+\b', re.IGNORECASE),

        # Pattern 6: General number pattern with strong negative lookahead.
        # This is the most crucial part to avoid misidentifying quality/year numbers.
        re.compile(
            r'(?:^|[^0-9A-Z])'      # Start of string or non-alphanumeric character before the number
            r'(\d{1,4})'         # Capture 1 to 4 digits (potential episode number)
            r'(?:[^0-9A-Z]|$)'      # End of string or non-alphanumeric character after the number
            r'(?!' + quality_pattern_for_exclusion + r')' # IMPORTANT: Negative lookahead for quality/year patterns
            , re.IGNORECASE
        ),
    ]

    for i, pattern in enumerate(patterns):
        matches = pattern.findall(filename)
        if matches:
            for match in matches:
                try:
                    # If the pattern has a non-capturing group at the start, match could be a tuple.
                    if isinstance(match, tuple):
                        episode_str = match[0] # Get the first (and only) captured group
                    else:
                        episode_str = match

                    episode_num = int(episode_str)

                    # Validate episode number (should be reasonable)
                    if 1 <= episode_num <= 9999:
                        # Final check to prevent very common quality numbers from being picked if the regex missed them
                        if episode_num in [360, 480, 720, 1080, 1440, 2160, 2020, 2021, 2022, 2023, 2024, 2025]: # Added common years
                            # If the filename contains this number IMMEDIATELY followed by 'p' or 'K'
                            if re.search(r'\b' + str(episode_num) + r'(?:p|K|HD|WEB|BLURAY|X264|X265|HEVC|Multi|Dual)\b', filename, re.IGNORECASE) or \
                               re.search(r'\b(?:19|20)\d{2}\b', filename, re.IGNORECASE) and len(str(episode_num)) == 4: # If it's a 4-digit number and looks like a year
                                logger.debug(f"Skipping {episode_num} as it is a common quality/year number.")
                                continue # Skip this match, it's a quality/year number

                        logger.debug(f"Episode Pattern {i+1} found episode: {episode_num}")
                        return episode_num
                except ValueError:
                    continue

    logger.debug(f"No episode number found in: '{filename}'")
    return None

# --- MODIFIED: extract_season_number (added negative lookahead) ---
def extract_season_number(filename):
    """
    Enhanced season extraction with better pattern matching and validation.
    Added negative lookahead to prevent quality numbers (like 480p) from being misinterpreted.
    """
    if not filename:
        return None

    logger.debug(f"Extracting season from: '{filename}'")

    # Define common quality and year indicators (same as for episodes)
    quality_and_year_indicators = [
        r'\d{2,4}[pP]',    # e.g., 480p, 720p, 1080p, 2160p (case-insensitive 'p')
        r'\dK',            # e.g., 4K, 2K
        r'HD(?:RIP)?',     # e.g., HD, HDRip
        r'WEB(?:-)?DL',    # e.g., WEB-DL, WEBDL
        r'BLURAY',         # e.g., BLURAY
        r'X264',           # e.g., X264
        r'X265',           # e.g., X265
        r'HEVC',           # e.g., HEVC
        r'FHD',            # e.g., FHD (Full HD)
        r'UHD',            # e.g., UHD (Ultra HD)
        r'HDR',            # e.g., HDR
        r'H\.264', r'H\.265', # common codec spellings
        r'(?:19|20)\d{2}', # Years like 19XX or 20XX
        r'Multi(?:audio)?', # Multi audio, as it can be near numbers
        r'Dual(?:audio)?', # Dual audio, as it can be near numbers
    ]
    # Create a single regex for negative lookaheads
    quality_pattern_for_exclusion = r'(?:' + '|'.join([f'(?:[\s._-]*{ind})' for ind in quality_and_year_indicators]) + r')'


    patterns = [
        # Pattern 1: S##E## format (extract season part) - Most reliable
        re.compile(r'S(\d+)[._-]?E\d+', re.IGNORECASE),

        # Pattern 2: Season XX, SEASON XX formats (more explicit)
        re.compile(r'(?:Season|SEASON|season)[\s._-]*(\d+)', re.IGNORECASE),

        # Pattern 3: S## standalone (with word boundaries) - ADDED NEGATIVE LOOKAHEAD
        re.compile(r'\bS(\d+)\b(?!E\d|' + quality_pattern_for_exclusion + r')', re.IGNORECASE),

        # Pattern 4: [S##] or (S##) format
        re.compile(r'[\[\(]S(\d+)[\]\)]', re.IGNORECASE),

        # Pattern 5: Season with separators (more flexible)
        re.compile(r'[._-]S(\d+)(?:[._-]|$)', re.IGNORECASE),

        # Pattern 6: Season followed by number (case insensitive)
        re.compile(r'(?:season|SEASON|Season)[\s._-]*(\d+)', re.IGNORECASE),

        # Pattern 7: More flexible season patterns
        re.compile(r'(?:^|[\s._-])(?:season|SEASON|Season)[\s._-]*(\d+)(?:[\s._-]|$)', re.IGNORECASE),

        # Pattern 8: Season in brackets or parentheses
        re.compile(r'[\[\(](?:season|SEASON|Season)[\s._-]*(\d+)[\]\)]', re.IGNORECASE),

        # Pattern 9: Season with various separators
        re.compile(r'(?:season|SEASON|Season)[._\s-]+(\d+)', re.IGNORECASE),

        # Pattern 10: Season at beginning or end
        re.compile(r'(?:^season|season$)[\s._-]*(\d+)', re.IGNORECASE),
    ]

    for i, pattern in enumerate(patterns):
        match = pattern.search(filename)
        if match:
            try:
                season_num = int(match.group(1))
                if 1 <= season_num <= 99:
                    logger.debug(f"Season Pattern {i+1} found season: {season_num}")
                    return season_num
            except ValueError:
                continue

    logger.debug(f"No season number found in: '{filename}'")
    return None

def extract_audio_info(filename):
    """Extract audio information from filename, including languages and 'dual'/'multi'."""
    audio_keywords = {
        'Hindi': re.compile(r'Hindi', re.IGNORECASE),
        'English': re.compile(r'English', re.IGNORECASE),
        'Multi': re.compile(r'Multi(?:audio)?', re.IGNORECASE),
        'Telugu': re.compile(r'Telugu', re.IGNORECASE),
        'Tamil': re.compile(r'Tamil', re.IGNORECASE),
        'Dual': re.compile(r'Dual(?:audio)?', re.IGNORECASE),
        'Dual_Enhanced': re.compile(r'(?:DUAL(?:[\s._-]?AUDIO)?|\[DUAL\])', re.IGNORECASE),
        'AAC': re.compile(r'AAC', re.IGNORECASE),
        'AC3': re.compile(r'AC3', re.IGNORECASE),
        'DTS': re.compile(r'DTS', re.IGNORECASE),
        'MP3': re.compile(r'MP3', re.IGNORECASE),
        '5.1': re.compile(r'5\.1', re.IGNORECASE),
        '2.0': re.compile(r'2\.0', re.IGNORECASE),
    }

    detected_audio = []

    if re.search(r'\bMulti(?:audio)?\b', filename, re.IGNORECASE):
        detected_audio.append("Multi")
    if re.search(r'\bDual(?:audio)?\b', filename, re.IGNORECASE):
        detected_audio.append("Dual")

    priority_keywords = ['Hindi', 'English', 'Telugu', 'Tamil']
    for keyword in priority_keywords:
        if audio_keywords[keyword].search(filename):
            if keyword not in detected_audio:
                detected_audio.append(keyword)

    for keyword in ['AAC', 'AC3', 'DTS', 'MP3', '5.1', '2.0']:
        if audio_keywords[keyword].search(filename):
            if keyword not in detected_audio:
                detected_audio.append(keyword)

    detected_audio = list(dict.fromkeys(detected_audio))

    if detected_audio:
        return ' '.join(detected_audio)

    return None

def extract_quality(filename):
    """Extract video quality from filename."""
    patterns = [
        re.compile(r'\b(4K|2K|2160p|1440p|1080p|720p|480p|360p)\b', re.IGNORECASE),
        re.compile(r'\b(HD(?:RIP)?|WEB(?:-)?DL|BLURAY)\b', re.IGNORECASE),
        re.compile(r'\b(X264|X265|HEVC)\b', re.IGNORECASE),
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            found_quality = match.group(1)
            if found_quality.lower() in ["4k", "2k", "hdrip", "web-dl", "bluray"]:
                return found_quality.upper() if found_quality.upper() in ["4K", "2K"] else found_quality.capitalize()
            return found_quality

    return None

def extract_title(filename):
    """
    Extracts the title of the movie or show by removing common patterns
    like quality, audio, season/episode numbers, and years.
    """
    if not filename:
        return "Untitled"

    file_name_no_ext, _ = os.path.splitext(filename)
    
    temp_name = file_name_no_ext.replace('.', ' ').replace('_', ' ').replace('-', ' ').strip()

    patterns_to_remove = [
        re.compile(r'\b(?:S\d+[._-]?E\d+|Season[\s._-]?\d+|E\d+|EP\d+)\b', re.IGNORECASE),
        re.compile(r'\b(?:2160p|1440p|1080p|720p|480p|360p|4K|2K)\b', re.IGNORECASE),
        re.compile(r'\b(?:HD(?:RIP)?|WEB(?:-)?DL|BLURAY|FHD|UHD)\b', re.IGNORECASE),
        re.compile(r'\b(?:X264|X265|HEVC|H\.264|H\.265)\b', re.IGNORECASE),
        re.compile(r'\b(?:AAC|AC3|DTS|MP3|5\.1|2\.0)\b', re.IGNORECASE),
        re.compile(r'\b(?:Hindi|English|Telugu|Tamil|Multi(?:audio)?|Dual(?:audio)?)\b', re.IGNORECASE),
        re.compile(r'\b(?:19|20)\d{2}\b', re.IGNORECASE),
        re.compile(r'\b(?:REPACK|PROPER|LIMITED|EXTENDED|DIRECTORS\.CUT)\b', re.IGNORECASE),
        re.compile(r'\[.*?\]|\(.*?\)|\{.*?\}', re.IGNORECASE),
    ]

    cleaned_name = temp_name
    for pattern in patterns_to_remove:
        cleaned_name = pattern.sub('', cleaned_name).strip()

    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()

    if not cleaned_name:
        parts = re.split(r'[._-]', file_name_no_ext, 1)
        if parts:
            cleaned_name = parts[0].replace('.', ' ').replace('_', ' ').strip()
        else:
            return "Untitled"
    
    return cleaned_name.title()


# --- Modified filename generation to NOT add UUID to filename ---
def generate_unique_paths(renamed_file_name):
    """
    Generate file paths.
    This version does NOT append a unique ID to the filename itself.
    """
    # A temporary unique ID is still good for the folder to avoid conflicts during processing
    unique_id = str(uuid.uuid4())
    temp_dir = os.path.join(Config.DOWNLOAD_DIR, unique_id)
    os.makedirs(temp_dir, exist_ok=True)
    
    renamed_file_path = os.path.join(temp_dir, renamed_file_name)
    metadata_file_path = os.path.join(temp_dir, f"metadata_{renamed_file_name}")

    return renamed_file_path, metadata_file_path, temp_dir


async def cleanup_files_async(*paths):
    """Safely remove files and directories asynchronously."""
    for path in paths:
        try:
            if path and os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                logger.info(f"Cleaned up path: {path}")
        except Exception as e:
            logger.error(f"Error cleaning up {path}: {e}")

async def process_thumb_async(ph_path):
    """Process thumbnail in thread pool to avoid blocking"""
    def _resize_thumb(path):
        with Image.open(path) as img:
            img = img.convert("RGB").resize((320, 320))
            img.save(path, "JPEG")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(thread_pool, _resize_thumb, ph_path)
    return ph_path

async def run_ffmpeg_async(metadata_command):
    """Run FFmpeg in thread pool with semaphore control"""
    async with ffmpeg_semaphore:
        def _run_ffmpeg():
            import subprocess
            result = subprocess.run(
                metadata_command,
                capture_output=True,
                text=True
            )
            return result.returncode, result.stdout, result.stderr

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(thread_pool, _run_ffmpeg)

async def concurrent_download(client, message, renamed_file_path, progress_msg):
    """Handle concurrent downloading with semaphore"""
    async with download_semaphore:
        try:
            path = await client.download_media(
                message,
                file_name=renamed_file_path,
                progress=progress_for_pyrogram,
                progress_args=("Dᴏᴡɴʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ....!!", progress_msg, time.time()),
            )
            return path
        except Exception as e:
            raise Exception(f"Download Error: {e}")

async def concurrent_upload(client, message, path, media_type, caption, ph_path, progress_msg):
    """Handle concurrent uploading with semaphore"""
    async with upload_semaphore:
        try:
            if media_type == "document":
                await client.send_document(
                    chat_id=message.chat.id,
                    document=path,
                    caption=caption,
                    thumb=ph_path,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅɪɴɢ ʙᴀᴄᴋ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "video":
                await client.send_video(
                    chat_id=message.chat.id,
                    video=path,
                    caption=caption,
                    thumb=ph_path,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅɪɴɢ ʙᴀᴄᴋ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    chat_id=message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅɪɴɢ ʙᴀᴄᴋ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            await progress_msg.delete()
        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping for {e.value}s")
            await asyncio.sleep(e.value)
            await concurrent_upload(client, message, path, media_type, caption, ph_path, progress_msg)
        except Exception as e:
            raise Exception(f"Upload Error: {e}")


# --- NEW FUNCTION: auto_rename_file_concurrent ---
async def auto_rename_file_concurrent(client, message, file_info):
    """
    Handles the entire file renaming and uploading process for a single file concurrently.
    This function was missing and now orchestrates the whole workflow.
    """
    user_id = message.from_user.id
    file_id = file_info["file_id"]

    # Use a semaphore to limit the total number of concurrent processing tasks
    async with processing_semaphore:
        # Check if an operation is already in progress for this file
        if file_id in renaming_operations:
            return
        renaming_operations[file_id] = datetime.now()

        # Initialize variables for cleanup
        download_path, metadata_path, ph_path, temp_dir = None, None, None, None

        try:
            # --- Get user settings and file info ---
            user_config = await Botskingdom.col.find_one({"_id": user_id})
            if not user_config or not user_config.get("rename_format"):
                return await message.reply_text("Pʟᴇᴀsᴇ sᴇᴛ ᴀ ʀᴇɴᴀᴍᴇ ғᴏʀᴍᴀᴛ ᴜsɪɴɢ /autorename")

            format_template = user_config["rename_format"]
            
            file_name = file_info["file_name"]
            
            if message.document:
                media_type = "document"
            elif message.video:
                media_type = "video"
            elif message.audio:
                media_type = "audio"
            else:
                return await message.reply_text("Uɴsᴜᴘᴘᴏʀᴛᴇᴅ ғɪʟᴇ ᴛʏᴘᴇ")

            # --- Check for NSFW content ---
            if await check_anti_nsfw(file_name, message):
                return await message.reply_text("Nsfw ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ")

            # --- Extract metadata from filename ---
            title 

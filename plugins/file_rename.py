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
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import Botskingdom
from config import Config
from functools import wraps

ADMIN_URL = Config.ADMIN_URL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

active_sequences = {}
message_ids = {}
renaming_operations = {}

# Concurrency limits
download_semaphore = asyncio.Semaphore(3)
upload_semaphore = asyncio.Semaphore(3)
ffmpeg_semaphore = asyncio.Semaphore(3)
processing_semaphore = asyncio.Semaphore(3)

thread_pool = ThreadPoolExecutor(max_workers=4)

# ===== Decorators =====
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
                "Wᴛғ ʏᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ ғʀᴏᴍ ᴜsɪɴɢ ᴍᴇ...",
                reply_markup=keyboard
            )
        return await func(client, message, *args, **kwargs)
    return wrapper

# ===== Metadata extractors =====
def extract_episode_number(filename):
    if not filename:
        return None
    quality_and_year_indicators = [
        r'\d{2,4}[pP]', r'\dK', r'HD(?:RIP)?', r'WEB(?:-)?DL', r'BLURAY',
        r'X264', r'X265', r'HEVC', r'FHD', r'UHD', r'HDR',
        r'H\.264', r'H\.265', r'(?:19|20)\d{2}', r'Multi(?:audio)?', r'Dual(?:audio)?',
    ]
    quality_pattern_for_exclusion = r'(?:' + '|'.join([f'(?:[\\s._-]*{ind})' for ind in quality_and_year_indicators]) + r')'
    patterns = [
        re.compile(r'S\d+[.-_]?E(\d+)', re.IGNORECASE),
        re.compile(r'(?:Episode|EP)[\s._-]*(\d+)', re.IGNORECASE),
        re.compile(r'\bE(\d+)\b', re.IGNORECASE),
        re.compile(r'[\[\(]E(\d+)[\]\)]', re.IGNORECASE),
        re.compile(r'\b(\d+)\s*of\s*\d+\b', re.IGNORECASE),
        re.compile(r'(?:^|[^0-9A-Z])(\d{1,4})(?:[^0-9A-Z]|$)(?!' + quality_pattern_for_exclusion + r')', re.IGNORECASE),
    ]
    for i, pattern in enumerate(patterns):
        matches = pattern.findall(filename)
        if matches:
            for match in matches:
                try:
                    episode_str = match[0] if isinstance(match, tuple) else match
                    episode_num = int(episode_str)
                    if 1 <= episode_num <= 9999:
                        if episode_num in [360, 480, 720, 1080, 1440, 2160, 2020, 2021, 2022, 2023, 2024, 2025]:
                            if re.search(r'\b' + str(episode_num) + r'(?:p|K|HD|WEB|BLURAY|X264|X265|HEVC|Multi|Dual)\b', filename, re.IGNORECASE) or \
                               re.search(r'\b(?:19|20)\d{2}\b', filename, re.IGNORECASE):
                                continue
                        return episode_num
                except ValueError:
                    continue
    return None

def extract_season_number(filename):
    if not filename:
        return None
    quality_and_year_indicators = [
        r'\d{2,4}[pP]', r'\dK', r'HD(?:RIP)?', r'WEB(?:-)?DL', r'BLURAY',
        r'X264', r'X265', r'HEVC', r'FHD', r'UHD', r'HDR',
        r'H\.264', r'H\.265', r'(?:19|20)\d{2}', r'Multi(?:audio)?', r'Dual(?:audio)?',
    ]
    quality_pattern_for_exclusion = r'(?:' + '|'.join([f'(?:[\\s._-]*{ind})' for ind in quality_and_year_indicators]) + r')'
    patterns = [
        re.compile(r'S(\d+)[._-]?E\d+', re.IGNORECASE),
        re.compile(r'(?:Season|SEASON|season)[\s._-]*(\d+)', re.IGNORECASE),
        re.compile(r'\bS(\d+)\b(?!E\d|' + quality_pattern_for_exclusion + r')', re.IGNORECASE),
        re.compile(r'[\[\(]S(\d+)[\]\)]', re.IGNORECASE),
        re.compile(r'[._-]S(\d+)(?:[._-]|$)', re.IGNORECASE),
    ]
    for i, pattern in enumerate(patterns):
        match = pattern.search(filename)
        if match:
            try:
                season_num = int(match.group(1))
                if 1 <= season_num <= 99:
                    return season_num
            except ValueError:
                continue
    return None

def extract_audio_info(filename):
    audio_keywords = {
        'Hindi': re.compile(r'Hindi', re.IGNORECASE),
        'English': re.compile(r'English', re.IGNORECASE),
        'Multi': re.compile(r'Multi(?:audio)?', re.IGNORECASE),
        'Telugu': re.compile(r'Telugu', re.IGNORECASE),
        'Tamil': re.compile(r'Tamil', re.IGNORECASE),
        'Dual': re.compile(r'Dual(?:audio)?', re.IGNORECASE),
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
    for keyword in ['Hindi', 'English', 'Telugu', 'Tamil', 'AAC', 'AC3', 'DTS', 'MP3', '5.1', '2.0']:
        if audio_keywords[keyword].search(filename) and keyword not in detected_audio:
            detected_audio.append(keyword)
    return ' '.join(detected_audio) if detected_audio else None

def extract_quality(filename):
    patterns = [
        re.compile(r'\b(4K|2K|2160p|1440p|1080p|720p|480p|360p)\b', re.IGNORECASE),
    ]
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            found_quality = match.group(1)
            return found_quality.upper() if found_quality.lower() in ["4k", "2k"] else found_quality
    return None

def extract_title(filename):
    name_no_ext, _ = os.path.splitext(filename)
    temp_name = name_no_ext.replace('.', ' ').replace('_', ' ').replace('-', ' ')
    patterns_to_remove = [
        re.compile(r'\b(?:S\d+[._-]?E\d+|Season[\s._-]?\d+|E\d+|EP\d+)\b', re.IGNORECASE),
        re.compile(r'\b(?:2160p|1440p|1080p|720p|480p|360p|4K|2K)\b', re.IGNORECASE),
        re.compile(r'\b(?:HD(?:RIP)?|WEB(?:-)?DL|BLURAY|FHD|UHD)\b', re.IGNORECASE),
        re.compile(r'\b(?:X264|X265|HEVC|H\.264|H\.265)\b', re.IGNORECASE),
        re.compile(r'\b(?:AAC|AC3|DTS|MP3|5\.1|2\.0)\b', re.IGNORECASE),
        re.compile(r'\b(?:Hindi|English|Telugu|Tamil|Multi(?:audio)?|Dual(?:audio)?)\b', re.IGNORECASE),
        re.compile(r'\b(?:19|20)\d{2}\b', re.IGNORECASE),
        re.compile(r'\[.*?\]|\(.*?\)|\{.*?\}', re.IGNORECASE),
    ]
    cleaned_name = temp_name
    for pattern in patterns_to_remove:
        cleaned_name = pattern.sub('', cleaned_name).strip()
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
    return cleaned_name.title() if cleaned_name else "Untitled"

# ===== Utility Functions =====
def generate_unique_paths(renamed_file_name):
    unique_id = str(uuid.uuid4())
    temp_dir = os.path.join("downloads", unique_id)
    os.makedirs(temp_dir, exist_ok=True)
    renamed_file_path = os.path.join(temp_dir, renamed_file_name)
    metadata_file_path = os.path.join(temp_dir, f"metadata_{renamed_file_name}")
    return renamed_file_path, metadata_file_path, temp_dir

async def process_thumb_async(ph_path):
    def _resize_thumb(path):
        with Image.open(path) as img:
            img = img.convert("RGB").resize((320, 320))
            img.save(path, "JPEG")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(thread_pool, _resize_thumb, ph_path)
    return ph_path

async def concurrent_download(client, message, renamed_file_path, progress_msg):
    async with download_semaphore:
        return await client.download_media(
            message,
            file_name=renamed_file_path,
            progress=progress_for_pyrogram,
            progress_args=("Downloading...", progress_msg, time.time()),
        )

async def concurrent_upload(client, message, path, media_type, caption, ph_path, progress_msg):
    async with upload_semaphore:
        send_func = {"document": client.send_document, "video": client.send_video, "audio": client.send_audio}
        await send_func[media_type](
            chat_id=message.chat.id,
            **{media_type: path},
            caption=caption,
            thumb=ph_path,
            progress=progress_for_pyrogram,
            progress_args=("Uploading...", progress_msg, time.time()),
        )
        await progress_msg.delete()

# ===== Main Auto Rename Function =====
async def auto_rename_file_concurrent(client, message, file_info):
    user_id = message.from_user.id
    file_id = file_info["file_id"]
    async with processing_semaphore:
        if file_id in renaming_operations:
            return
        renaming_operations[file_id] = datetime.now()
        temp_dir = None
        try:
            user_config = await Botskingdom.col.find_one({"_id": user_id})
            if not user_config or not user_config.get("rename_format"):
                return await message.reply_text("Please set a rename format using /autorename")
            format_template = user_config["rename_format"]
            file_name = file_info["file_name"]
            media_type = "document" if message.document else "video" if message.video else "audio" if message.audio else None
            if not media_type:
                return await message.reply_text("Unsupported file type")
            if await check_anti_nsfw(file_name, message):
                return await message.reply_text("NSFW content detected")
            title = extract_title(file_name)
            season = extract_season_number(file_name)
            episode = extract_episode_number(file_name)
            quality = extract_quality(file_name)
            audio = extract_audio_info(file_name)
            new_file_name = format_template.format(
                title=title or "",
                season=f"S{season:02d}" if season else "",
                episode=f"E{episode:02d}" if episode else "",
                quality=quality or "",
                audio=audio or ""
            ).strip()
            new_file_name = re.sub(r'\s+', ' ', new_file_name)
            renamed_file_path, metadata_path, temp_dir = generate_unique_paths(new_file_name)
            progress_msg = await message.reply_text("Downloading...")
            downloaded_path = await concurrent_download(client, message, renamed_file_path, progress_msg)
            ph_path = await process_thumb_async(file_info.get("thumb_path")) if file_info.get("thumb_path") else None
            await concurrent_upload(client, message, downloaded_path, media_type, new_file_name, ph_path, progress_msg)
        except Exception as e:
            await message.reply_text(f"Error: {e}")
        finally:
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
            renaming_operations.pop(file_id, None)

# ===== Handlers =====
@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
@check_ban
async def auto_rename_files(client, message):
    file_id = message.document.file_id if message.document else message.video.file_id if message.video else message.audio.file_id
    file_name = message.document.file_name if message.document else message.video.file_name if message.video else message.audio.file_name
    file_info = {"file_id": file_id, "file_name": file_name or "Unknown", "message": message}
    asyncio.create_task(auto_rename_file_concurrent(client, message, file_info))


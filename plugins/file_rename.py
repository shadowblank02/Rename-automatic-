import os
import re
import time
import shutil
import asyncio
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import Message
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config
from functools import wraps

def check_ban_status(func):
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        is_banned, ban_reason = await codeflixbots.is_user_banned(user_id)
        if is_banned:
            await message.reply_text(
                f"**Yᴏᴜ ᴀʀᴇ ʙᴀɴɴᴇᴅ ғʀᴏᴍ ᴜsɪɴɢ ᴛʜɪs ʙᴏᴛ**"
            )
            return
        return await func(client, message, *args, **kwargs)
    return wrapper

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Enhanced Semaphores for better concurrency ---
download_semaphore = asyncio.Semaphore(3)  # Allow 3 concurrent downloads
upload_semaphore = asyncio.Semaphore(3)    # Limit 3 concurrent uploads
ffmpeg_semaphore = asyncio.Semaphore(3)    # Limit FFmpeg processes
processing_semaphore = asyncio.Semaphore(3) # Overall processing limit

# Thread pool for CPU-intensive operations
thread_pool = ThreadPoolExecutor(max_workers=4)

def detect_quality(file_name):
    """Detects quality for sorting, not for direct filename replacement."""
    quality_order = {"360p": 0, "480p": 1, "720p": 2, "1080p": 3}
    match = re.search(r"(360p|480p|720p|1080p)", file_name, re.IGNORECASE)
    return quality_order.get(match.group(1).lower(), 4) if match else 4

def extract_episode_number(filename):
    """
    Enhanced episode extraction with better pattern matching and validation.
    """
    if not filename:
        return None
        
    print(f"DEBUG: Extracting episode from: '{filename}'")
    
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
        # Pattern 6: Three digit numbers (likely episodes 001-999)
        re.compile(r'(?:^|[^0-9])(\d{3})(?:[^0-9]|$)', re.IGNORECASE),
        # Pattern 7: Two digit numbers in specific contexts
        re.compile(r'(?:[\s._-]|^)(\d{2})(?:[\s._-]|$)', re.IGNORECASE),
        # Pattern 8: Single digits with separators
        re.compile(r'(?:[\s._-])(\d{1})(?:[\s._-]|$)', re.IGNORECASE),
    ]
    
    for i, pattern in enumerate(patterns):
        matches = pattern.findall(filename)
        if matches:
            for match in matches:
                try:
                    episode_num = int(match)
                    # Validate episode number (should be reasonable)
                    if 1 <= episode_num <= 9999:
                        print(f"DEBUG: Episode Pattern {i+1} found episode: {episode_num}")
                        return episode_num
                except ValueError:
                    continue
    
    print(f"DEBUG: No episode number found in: '{filename}'")
    return None

def extract_season_number(filename):
    """
    Enhanced season extraction with better pattern matching and validation.
    """
    if not filename:
        return None
        
    print(f"DEBUG: Extracting season from: '{filename}'")
    
    patterns = [
        # Pattern 1: S##E## format (extract season part) - Most reliable
        re.compile(r'S(\d+)[._-]?E\d+', re.IGNORECASE),
        
        # Pattern 2: Season XX, SEASON XX formats (more explicit)
        re.compile(r'(?:Season|SEASON|season)[\s._-]*(\d+)', re.IGNORECASE),
        
        # Pattern 3: S## standalone (with word boundaries)
        re.compile(r'\bS(\d+)\b', re.IGNORECASE),
        
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
                # Validate season number (should be reasonable)
                if 1 <= season_num <= 99:
                    print(f"DEBUG: Season Pattern {i+1} found season: {season_num}")
                    return season_num
            except ValueError:
                continue
    
    print(f"DEBUG: No season number found in: '{filename}'")
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
    
    if re.search(r'pDual(?:audio)?', filename, re.IGNORECASE):
        detected_audio.append("Dual")

    if audio_keywords['Dual_Enhanced'].search(filename) and "Dual" not in detected_audio:
        detected_audio.append("Dual")

    priority_keywords = ['Hindi', 'English', 'Multi', 'Telugu', 'Tamil']
    for keyword in priority_keywords:
        if audio_keywords[keyword].search(filename):
            detected_audio.append(keyword)
            
    if "Dual" not in detected_audio and audio_keywords['Dual'].search(filename):
        detected_audio.append("Dual")

    for keyword, pattern in audio_keywords.items():
        if keyword not in priority_keywords and keyword not in ['Dual', 'Dual_Enhanced'] and pattern.search(filename):
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

# --- Modified filename generation to NOT add UUID to filename ---
def generate_unique_paths(renamed_file_name):
    """
    Generate file paths.
    IMPORTANT: This version does NOT append a unique ID to the filename itself.
    This means if two files are renamed to the exact same name, one will overwrite the other.
    Ensure your renaming template creates unique names or be aware of this limitation.
    """
    base_name, ext = os.path.splitext(renamed_file_name)
    
    if not ext.startswith('.'):
        ext = '.' + ext if ext else ''
    
    # Use the renamed_file_name directly as the unique_file_name_for_storage
    unique_file_name_for_storage = renamed_file_name 

    renamed_file_path = os.path.join("downloads", unique_file_name_for_storage)
    metadata_file_path = os.path.join("Metadata", unique_file_name_for_storage)
    
    os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
    
    return renamed_file_path, metadata_file_path, unique_file_name_for_storage

@Client.on_message(filters.command("start_sequence") & filters.private)
async def start_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id in active_sequences:
        await message.reply_text("Hᴇʏ ᴅᴜᴅᴇ...!! A sᴇǫᴜᴇɴᴄᴇ ɪs ᴀʟʀᴇᴀᴅʏ ᴀᴄᴛɪᴠᴇ! Usᴇ /end_sequence ᴛᴏ ᴇɴᴅ ɪᴛ.")
    else:
        active_sequences[user_id] = []
        message_ids[user_id] = []
        msg = await message.reply_text("Sᴇǫᴜᴇɴᴄᴇ sᴛᴀʀᴛᴇᴅ! Sᴇɴᴅ ʏᴏᴜʀ ғɪʟᴇs ɴᴏᴡ ʙʀᴏ....Fᴀsᴛ")
        message_ids[user_id].append(msg.message_id)

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    user_id = message.from_user.id

    file_id = (
        message.document.file_id if message.document else
        message.video.file_id if message.video else
        message.audio.file_id
    )
    file_name = (
        message.document.file_name if message.document else
        message.video.file_name if message.video else
        message.audio.file_name
    )
    file_info = {
        "file_id": file_id, 
        "file_name": file_name if file_name else "Unknown",
        "message": message,
        "episode_num": extract_episode_number(file_name if file_name else "Unknown")
    }

    if user_id in active_sequences:
        active_sequences[user_id].append(file_info)
        reply_msg = await message.reply_text("Wᴇᴡ...ғɪʟᴇs ʀᴇᴄᴇɪᴠᴇᴅ ɴᴏᴡ ᴜsᴇ /end_sequence ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ғɪʟᴇs...!!")
        message_ids[user_id].append(reply_msg.message_id)
        return

    task = asyncio.create_task(auto_rename_file_concurrent(client, message, file_info))

@Client.on_message(filters.command("end_sequence") & filters.private)
async def end_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id not in active_sequences:
        await message.reply_text("Wʜᴀᴛ ᴀʀᴇ ʏᴏᴜ ᴅᴏɪɴɢ ɴᴏ ᴀᴄᴛɪᴠᴇ sᴇǫᴜᴇɴᴄᴇ ғᴏᴜɴᴅ...!!")
    else:
        file_list = active_sequences.pop(user_id, [])
        delete_messages = message_ids.pop(user_id, [])
        count = len(file_list)

        if not file_list:
            await message.reply_text("Nᴏ ғɪʟᴇs ᴡᴇʀᴇ sᴇɴᴛ ɪɴ ᴛʜɪs sᴇǫᴜᴇɴᴄᴇ....ʙʀᴏ...!!")
        else:
            file_list.sort(key=lambda x: x["episode_num"] if x["episode_num"] is not None else float('inf'))
            await message.reply_text(f"Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ. Nᴏᴡ sᴇɴᴅɪɴɢ ʏᴏᴜʀ {count} ғɪʟᴇ(s) ʙᴀᴄᴋ ɪɴ sᴇǫᴜᴇɴᴄᴇ...!!")
            
            for index, file_info in enumerate(file_list, 1):
                try:
                    await asyncio.sleep(0.5)
                    
                    original_message = file_info["message"]
                    
                    if original_message.document:
                        await client.send_document(
                            message.chat.id,
                            original_message.document.file_id,
                            caption=f"{file_info['file_name']}"
                        )
                    elif original_message.video:
                        await client.send_video(
                            message.chat.id,
                            original_message.video.file_id,
                            caption=f"{file_info['file_name']}"
                        )
                    elif original_message.audio:
                        await client.send_audio(
                            message.chat.id,
                            original_message.audio.file_id,
                            caption=f"{file_info['file_name']}"
                        )
                except Exception as e:
                    await message.reply_text(f"Fᴀɪʟᴇᴅ ᴛᴏ sᴇɴᴅ ғɪʟᴇ: {file_info.get('file_name', '')}\n{e}")
            
            await message.reply_text(f"✅ Aʟʟ {count} ғɪʟes sᴇɴᴛ sᴜᴄᴄᴇssғᴜʟʟʏ ɪɴ sᴇǫᴜᴇɴᴄᴇ!")

        try:
            await client.delete_messages(chat_id=message.chat.id, message_ids=delete_messages)
        except Exception as e:
            print(f"Error deleting messages: {e}")

async def process_thumb_async(ph_path):
    """Process thumbnail in thread pool to avoid blocking"""
    def _resize_thumb(path):
        img = Image.open(path).convert("RGB")
        img = img.resize((320, 320))
        img.save(path, "JPEG")
    
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(thread_pool, _resize_thumb, ph_path)

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
                    message.chat.id,
                    document=path,
                    thumb=ph_path,
                    caption=caption,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "video":
                await client.send_video(
                    message.chat.id,
                    video=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
        except Exception as e:
            raise Exception(f"Upload Error: {e}")

async def auto_rename_file_concurrent(client, message, file_info):
    """
    MAIN CONCURRENT FUNCTION - Enhanced with better episode/season extraction
    and proper placeholder handling
    """
    async with processing_semaphore:  # Limit overall concurrent processing
        try:
            user_id = message.from_user.id
            file_id = file_info["file_id"]
            file_name = file_info["file_name"]

            if file_id in renaming_operations:
                elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
                if elapsed_time < 10:
                    return
            renaming_operations[file_id] = datetime.now()

            format_template = await codeflixbots.get_format_template(user_id)
            media_preference = await codeflixbots.get_media_preference(user_id)

            if not format_template:
                await message.reply_text("Pʟᴇᴀsᴇ Sᴇᴛ Aɴ Aᴜᴛᴏ Rᴇɴᴀᴍᴇ Fᴏʀᴍᴀᴛ Fɪʀsᴛ Usɪɴɢ /autorename")
                return

            media_type = media_preference

            if not media_type:
                if file_name.endswith((".mp4", ".mkv", ".avi", ".webm")):
                    media_type = "video"
                elif file_name.endswith((".mp3", ".flac", ".wav", ".ogg")):
                    media_type = "audio"
                else:
                    media_type = "document"
            
            if not media_type:
                media_type = "document"

            if await check_anti_nsfw(file_name, message):
                await message.reply_text("NSFW ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ. Fɪʟᴇ ᴜᴘʟᴏᴀᴅ ʀᴇᴊᴇᴄᴛᴇᴅ.")
                return

            # ENHANCED EXTRACTION - Fixed to properly detect from actual filename
            episode_number = extract_episode_number(file_name)
            season_number = extract_season_number(file_name)
            audio_info_extracted = extract_audio_info(file_name)  
            quality_extracted = extract_quality(file_name)

            print(f"DEBUG: Final extracted values - Season: {season_number}, Episode: {episode_number}")

            template = format_template
            
            # --- FIXED PLACEHOLDER REPLACEMENT LOGIC (RESTORED {} and word boundaries) ---
            
            # Format numbers with leading zeros
            season_value_formatted = str(season_number).zfill(2) if season_number is not None else "01"  # Default to 01 if not found
            episode_value_formatted = str(episode_number).zfill(2) if episode_number is not None else "01"  # Default to 01 if not found

            # 1. Handle SSeasonXX pattern specifically first (e.g., SSeason01 -> S01)
            # This regex looks for 'S' immediately followed by 'Season' (case-insensitive) and then digits.
            # It replaces it with 'S' and the formatted season number.
            template = re.sub(r'S(?:Season|season|SEASON)(\d+)', f'S{season_value_formatted}', template, flags=re.IGNORECASE)

            # 2. Regular SEASON PLACEHOLDER REPLACEMENT - Multiple patterns
            season_replacements = [
                # Curly brace patterns - REPLACES WITH JUST THE NUMBER (e.g., {season} -> 01)
                (re.compile(r'\{season\}', re.IGNORECASE), season_value_formatted),
                (re.compile(r'\{Season\}', re.IGNORECASE), season_value_formatted),
                (re.compile(r'\{SEASON\}', re.IGNORECASE), season_value_formatted),

                # Word boundary patterns - standalone words - REPLACES WITH JUST THE NUMBER (e.g., Season -> 01)
                (re.compile(r'\bseason\b', re.IGNORECASE), season_value_formatted),
                (re.compile(r'\bSeason\b', re.IGNORECASE), season_value_formatted),
                (re.compile(r'\bSEASON\b', re.IGNORECASE), season_value_formatted),
                
                # Specific season patterns with separators (e.g., Season 1, season-02) - NOW REMOVES "Season" TEXT (e.g., Season 01 -> 01)
                (re.compile(r'Season[\s._-]*\d*', re.IGNORECASE), season_value_formatted),
                (re.compile(r'season[\s._-]*\d*', re.IGNORECASE), season_value_formatted),
                (re.compile(r'SEASON[\s._-]*\d*', re.IGNORECASE), season_value_formatted),
            ]
            
            for pattern, replacement in season_replacements:
                template = pattern.sub(replacement, template)

            # NEW: Handle EPEpisode patterns specifically (e.g., EPEpisode -> EP01)
            # This regex looks for 'EP' immediately followed by 'Episode' (case-insensitive).
            # It replaces the matched "Episode" part with the formatted episode number, preserving 'EP'.
            template = re.sub(r'EP(?:Episode|episode|EPISODE)', f'EP{episode_value_formatted}', template, flags=re.IGNORECASE)

            # 3. Episode placeholder replacement - Now correctly handles {episode}, {Episode}, and standalone "episode"
            episode_patterns = [
                re.compile(r'\{episode\}', re.IGNORECASE),  # {episode}, {Episode} -> 01
                re.compile(r'\bEpisode\b', re.IGNORECASE),  # Episode, episode, EPISODE as standalone words -> 01
                # Removed re.compile(r'\bEP\b', re.IGNORECASE) previously
            ]
            
            for pattern in episode_patterns:
                template = pattern.sub(episode_value_formatted, template)

            # 4. Audio placeholder replacement - Now correctly handles {audio}, {Audio}, and standalone "audio"
            audio_replacement = audio_info_extracted if audio_info_extracted else ""
            audio_patterns = [
                re.compile(r'\{audio\}', re.IGNORECASE),    # {audio}, {Audio}
                re.compile(r'\bAudio\b', re.IGNORECASE),    # Audio, audio, AUDIO as standalone words
            ]
            
            for pattern in audio_patterns:
                template = pattern.sub(audio_replacement, template)

            # 5. Quality placeholder replacement - Now correctly handles {quality}, {Quality}, and standalone "quality"
            quality_replacement = quality_extracted if quality_extracted else ""
            quality_patterns = [
                re.compile(r'\{quality\}', re.IGNORECASE),  # {quality}, {Quality}
                re.compile(r'\bQuality\b', re.IGNORECASE),  # Quality, quality, QUALITY as standalone words
            ]
            
            for pattern in quality_patterns:
                template = pattern.sub(quality_replacement, template)

            # --- END FIXED PLACEHOLDER LOGIC ---

            # Clean up extra spaces, brackets, and separators
            template = re.sub(r'\s{2,}', ' ', template)  # Multiple spaces to single
            template = re.sub(r'\[\s*-\s*\]', '', template)  # Empty brackets with dash
            template = re.sub(r'\[\s*\]', '', template)  # Empty brackets
            template = template.strip()  # Remove leading/trailing whitespace
            template = re.sub(r'(\s*-\s*){2,}', r' - ', template)  # Multiple dashes
            template = re.sub(r'\s*-\s*', '-', template)  # Clean dash spacing
            template = re.sub(r'\s*\.\s*', '.', template)  # Clean dot spacing
            template = re.sub(r'(\.-|-\.)', '', template)  # Remove dot-dash combinations

            # Final cleanup for unwanted patterns
            template = re.sub(r'^\s*[-._\s]+', '', template)  # Leading separators
            template = re.sub(r'[-._\s]+\s*$', '', template)  # Trailing separators
            template = re.sub(r'(\s*[-._]+\s*){2,}', r' - ', template)  # Multiple separators

            _, file_extension = os.path.splitext(file_name)
            
            if not file_extension.startswith('.'):
                file_extension = '.' + file_extension if file_extension else ''
            
            renamed_file_name = f"{template}{file_extension}"
            
            print(f"DEBUG: Final renamed file: {renamed_file_name}")
            
            # This is where the change is:
            # We are now passing renamed_file_name directly without appending a unique_id
            renamed_file_path, metadata_file_path, unique_file_name_for_storage = generate_unique_paths(renamed_file_name)


            download_msg = await message.reply_text("Wᴇᴡ... Iᴀᴍ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

            ph_path = None

            try:
                path = await concurrent_download(client, message, renamed_file_path, download_msg)
                
                await download_msg.edit("Nᴏᴡ ᴀᴅᴅɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ ᴅᴜᴅᴇ...!!")

                ffmpeg_cmd = shutil.which('ffmpeg')
                if not ffmpeg_cmd:
                    raise Exception("FFmpeg not found")

                metadata_command = [
                    ffmpeg_cmd,
                    '-i', path,
                    '-metadata', f'title={await codeflixbots.get_title(user_id)}',
                    '-metadata', f'artist={await codeflixbots.get_artist(user_id)}',
                    '-metadata', f'author={await codeflixbots.get_author(user_id)}',
                    '-metadata:s:v', f'title={await codeflixbots.get_video(user_id)}',
                    '-metadata:s:a', f'title={await codeflixbots.get_audio(user_id)}',
                    '-metadata:s:s', f'title={await codeflixbots.get_subtitle(user_id)}',
                    '-metadata', f'encoded_by={await codeflixbots.get_encoded_by(user_id)}',
                    '-metadata', f'custom_tag={await codeflixbots.get_custom_tag(user_id)}',
                    '-map', '0',
                    '-c', 'copy',
                    '-loglevel', 'error',
                    metadata_file_path
                ]

                returncode, stdout, stderr = await run_ffmpeg_async(metadata_command)
                
                if returncode != 0:
                    error_message = stderr
                    await download_msg.edit(f"Mᴇᴛᴀᴅᴀᴛᴀ Eʀʀᴏʀ:\n{error_message}")
                    del renaming_operations[file_id]
                    return

                path = metadata_file_path

                await download_msg.edit("Wᴇᴡ... Iᴀm Uᴘʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

                c_caption = await codeflixbots.get_caption(message.chat.id)
                c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

                caption = (
                    c_caption.format(
                        filename=renamed_file_name,  
                        filesize=humanbytes(message.document.file_size) if message.document else "Unknown",
                        duration=convert(0),
                    )
                    if c_caption
                    else f"{renamed_file_name}"
                )

                if c_thumb:
                    ph_path = await client.download_media(c_thumb)
                elif media_type == "video" and getattr(message.video, "thumbs", None):
                    ph_path = await client.download_media(message.video.thumbs[0].file_id)

                if ph_path:
                    await process_thumb_async(ph_path)

                await concurrent_upload(client, message, path, media_type, caption, ph_path, download_msg)

                await download_msg.delete()

            except Exception as e:
                await download_msg.edit(f"❌ Eʀʀᴏʀ: {str(e)}")
                raise

            finally:
                cleanup_files = [path, renamed_file_path, metadata_file_path]
                if ph_path:
                    cleanup_files.append(ph_path)
                
                for file_path in cleanup_files:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as cleanup_e:
                            print(f"Error during file cleanup for {file_path}: {cleanup_e}")
                            pass
                
                if file_id in renaming_operations:
                    del renaming_operations[file_id]

        except Exception as e:
            if 'file_id' in locals() and file_id in renaming_operations:
                print(f"An error occurred during renaming for file_id {file_id}: {e}")

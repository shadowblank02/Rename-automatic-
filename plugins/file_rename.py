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

active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Enhanced Semaphores for better concurrency ---
download_semaphore = asyncio.Semaphore(5)  # Allow 5 concurrent downloads
upload_semaphore = asyncio.Semaphore(3)    # Allow 3 concurrent uploads
ffmpeg_semaphore = asyncio.Semaphore(2)    # Limit FFmpeg processes
processing_semaphore = asyncio.Semaphore(10) # Overall processing limit

# Thread pool for CPU-intensive operations
thread_pool = ThreadPoolExecutor(max_workers=4)

def detect_quality(file_name):
    quality_order = {"360p": 0, "480p": 1, "720p": 2, "1080p": 3}
    match = re.search(r"(360p|480p|720p|1080p)", file_name)
    return quality_order.get(match.group(1), 4) if match else 4

def extract_episode_number(filename):
    """Extract episode number from filename for sorting"""
    pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
    pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
    pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
    pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
    pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
    patternX = re.compile(r'(\d+)')
    
    for pattern in [pattern1, pattern2, pattern3, pattern3_2, pattern4]:
        match = re.search(pattern, filename)
        if match:
            return int(match.groups()[-1])
    
    match = re.search(patternX, filename)
    if match:
        return int(match.group(1))
    
    return 999

def extract_season_number(filename):
    """Extract season number from filename"""
    # Pattern for Season extraction (S01, S1, Season 1, etc.)
    season_patterns = [
        re.compile(r'S(\d+)(?:E|EP|\s)', re.IGNORECASE),  # S01E01, S1E1, S01 E01
        re.compile(r'Season\s*(\d+)', re.IGNORECASE),      # Season 1, Season 01
        re.compile(r'S(\d+)', re.IGNORECASE),              # S1, S01 (standalone)
        re.compile(r'Season(\d+)', re.IGNORECASE),         # Season1, Season01
        re.compile(r'(?:^|\s)(\d+)(?:st|nd|rd|th)?\s*Season', re.IGNORECASE), # 1st Season, 2nd Season
        re.compile(r'S\.(\d+)', re.IGNORECASE),            # S.01, S.1
    ]
    
    for pattern in season_patterns:
        match = re.search(pattern, filename)
        if match:
            season_num = int(match.group(1))
            return season_num if season_num > 0 else 1
    
    return 1  # Default to season 1 if not found

def extract_audio_info(filename):
    """Extract audio information from filename, including languages and 'dual'/'multi'"""
    audio_patterns = {
        # Specific Audio Language/Type
        'hindi': re.compile(r'\b(?:Hindi|HINDI)\b', re.IGNORECASE),
        'english': re.compile(r'\b(?:English|ENGLISH)\b', re.IGNORECASE),
        'multi': re.compile(r'\b(?:Multi|MULTI)\b', re.IGNORECASE),
        'telugu': re.compile(r'\b(?:Telugu|TELUGU)\b', re.IGNORECASE),
        'tamil': re.compile(r'\b(?:Tamil|TAMIL)\b', re.IGNORECASE),
        'dual': re.compile(r'\b(?:Dual|DUAL)\b', re.IGNORECASE), # Keeping this from previous fix

        # Audio codecs
        'aac': re.compile(r'\b(?:AAC|aac)\b', re.IGNORECASE),
        'ac3': re.compile(r'\b(?:AC3|ac3|AC-3|ac-3)\b', re.IGNORECASE),
        'dts': re.compile(r'\b(?:DTS|dts)\b', re.IGNORECASE),
        'mp3': re.compile(r'\b(?:MP3|mp3)\b', re.IGNORECASE),
        'flac': re.compile(r'\b(?:FLAC|flac)\b', re.IGNORECASE),
        'opus': re.compile(r'\b(?:OPUS|opus)\b', re.IGNORECASE),
        'vorbis': re.compile(r'\b(?:VORBIS|vorbis|OGG|ogg)\b', re.IGNORECASE),
        
        # Audio channels
        '2.0': re.compile(r'\b(?:2\.0|2ch|stereo)\b', re.IGNORECASE),
        '5.1': re.compile(r'\b(?:5\.1|5ch)\b', re.IGNORECASE),
        '7.1': re.compile(r'\b(?:7\.1|7ch)\b', re.IGNORECASE),
        'mono': re.compile(r'\b(?:mono|1ch|1\.0)\b', re.IGNORECASE),
        
        # Audio quality indicators
        'hq': re.compile(r'\b(?:HQ|hq|High[\s-]?Quality)\b', re.IGNORECASE),
        'lq': re.compile(r'\b(?:LQ|lq|Low[\s-]?Quality)\b', re.IGNORECASE),
        
        # Dolby variants
        'dolby': re.compile(r'\b(?:Dolby|DOLBY|DD|dd)\b', re.IGNORECASE),
        'atmos': re.compile(r'\b(?:Atmos|ATMOS)\b', re.IGNORECASE),
    }
    
    detected_audio = []
    
    for audio_type, pattern in audio_patterns.items():
        if pattern.search(filename):
            # Prioritize language/type names over generic ones for cleaner output
            if audio_type in ['hindi', 'english', 'multi', 'telugu', 'tamil', 'dual']:
                detected_audio.insert(0, audio_type.upper()) # Add to beginning to prioritize
            else:
                detected_audio.append(audio_type.upper())
    
    # Remove duplicates while preserving order (important after insert(0))
    detected_audio = list(dict.fromkeys(detected_audio))
    
    if detected_audio:
        return ' '.join(detected_audio)
    
    return 'Unknown' # Returns 'Unknown' if nothing specific is found

# --- Enhanced filename generation with UUID for uniqueness ---
def generate_unique_paths(renamed_file_name):
    """Generate unique file paths to avoid conflicts"""
    unique_id = str(uuid.uuid4())[:8]
    base_name, ext = os.path.splitext(renamed_file_name)
    
    unique_file_name = f"{base_name}_{unique_id}{ext}"
    renamed_file_path = f"downloads/{unique_file_name}"
    metadata_file_path = f"Metadata/{unique_file_name}"
    
    os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
    
    return renamed_file_path, metadata_file_path, unique_file_name

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

    # Create concurrent task for auto renaming - TRUE CONCURRENCY
    task = asyncio.create_task(auto_rename_file_concurrent(client, message, file_info))
    # Don't await here - let it run concurrently!

@Client.on_message(filters.command("end_sequence") & filters.private)
async def end_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id not in active_sequences:
        await message.reply_text("Wʜᴀᴛ ᴀʀᴇ ʏᴏᴜ ᴅᴏɪɴɢ ɴᴏ ᴀᴄᴛɪᴠᴇ sᴇǫᴜᴇɴᴄᴇ ғᴏᴜɴᴅ...!!")
        return

    file_list = active_sequences.pop(user_id, [])
    delete_messages = message_ids.pop(user_id, [])
    count = len(file_list)

    if not file_list:
        await message.reply_text("Nᴏ ғɪʟᴇs ᴡᴇʀᴇ sᴇɴᴛ ɪɴ ᴛʜɪs sᴇǫᴜᴇɴᴄᴇ....ʙʀᴏ...!!")
    else:
        file_list.sort(key=lambda x: x["episode_num"])
        await message.reply_text(f"Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ. Nᴏᴡ sᴇɴᴅɪɴɢ ʏᴏᴜʀ {count} ғɪʟᴇ(s) ʙᴀᴄᴋ ɪɴ sᴇǫᴜᴇɴᴄᴇ...!!")
        
        for index, file_info in enumerate(file_list, 1):
            try:
                await asyncio.sleep(0.5)
                
                if file_info["message"].document:
                    await client.send_document(
                        message.chat.id,
                        file_info["file_id"],
                        caption=f"{file_info['file_name']}"
                    )
                elif file_info["message"].video:
                    await client.send_video(
                        message.chat.id,
                        file_info["file_id"],
                        caption=f"{file_info['file_name']}"
                    )
                elif file_info["message"].audio:
                    await client.send_audio(
                        message.chat.id,
                        file_info["file_id"],
                        caption=f"{file_info['file_name']}"
                    )
                
            except Exception as e:
                await message.reply_text(f"Fᴀɪʟᴇᴅ ᴛᴏ sᴇɴᴅ ғɪʟᴇ: {file_info.get('file_name', '')}\n{e}")
        
        await message.reply_text(f"✅ Aʟʟ {count} ғɪʟᴇs sᴇɴᴛ sᴜᴄᴄᴇssғᴜʟʟʏ ɪɴ sᴇǫᴜᴇɴᴄᴇ!")

    try:
        await client.delete_messages(chat_id=message.chat.id, message_ids=delete_messages)
    except Exception as e:
        print(f"Error deleting messages: {e}")

# Regex patterns for filename parsing
pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
patternX = re.compile(r'(\d+)')
# Updated pattern5 to include 360p
pattern5 = re.compile(r'\b(?:.*?(\d{3}p|\d{3,4}[^\dp]*p).*?|.*?(\d{3}p|\d{3,4}p))\b', re.IGNORECASE)
pattern6 = re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE)
pattern7 = re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE)
pattern8 = re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE)
pattern9 = re.compile(r'[([<{]?\s*4kX264\s*[)\]>}]?', re.IGNORECASE)
pattern10 = re.compile(r'[([<{]?\s*4kx265\s*[)\]>}]?', re.IGNORECASE)

def extract_quality(filename):
    match5 = re.search(pattern5, filename)
    if match5:
        return match5.group(1) or match5.group(2)
    match6 = re.search(pattern6, filename)
    if match6:
        return "4k"
    match7 = re.search(pattern7, filename)
    if match7:
        return "2k"
    match8 = re.search(pattern8, filename)
    if match8:
        return "HdRip"
    match9 = re.search(pattern9, filename)
    if match9:
        return "4kX264"
    match10 = re.search(pattern10, filename)
    if match10:
        return "4kx265"
    return "Unknown"

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
    MAIN CONCURRENT FUNCTION - Uses semaphores and thread pools for true concurrency
    Enhanced with season and audio extraction
    """
    async with processing_semaphore:  # Limit overall concurrent processing
        try:
            user_id = message.from_user.id
            file_id = file_info["file_id"]
            file_name = file_info["file_name"]

            # Early duplicate check
            if file_id in renaming_operations:
                elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
                if elapsed_time < 10:
                    return
            renaming_operations[file_id] = datetime.now()

            # Get user settings
            format_template = await codeflixbots.get_format_template(user_id)
            media_preference = await codeflixbots.get_media_preference(user_id)

            if not format_template:
                await message.reply_text("Pʟᴇᴀsᴇ Sᴇᴛ Aɴ Aᴜᴛᴏ Rᴇɴᴀᴍᴇ Fᴏʀᴍᴀᴛ Fɪʀsᴛ Usɪɴɢ /autorename")
                return

            media_type = media_preference or "document"
            if file_name.endswith(".mp4"):
                media_type = "video"
            elif file_name.endswith(".mp3"):
                media_type = "audio"

            # NSFW check
            if await check_anti_nsfw(file_name, message):
                await message.reply_text("NSFW ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ. Fɪʟᴇ ᴜᴘʟᴏᴀᴅ ʀᴇᴊᴇᴄᴛᴇᴅ.")
                return

            # Extract information from filename
            episode_number = extract_episode_number(file_name)
            season_number = extract_season_number(file_name)
            audio_info = extract_audio_info(file_name) 
            
            print(f"Extracted Episode Number: {episode_number}")
            print(f"Extracted Season Number: {season_number}")
            print(f"Extracted Audio Info: {audio_info}")

            # Process template with all placeholders
            template = format_template
            
            # Episode placeholders
            if episode_number and episode_number != 999:
                episode_placeholders = ["episode", "Episode", "EPISODE", "{episode}"]
                for placeholder in episode_placeholders:
                    template = template.replace(placeholder, str(episode_number).zfill(2), 1)
            
            # Season placeholders
            if season_number:
                season_placeholders = ["season", "Season", "SEASON", "{season}"]
                for placeholder in season_placeholders:
                    template = template.replace(placeholder, str(season_number).zfill(2), 1)
            
            # Audio placeholders (Logic adjusted based on your feedback)
            audio_placeholders = ["audio", "Audio", "AUDIO", "{audio}"]
            
            replacement_audio = audio_info if audio_info and audio_info != "Unknown" else ""
            
            for placeholder in audio_placeholders:
                # This regex will match "[audio]", "{audio}", or "audio" (case-insensitive)
                # and replace it with replacement_audio.
                # If replacement_audio is empty, it will effectively remove "[audio]" or "{audio}"
                # If replacement_audio is not empty, it will replace "[audio]" with "[YOUR_AUDIO_INFO]" or "{audio}" with "{YOUR_AUDIO_INFO}"
                # If the user's template is just "audio", it will replace "audio" with "YOUR_AUDIO_INFO"
                template = re.sub(r'(\[|\{)?' + re.escape(placeholder) + r'(\]|\})?', 
                                  lambda m: f"{m.group(1) or ''}{replacement_audio}{m.group(2) or ''}", 
                                  template, flags=re.IGNORECASE)


            # Quality placeholders
            quality_placeholders = ["quality", "Quality", "QUALITY", "{quality}"]
            for quality_placeholder in quality_placeholders:
                # Use regex to replace all occurrences of the placeholder,
                # considering it might be enclosed in brackets like [quality] or {quality}
                template = re.sub(r'(\[|\{)?' + re.escape(quality_placeholder) + r'(\]|\})?', 
                                  lambda m: f"{m.group(1) or ''}{extract_quality(file_name)}{m.group(2) or ''}", 
                                  template, flags=re.IGNORECASE)

            _, file_extension = os.path.splitext(file_name)
            renamed_file_name = f"{template}{file_extension}"
            
            # Generate unique paths to avoid conflicts
            renamed_file_path, metadata_file_path, unique_file_name = generate_unique_paths(renamed_file_name)

            # Start download with status message
            download_msg = await message.reply_text("Wᴇᴡ... Iᴀᴍ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

            ph_path = None

            try:
                # Concurrent download
                path = await concurrent_download(client, message, renamed_file_path, download_msg)
                
                await download_msg.edit("Nᴏᴡ ᴀᴅᴅɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ ᴅᴜᴅᴇ...!!")

                # Get metadata settings
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

                # Run FFmpeg asynchronously in thread pool
                returncode, stdout, stderr = await run_ffmpeg_async(metadata_command)
                
                if returncode != 0:
                    error_message = stderr
                    await download_msg.edit(f"Mᴇᴛᴀᴅᴀᴛᴀ Eʀʀᴏʀ:\n{error_message}")
                    del renaming_operations[file_id]
                    return

                path = metadata_file_path

                await download_msg.edit("Wᴇᴡ... Iᴀᴍ Uᴘʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

                # Prepare caption and thumbnail
                c_caption = await codeflixbots.get_caption(message.chat.id)
                c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

                caption = (
                    c_caption.format(
                        filename=renamed_file_name,  # Use original renamed name, not unique
                        filesize=humanbytes(message.document.file_size) if message.document else "Unknown",
                        duration=convert(0),
                    )
                    if c_caption
                    else f"{renamed_file_name}"
                )

                # Process thumbnail concurrently
                if c_thumb:
                    ph_path = await client.download_media(c_thumb)
                elif media_type == "video" and getattr(message.video, "thumbs", None):
                    ph_path = await client.download_media(message.video.thumbs[0].file_id)

                if ph_path:
                    await process_thumb_async(ph_path)

                # Concurrent upload
                await concurrent_upload(client, message, path, media_type, caption, ph_path, download_msg)

                # Success - delete status message
                await download_msg.delete()

            except Exception as e:
                await download_msg.edit(f"❌ Eʀʀᴏʀ: {str(e)}")
                raise

            finally:
                # Cleanup files
                cleanup_files = [path, renamed_file_path, metadata_file_path]
                if ph_path:
                    cleanup_files.append(ph_path)
                
                for file_path in cleanup_files:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except:
                            pass
                
                # Remove from operations tracking
                if file_id in renaming_operations:
                    del renaming_operations[file_id]

        except Exception as e:
            if 'file_id' in locals() and file_id in renaming_operations:
                del renaming_operations[file_id]
            print(f"Concurrent rename error: {e}")

# Keep the original function for sequence mode (it now calls the concurrent one)
async def auto_rename_file(client, message, file_info, is_sequence=False, status_msg=None):
    """Original function for sequence mode - kept for compatibility"""
    return await auto_rename_file_concurrent(client, message, file_info)

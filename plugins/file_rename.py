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
    Extracts episode number from filename.
    Prioritizes specific patterns (SXXEXX, EXX) to avoid misidentification.
    """
    patterns = [
        # S01E01, S01.EP01, S01-E01 (S then optional separator then E/EP and digits)
        re.compile(r'S\d+(?:[.-]?|_)?[EePp](\d+)', re.IGNORECASE),
        # E01, EP01, [E01], (E01) - more robust for standalone E/EP, bounded to prevent partial matches
        re.compile(r'(?:[\[(]?|\b)(?:[EePp])(\d+)(?:[\])]?|\b)', re.IGNORECASE),
        # Episode 01, EPISODE 01, [Episode 01]
        re.compile(r'(?:[\[(]?Episode[\s._-]*|[\[(]?EP[\s._-]*)(?:\s*)?(\d+)[\])]?', re.IGNORECASE),
        # Simple digit after common separators for episodes (e.g., - 01), ensuring it's not a year or other number
        re.compile(r'(?:[\s._-][EePp]?|\b)(\d+)(?:\s|\.|$)', re.IGNORECASE),
        # Matches "1 of 10" or similar episode count structures
        re.compile(r'\b(\d+)\s*of\s*\d+\b', re.IGNORECASE),
    ]
    
    print(f"DEBUG: Attempting to extract episode from: '{filename}'") # Debug print
    for i, pattern in enumerate(patterns):
        match = re.search(pattern, filename)
        if match:
            try:
                extracted_episode = int(match.group(1))
                print(f"DEBUG: Episode Pattern {i+1} ('{pattern.pattern}') matched '{match.group(0)}', extracted episode: {extracted_episode}") # Debug print
                return extracted_episode
            except ValueError:
                print(f"DEBUG: Episode Pattern {i+1} matched but could not convert to int: '{match.group(1)}'") # Debug print
                continue 
            
    print(f"DEBUG: No episode number extracted for: '{filename}'") # Debug print
    return None 

def extract_season_number(filename):
    """
    Extracts season number from filename.
    Prioritizes specific patterns (SXXEXX, Season XX, SXX).
    """
    season_patterns = [
        # S01E01, S01.EP01, S01-E01 (S and digits then optional separator then E/EP and digits)
        re.compile(r'S(\d+)(?:[.-]?|_)?[EePp]\d+', re.IGNORECASE),
        # Season 1, Season 01, [Season 1], (Season 01) - more robust for standalone Season
        re.compile(r'(?:[\[(]?Season[\s._-]*|[\[(]?S[\s._-]*)(?:\s*)?(\d+)[\])]?', re.IGNORECASE),
        # S1, S01 (standalone, using more reliable word boundaries or separators)
        re.compile(r'\bS(\d+)\b', re.IGNORECASE), # Matches S1, S01 as whole words (e.g., "Show.S01.mkv")
        re.compile(r'[._-]S(\d+)(?:[._-]|$)', re.IGNORECASE) # Matches ".S1." or "-S01-" or "S01" at end (e.g., "Show.Name.S01.mkv")
    ]
    
    print(f"DEBUG: Attempting to extract season from: '{filename}'") # Debug print
    for i, pattern in enumerate(season_patterns):
        match = re.search(pattern, filename)
        if match:
            try:
                extracted_season = int(match.group(1))
                print(f"DEBUG: Season Pattern {i+1} ('{pattern.pattern}') matched '{match.group(0)}', extracted season: {extracted_season}") # Debug print
                return extracted_season
            except ValueError:
                print(f"DEBUG: Season Pattern {i+1} matched but could not convert to int: '{match.group(1)}'") # Debug print
                continue 
            
    print(f"DEBUG: No season number extracted for: '{filename}'") # Debug print
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

# --- Enhanced filename generation with UUID for uniqueness ---
def generate_unique_paths(renamed_file_name):
    """Generate unique file paths to avoid conflicts"""
    unique_id = str(uuid.uuid4())[:8]
    base_name, ext = os.path.splitext(renamed_file_name)
    
    if not ext.startswith('.'):
        ext = '.' + ext if ext else ''
    
    unique_file_name = f"{base_name}_{unique_id}{ext}"
    renamed_file_path = os.path.join("downloads", unique_file_name)
    metadata_file_path = os.path.join("Metadata", unique_file_name)
    
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

    task = asyncio.create_task(auto_rename_file_concurrent(client, message, file_info))

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
    MAIN CONCURRENT FUNCTION - Uses semaphores and thread pools for true concurrency
    Enhanced with season and audio extraction
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

            episode_number = extract_episode_number(file_name)
            season_number = extract_season_number(file_name)
            audio_info_extracted = extract_audio_info(file_name)  
            quality_extracted = extract_quality(file_name)

            template = format_template
            
            # --- Placeholder Replacement Logic ---
            
            season_value_formatted = str(season_number).zfill(2) if season_number is not None else None 
            episode_value_formatted = str(episode_number).zfill(2) if episode_number is not None else None 

            # 1. Combined SSeason and EP{episode} placeholder handling
            season_episode_block_regex = re.compile(r'(\[?\s*SSeason\s*-\s*EP\{episode\}\s*\]?)', re.IGNORECASE)

            def season_episode_replacer(match):
                has_brackets = match.group(1).startswith('[') and match.group(1).endswith(']')

                season_part = ""
                episode_part = ""

                if season_value_formatted:
                    season_part = f"S{season_value_formatted}"
                
                if episode_value_formatted:
                    episode_part = f"EP{episode_value_formatted}"

                if season_part and episode_part:
                    return f"[{season_part} -{episode_part}]" if has_brackets else f"{season_part}-{episode_part}"
                elif season_part: 
                    return f"[{season_part}]" if has_brackets else season_part
                elif episode_part: 
                    return f"[{episode_part}]" if has_brackets else episode_part
                else: 
                    return ""
            
            template = season_episode_block_regex.sub(season_episode_replacer, template)


            # 2. Season placeholder replacement (covers Season, season, SEASON, {season})
            season_placeholder_regex = re.compile(r'\b(?:Season|\{Season\})\b', re.IGNORECASE)
            if season_value_formatted:
                template = season_placeholder_regex.sub(season_value_formatted, template)
            else:
                template = season_placeholder_regex.sub("", template) 

            # 3. Episode placeholder replacement (covers Episode, episode, EPISODE, {episode})
            episode_placeholder_regex = re.compile(r'\b(?:Episode|\{Episode\})\b', re.IGNORECASE)
            if episode_value_formatted:
                template = episode_placeholder_regex.sub(episode_value_formatted, template)
            else:
                template = episode_placeholder_regex.sub("", template) 


            # 4. Audio placeholder replacement (Restored [Audio] recognition)
            audio_placeholder_regex = re.compile(r'\b(?:Audio|\{Audio\}|\[Audio\])\b', re.IGNORECASE) 
            replacement_audio = audio_info_extracted if audio_info_extracted else ""

            def audio_replacer_new(match):
                return replacement_audio 
            template = audio_placeholder_regex.sub(audio_replacer_new, template)


            # 5. Quality placeholder replacement (Restored [Quality] recognition)
            quality_placeholder_regex = re.compile(r'\b(?:Quality|\{Quality\}|\[Quality\])\b', re.IGNORECASE) 
            replacement_quality = quality_extracted if quality_extracted else ""

            def quality_replacer_new(match):
                return replacement_quality 
            template = quality_placeholder_regex.sub(quality_replacer_new, template)

            # --- END Placeholder Logic ---

            # Clean up extra spaces or hyphens left by removed placeholders
            template = re.sub(r'\s{2,}', ' ', template) 
            template = re.sub(r'\[\s*-\s*\]', '', template) 
            template = template.strip() 
            template = re.sub(r'(\s*-\s*){2,}', r' - ', template) 
            template = re.sub(r'\s*-\s*', '-', template) 
            template = re.sub(r'\s*\.\s*', '.', template) 
            template = re.sub(r'(\.-|-\.)', '', template) 

            # Final pass for common unwanted patterns after replacement
            template = re.sub(r'^\s*[-._\s]+', '', template) 
            template = re.sub(r'[-._\s]+\s*$', '', template) 
            template = re.sub(r'(\s*[-._]+\s*){2,}', r' - ', template) 

            _, file_extension = os.path.splitext(file_name)
            
            if not file_extension.startswith('.'):
                file_extension = '.' + file_extension if file_extension else ''
            
            renamed_file_name = f"{template}{file_extension}"
            
            renamed_file_path, metadata_file_path, unique_file_name = generate_unique_paths(renamed_file_name)

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
                del renaming_operations[file_id]
            print(f"Concurrent rename outer error: {e}")

async def auto_rename_file(client, message, file_info, is_sequence=False, status_msg=None):
    return await auto_rename_file_concurrent(client, message, file_info)

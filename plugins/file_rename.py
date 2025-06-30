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

# --- Global Dictionaries ---
active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Enhanced Semaphores for better concurrency ---
download_semaphore = asyncio.Semaphore(5)  # Allow 5 concurrent downloads
upload_semaphore = asyncio.Semaphore(3)    # Allow 3 concurrent uploads
ffmpeg_semaphore = asyncio.Semaphore(2)    # Limit FFmpeg processes
processing_semaphore = asyncio.Semaphore(10) # Overall processing limit - Adjust based on your server's capacity and I/O.

# Thread pool for CPU-intensive operations
thread_pool = ThreadPoolExecutor(max_workers=4)

# --- Utility Functions (Keep as is, no changes needed here for the issues) ---
def detect_quality(file_name):
    quality_order = {"480p": 1, "720p": 2, "1080p": 3}
    match = re.search(r"(480p|720p|1080p)", file_name)
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
    season_patterns = [
        re.compile(r'S(\d+)(?:E|EP|\s)', re.IGNORECASE),  
        re.compile(r'Season\s*(\d+)', re.IGNORECASE),      
        re.compile(r'S(\d+)', re.IGNORECASE),              
        re.compile(r'Season(\d+)', re.IGNORECASE),         
        re.compile(r'(?:^|\s)(\d+)(?:st|nd|rd|th)?\s*Season', re.IGNORECASE), 
        re.compile(r'S\.(\d+)', re.IGNORECASE),            
    ]
    
    for pattern in season_patterns:
        match = re.search(pattern, filename)
        if match:
            season_num = int(match.group(1))
            return season_num if season_num > 0 else 1
    
    return 1  

def extract_audio_info(filename):
    """Extract audio information from filename, including languages, 'dual'/'multi', and generic 'audio'"""
    audio_patterns = {
        'hindi': re.compile(r'\b(?:Hindi|HINDI)\b', re.IGNORECASE),
        'english': re.compile(r'\b(?:English|ENGLISH)\b', re.IGNORECASE),
        'multi': re.compile(r'\b(?:Multi|MULTI)\b', re.IGNORECASE),
        'telugu': re.compile(r'\b(?:Telugu|TELUGU)\b', re.IGNORECASE),
        'tamil': re.compile(r'\b(?:Tamil|TAMIL)\b', re.IGNORECASE),
        'dual': re.compile(r'\b(?:Dual|DUAL)\b', re.IGNORECASE),
        'audio_quality_combo': re.compile(r'((?:360p|480p|720p|1080p)?\s*audio)\b', re.IGNORECASE), 
        'aac': re.compile(r'\b(?:AAC|aac)\b', re.IGNORECASE),
        'ac3': re.compile(r'\b(?:AC3|ac3|AC-3|ac-3)\b', re.IGNORECASE),
        'dts': re.compile(r'\b(?:DTS|dts)\b', re.IGNORECASE),
        'mp3': re.compile(r'\b(?:MP3|mp3)\b', re.IGNORECASE),
        'flac': re.compile(r'\b(?:FLAC|flac)\b', re.IGNORECASE),
        'opus': re.compile(r'\b(?:OPUS|opus)\b', re.IGNORECASE),
        'vorbis': re.compile(r'\b(?:VORBIS|vorbis|OGG|ogg)\b', re.IGNORECASE),
        '2.0': re.compile(r'\b(?:2\.0|2ch|stereo)\b', re.IGNORECASE),
        '5.1': re.compile(r'\b(?:5\.1|5ch)\b', re.IGNORECASE),
        '7.1': re.compile(r'\b(?:7\.1|7ch)\b', re.IGNORECASE),
        'mono': re.compile(r'\b(?:mono|1ch|1\.0)\b', re.IGNORECASE),
        'hq': re.compile(r'\b(?:HQ|hq|High[\s-]?Quality)\b', re.IGNORECASE),
        'lq': re.compile(r'\b(?:LQ|lq|Low[\s-]?Quality)\b', re.IGNORECASE),
        'dolby': re.compile(r'\b(?:Dolby|DOLBY|DD|dd)\b', re.IGNORECASE),
        'atmos': re.compile(r'\b(?:Atmos|ATMOS)\b', re.IGNORECASE),
    }
    
    detected_audio = []
    
    for audio_type, pattern in audio_patterns.items():
        match = pattern.search(filename)
        if match:
            if audio_type == 'audio_quality_combo':
                detected_audio.append(match.group(1).replace(' ', '').upper())  
            elif audio_type in ['hindi', 'english', 'multi', 'telugu', 'tamil', 'dual']:
                detected_audio.insert(0, audio_type.upper()) 
            else:
                detected_audio.append(audio_type.upper())
    
    detected_audio = list(dict.fromkeys(detected_audio)) 
    
    if detected_audio:
        return ' '.join(detected_audio)
    
    return 'Unknown'

def generate_unique_paths(renamed_file_name):
    """Generate unique file paths to avoid conflicts"""
    unique_id = str(uuid.uuid4())[:8]
    base_name, ext = os.path.splitext(renamed_file_name)
    
    downloads_dir = Config.DOWNLOAD_DIR
    metadata_dir = "Metadata" # Ensure this directory exists or use Config

    os.makedirs(downloads_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True) # Create Metadata dir if not in Config

    unique_file_name = f"{base_name}_{unique_id}{ext}"
    renamed_file_path = os.path.join(downloads_dir, unique_file_name)
    metadata_file_path = os.path.join(metadata_dir, unique_file_name)
    
    return renamed_file_path, metadata_file_path, unique_file_name

# Regex patterns for filename parsing (duplicated, but kept as is per your request)
pattern5 = re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE)
pattern6 = re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE)
pattern7 = re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE)
pattern8 = re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE)
pattern9 = re.compile(r'[([<{]?\s*4kX264\s*[)\]>}]?', re.IGNORECASE)
pattern10 = re.compile(r'[([<{]?\s*4kx265\s*[)\]>}]?', re.IGNORECASE)

def extract_quality(filename):
    match5 = re.search(pattern5, filename)
    if match5:
        return (match5.group(1) or match5.group(2)).lower() 
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
            file_size = os.path.getsize(path) # Get actual file size after processing
            duration = 0
            if message.video:
                duration = message.video.duration
            elif message.audio:
                duration = message.audio.duration

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
                    duration=duration, 
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=duration, 
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
        user_id = message.from_user.id
        file_id = file_info["file_id"]
        file_name = file_info["file_name"]
        download_msg = None # Initialize download_msg here

        try:
            if file_id in renaming_operations:
                elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
                if elapsed_time < 10: 
                    print(f"Skipping duplicate processing for file_id: {file_id}")
                    return
            renaming_operations[file_id] = datetime.now() 

            format_template = await codeflixbots.get_format_template(user_id)
            media_preference = await codeflixbots.get_media_preference(user_id)

            if not format_template:
                # If no format template is set, just reply and exit
                await message.reply_text("Pʟᴇᴀsᴇ Sᴇᴛ Aɴ Aᴜᴛᴏ Rᴇɴᴀᴍᴇ Fᴏʀᴍᴀᴛ Fɪʀsᴛ Usɪɴɢ /autorename")
                if file_id in renaming_operations:
                    del renaming_operations[file_id]
                return

            media_type = media_preference or "document"
            if file_name.lower().endswith((".mp4", ".mkv", ".avi", ".webm")):
                media_type = "video"
            elif file_name.lower().endswith((".mp3", ".flac", ".wav", ".ogg")):
                media_type = "audio"

            if await check_anti_nsfw(file_name, message):
                await message.reply_text("NSFW ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ. Fɪʟᴇ ᴜᴘʟᴏᴀᴅ ʀᴇᴊᴇᴄᴛᴇᴅ.")
                if file_id in renaming_operations:
                    del renaming_operations[file_id]
                return

            episode_number = extract_episode_number(file_name)
            season_number = extract_season_number(file_name)
            audio_info = extract_audio_info(file_name)  
            
            print(f"Original filename: {file_name}")
            print(f"Extracted Episode Number: {episode_number}")
            print(f"Extracted Season Number: {season_number}")
            print(f"Extracted Audio Info: {audio_info}")

            template = format_template
            
            # Episode placeholders
            if episode_number and episode_number != 999:
                episode_placeholders = ["episode", "Episode", "EPISODE", "{episode}"]
                for placeholder in episode_placeholders:
                    template = template.replace(placeholder, str(episode_number).zfill(2), 1)
            else:
                template = re.sub(r'\{episode\}', '', template, flags=re.IGNORECASE)
                template = re.sub(r'EP(?:ISODE)?\s*\d+', '', template, flags=re.IGNORECASE) 
                template = re.sub(r'(?:E|EP)\s*(\d+)', '', template, flags=re.IGNORECASE) 

            # Season placeholders
            if season_number and season_number != 1: 
                season_placeholders = ["season", "Season", "SEASON", "{season}"]
                for placeholder in season_placeholders:
                    template = template.replace(placeholder, str(season_number).zfill(2), 1)
            else:
                template = re.sub(r'\{season\}', '', template, flags=re.IGNORECASE)
                template = re.sub(r'S(?:EASON)?\s*\d+', '', template, flags=re.IGNORECASE) 

            # Quality placeholders
            quality_placeholders = ["quality", "Quality", "QUALITY", "{quality}"]
            extracted_qualities = extract_quality(file_name)
            
            for quality_placeholder in quality_placeholders:
                bracketed_pattern = re.escape(f"[{quality_placeholder}]")
                if re.search(bracketed_pattern, template, re.IGNORECASE):
                    if extracted_qualities == "Unknown":
                        template = re.sub(bracketed_pattern, '', template, flags=re.IGNORECASE) 
                    else:
                        template = re.sub(bracketed_pattern, f'[{extracted_qualities}]', template, flags=re.IGNORECASE)
                
                non_bracketed_pattern = re.compile(r'\b' + re.escape(quality_placeholder) + r'\b', re.IGNORECASE)
                if re.search(non_bracketed_pattern, template, re.IGNORECASE):
                    if extracted_qualities == "Unknown":
                        template = re.sub(non_bracketed_pattern, '', template) 
                    else:
                        template = re.sub(non_bracketed_pattern, extracted_qualities, template)

            actual_audio_string = ""
            if audio_info and audio_info != "Unknown":
                actual_audio_string = audio_info

            template = re.sub(r'\[audio\]', f'[{actual_audio_string}]' if actual_audio_string else '', template, flags=re.IGNORECASE)
            template = re.sub(r'\{audio\}', f'{actual_audio_string}' if actual_audio_string else '', template, flags=re.IGNORECASE)
            template = re.sub(r'\baudio\b', actual_audio_string, template, flags=re.IGNORECASE)

            # Specific cleanup for your format [SSeason -EP{episode}]
            season_episode_block_content = []
            if season_number and season_number != 1:
                season_episode_block_content.append(f"S{str(season_number).zfill(2)}")
            if episode_number and episode_number != 999:
                season_episode_block_content.append(f"EP{str(episode_number).zfill(2)}")
            
            dynamic_season_episode = ""
            if season_episode_block_content:
                dynamic_season_episode = "[" + "-".join(season_episode_block_content) + "]"

            template = re.sub(r'\[SSeason\s*-EP(?:{episode})?\]', dynamic_season_episode, template, flags=re.IGNORECASE)
            template = re.sub(r'\[SSeason\]', f'[S{str(season_number).zfill(2)}]' if season_number and season_number != 1 else '', template, flags=re.IGNORECASE)
            template = re.sub(r'\[EP(?:{episode})?\]', f'[EP{str(episode_number).zfill(2)}]' if episode_number and episode_number != 999 else '', template, flags=re.IGNORECASE)


            # Final Cleanup Steps
            template = template.replace('[]', '').strip()  
            template = re.sub(r'\s+', ' ', template).strip()
            template = re.sub(r'^[-\s._]+|[ -_.]+$', '', template)
            template = re.sub(r'\s*-\s*', '-', template)

            _, file_extension = os.path.splitext(file_name)
            renamed_file_name = f"{template}{file_extension}"
            
            renamed_file_path, metadata_file_path, unique_file_name = generate_unique_paths(renamed_file_name)

            # Send initial "downloading" message for *this specific file's process*
            # This is crucial for individual files and also for each file in a sequence
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
                    # Don't raise here if you want to continue processing other files in a sequence
                    # Just ensure cleanup is handled.
                    return 

                path = metadata_file_path

                await download_msg.edit("Wᴇᴡ... Iᴀᴍ Uᴘʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

                c_caption = await codeflixbots.get_caption(message.chat.id)
                c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

                file_size_display = "Unknown"
                if os.path.exists(path):
                    file_size_display = humanbytes(os.path.getsize(path))

                caption = (
                    c_caption.format(
                        filename=renamed_file_name,  
                        filesize=file_size_display,
                        duration=convert(message.video.duration) if message.video else convert(0),
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

                await download_msg.delete() # Delete the processing message on success

            except Exception as e:
                # Catch specific errors and update the message
                error_text = f"❌ Eʀʀᴏʀ ᴘʀᴏᴄᴇssɪɴɢ {file_name}: {str(e)}"
                print(error_text) # Log the error
                if download_msg: # Only try to edit if the message was sent
                    await download_msg.edit(error_text)
                # Don't re-raise here for concurrent tasks, let them fail gracefully
                # and allow other tasks to complete.

            finally:
                # Cleanup files regardless of success or failure
                cleanup_files = [path, renamed_file_path, metadata_file_path]
                if ph_path:
                    cleanup_files.append(ph_path)
                
                for file_path_to_clean in cleanup_files:
                    if file_path_to_clean and os.path.exists(file_path_to_clean):
                        try:
                            os.remove(file_path_to_clean)
                            print(f"Cleaned up: {file_path_to_clean}")
                        except OSError as e:
                            print(f"Error cleaning up {file_path_to_clean}: {e}")
                
                if file_id in renaming_operations:
                    del renaming_operations[file_id]

# --- Modified auto_rename_files to provide immediate feedback for individual files ---
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
        "message": message, # Store the original message for downloading
        "episode_num": extract_episode_number(file_name if file_name else "Unknown")
    }

    if user_id in active_sequences:
        active_sequences[user_id].append(file_info)
        reply_msg = await message.reply_text("Wᴇᴡ...ғɪʟᴇs ʀᴇᴄᴇɪᴠᴇᴅ ɴᴏᴡ ᴜsᴇ /end_sequence ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ғɪʟᴇs...!!")
        message_ids[user_id].append(reply_msg.message_id)
        return
    else:
        # **This is the key change for individual files**
        # Send an immediate reply, then start the processing in the background.
        await message.reply_text("Yᴏᴜʀ ғɪʟᴇ ɪs ʙᴇɪɴɢ ᴘʀᴏᴄᴇssᴇᴅ... Pʟᴇᴀsᴇ ᴡᴀɪᴛ.", quote=True)
        asyncio.create_task(auto_rename_file_concurrent(client, message, file_info))

# --- Modified end_sequence to trigger concurrent renaming and wait ---
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
        # Sort the files by episode number
        file_list.sort(key=lambda x: x["episode_num"])
        
        await message.reply_text(f"Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ. Nᴏᴡ ᴘʀᴏᴄᴇssɪɴɢ ʏᴏᴜʀ {count} ғɪʟᴇ(s) ɪɴ sᴇǫᴜᴇɴᴄᴇ...!!")
        
        processing_tasks = []
        for index, file_info in enumerate(file_list, 1):
            # For each file in the sorted list, create a concurrent processing task
            # Pass the original message stored in file_info to auto_rename_file_concurrent
            task = asyncio.create_task(auto_rename_file_concurrent(client, file_info["message"], file_info))
            processing_tasks.append(task)
        
        # Wait for all processing tasks to complete
        # return_exceptions=True means if one task fails, others still run and you get all results/exceptions
        await asyncio.gather(*processing_tasks, return_exceptions=True) 
        
        await message.reply_text(f"✅ Aʟʟ {count} ғɪʟᴇs ᴘʀᴏᴄᴇssᴇᴅ (ᴏʀ ᴀᴛᴛᴇᴍᴘᴛᴇᴅ) sᴜᴄᴄᴇssғᴜʟʟʏ ɪɴ sᴇǫᴜᴇɴᴄᴇ!")

    try:
        # It's good practice to ensure `delete_messages` exists before trying to iterate
        if delete_messages:
            await client.delete_messages(chat_id=message.chat.id, message_ids=delete_messages)
    except Exception as e:
        print(f"Error deleting messages: {e}")

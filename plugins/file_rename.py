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
upload_semaphore = asyncio.Semaphore(3)    # Allow 3 concurrent uploads
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
        # E01, EP01 (E/EP then digits, standalone or in brackets)
        re.compile(r'(?:[EePp])(\d+)', re.IGNORECASE),
        # Simple digit after common separators for episodes (e.g., - 01)
        re.compile(r'[\s._-][EePp]?(\d+)(?:\s|\.|$)', re.IGNORECASE),
        # Matches "1 of 10" or similar episode count structures
        re.compile(r'\b(\d+)\s*of\s*\d+\b', re.IGNORECASE),
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue # Skip if conversion fails
            
    return None # Return None if no specific episode pattern is found

def extract_season_number(filename):
    """
    Extracts season number from filename.
    Prioritizes specific patterns (SXXEXX, Season XX, SXX).
    """
    season_patterns = [
        # S01E01, S01.EP01, S01-E01 (S and digits then optional separator then E/EP and digits)
        re.compile(r'S(\d+)(?:[.-]?|_)?[EePp]\d+', re.IGNORECASE),
        # Season 1, Season 01
        re.compile(r'Season\s*(\d+)', re.IGNORECASE),
        # S1, S01 (standalone, followed by non-digit or end of string)
        re.compile(r'\bS(\d+)(?:\D|$)', re.IGNORECASE), 
    ]
    
    for pattern in season_patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue # Skip if conversion fails
            
    return None # Return None if no specific season pattern is found

def extract_audio_info(filename):
    """Extract audio information from filename, including languages and 'dual'/'multi'."""
    audio_keywords = {
        'Hindi': re.compile(r'Hindi', re.IGNORECASE),
        'English': re.compile(r'English', re.IGNORECASE),
        'Multi': re.compile(r'Multi(?:audio)?', re.IGNORECASE),
        'Telugu': re.compile(r'Telugu', re.IGNORECASE),
        'Tamil': re.compile(r'Tamil', re.IGNORECASE),
        'Dual': re.compile(r'Dual(?:audio)?', re.IGNORECASE), 
        # Add more specific codecs/channels if needed
        'AAC': re.compile(r'AAC', re.IGNORECASE),
        'AC3': re.compile(r'AC3', re.IGNORECASE),
        'DTS': re.compile(r'DTS', re.IGNORECASE),
        'MP3': re.compile(r'MP3', re.IGNORECASE),
        '5.1': re.compile(r'5\.1', re.IGNORECASE),
        '2.0': re.compile(r'2\.0', re.IGNORECASE),
    }
    
    detected_audio = []
    
    # Check for 'Dual' first without strict word boundary if it's combined with quality
    # This specifically addresses the '480pDUAL' case.
    if re.search(r'pDual(?:audio)?', filename, re.IGNORECASE):
        detected_audio.append("Dual")

    # Prioritize language/type keywords (excluding 'Dual' which was handled above if combined)
    priority_keywords = ['Hindi', 'English', 'Multi', 'Telugu', 'Tamil']
    
    for keyword in priority_keywords:
        if audio_keywords[keyword].search(filename):
            detected_audio.append(keyword)
            
    # Also check for standalone 'Dual' if not already added
    if "Dual" not in detected_audio and audio_keywords['Dual'].search(filename):
        detected_audio.append("Dual")


    # Add other codecs/channels if not already covered
    for keyword, pattern in audio_keywords.items():
        if keyword not in priority_keywords and keyword != 'Dual' and pattern.search(filename):
            detected_audio.append(keyword)
            
    # Remove duplicates while preserving order
    detected_audio = list(dict.fromkeys(detected_audio))
    
    if detected_audio:
        return ' '.join(detected_audio)
        
    return None # Return None if nothing specific is found

def extract_quality(filename):
    """Extract video quality from filename."""
    patterns = [
        re.compile(r'\b(4K|2K|2160p|1440p|1080p|720p|480p|360p)\b', re.IGNORECASE),
        re.compile(r'\b(HD(?:RIP)?|WEB(?:-)?DL|BLURAY)\b', re.IGNORECASE), # e.g., HdRip, WEBDL
        re.compile(r'\b(X264|X265|HEVC)\b', re.IGNORECASE), # Codecs as quality indicator
    ]

    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            # Capitalize first letter for common formats, keep original for codecs
            found_quality = match.group(1)
            if found_quality.lower() in ["4k", "2k", "hdrip", "web-dl", "bluray"]:
                return found_quality.upper() if found_quality.upper() in ["4K", "2K"] else found_quality.capitalize()
            return found_quality # Return as found for codecs
            
    return None # Return None if no specific quality pattern is found

# --- Enhanced filename generation with UUID for uniqueness ---
def generate_unique_paths(renamed_file_name):
    """Generate unique file paths to avoid conflicts"""
    unique_id = str(uuid.uuid4())[:8]
    base_name, ext = os.path.splitext(renamed_file_name)
    
    # Ensure extension starts with a dot if not present
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
        file_list.sort(key=lambda x: x["episode_num"] if x["episode_num"] is not None else float('inf')) # Sort with None at end
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
                    duration=0, # Pyrogram will automatically calculate duration
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", progress_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0, # Pyrogram will automatically calculate duration
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
            # Auto-detect media type based on extension if not explicitly set by user preference
            if file_name.endswith((".mp4", ".mkv", ".avi", ".webm")):
                media_type = "video"
            elif file_name.endswith((".mp3", ".flac", ".wav", ".ogg")):
                media_type = "audio"
            # Otherwise, it defaults to "document"

            # NSFW check
            if await check_anti_nsfw(file_name, message):
                await message.reply_text("NSFW ᴄᴏɴᴛᴇɴᴛ ᴅᴇᴛᴇᴄᴛᴇᴅ. Fɪʟᴇ ᴜᴘʟᴏᴀᴅ ʀᴇᴊᴇᴄᴛᴇᴅ.")
                return

            # Extract information from filename
            episode_number = extract_episode_number(file_name)
            season_number = extract_season_number(file_name)
            audio_info_extracted = extract_audio_info(file_name)  
            quality_extracted = extract_quality(file_name)

            print(f"Extracted Episode Number: {episode_number}")
            print(f"Extracted Season Number: {season_number}")
            print(f"Extracted Audio Info: {audio_info_extracted}")
            print(f"Extracted Quality: {quality_extracted}")

            template = format_template
            
            # --- Placeholder Replacement Logic ---
            
            # 1. Specific 'SSeason' placeholder handling
            season_value_formatted = str(season_number).zfill(2) if season_number is not None else ""
            if "SSeason" in template:
                if season_value_formatted:
                    template = template.replace("SSeason", f"S{season_value_formatted}")
                else:
                    template = template.replace("SSeason", "") # Remove SSeason if no season found

            # 2. Specific 'EP{episode}' placeholder handling
            episode_value_formatted = str(episode_number).zfill(2) if episode_number is not None else ""
            if "EP{episode}" in template:
                if episode_value_formatted:
                    template = template.replace("EP{episode}", f"EP{episode_value_formatted}")
                else:
                    template = template.replace("EP{episode}", "") # Remove EP{episode} if no episode found

            # 3. Generic Season placeholder replacement (only for {season} or bare season)
            # This regex now specifically matches "{Season}" or "Season" (case-insensitive) as a whole word.
            season_generic_placeholder_regex = re.compile(r'\b(?:\{Season\}|Season)\b', re.IGNORECASE)
            def season_generic_replacer(match):
                if season_value_formatted:
                    return f"{season_value_formatted}"
                else:
                    return "" 
            template = season_generic_placeholder_regex.sub(season_generic_replacer, template)

            # 4. Generic Episode placeholder replacement (only for {episode} or bare episode)
            # This regex now specifically matches "{Episode}" or "Episode" (case-insensitive) as a whole word.
            episode_generic_placeholder_regex = re.compile(r'\b(?:\{Episode\}|Episode)\b', re.IGNORECASE)
            def episode_generic_replacer(match):
                if episode_value_formatted:
                    return f"{episode_value_formatted}"
                else:
                    return ""
            template = episode_generic_placeholder_regex.sub(episode_generic_replacer, template)

            # 5. Audio placeholder replacement (only for {audio} or bare audio)
            # This regex now specifically matches "{Audio}" or "Audio" (case-insensitive) as a whole word.
            replacement_audio = audio_info_extracted if audio_info_extracted else ""
            audio_placeholder_regex = re.compile(r'\b(?:\{Audio\}|Audio)\b', re.IGNORECASE)
            def audio_replacer(match):
                if replacement_audio:
                    return f"{replacement_audio}"
                else:
                    return ""
            template = audio_placeholder_regex.sub(audio_replacer, template)

            # 6. Quality placeholder replacement (only for {quality} or bare quality)
            # This regex now specifically matches "{Quality}" or "Quality" (case-insensitive) as a whole word.
            replacement_quality = quality_extracted if quality_extracted else ""
            quality_placeholder_regex = re.compile(r'\b(?:\{Quality\}|Quality)\b', re.IGNORECASE)
            def quality_replacer(match):
                if replacement_quality:
                    return f"{replacement_quality}"
                else:
                    return ""
            template = quality_placeholder_regex.sub(quality_replacer, template)

            # Clean up extra spaces or hyphens left by removed placeholders
            template = re.sub(r'\s{2,}', ' ', template) # Replace multiple spaces with a single space
            template = re.sub(r'\[\s*-\s*\]', '', template) # Remove empty bracket hyphen like "[ - ]"
            template = template.strip() # Remove leading/trailing whitespace

            _, file_extension = os.path.splitext(file_name)
            # Ensure the extension starts with a dot
            if not file_extension.startswith('.'):
                file_extension = '.' + file_extension if file_extension else ''
            
            renamed_file_name = f"{template}{file_extension}"
            
            # Generate unique paths to avoid conflicts
            renamed_file_path, metadata_file_path, unique_file_name = generate_unique_paths(renamed_file_name)

            # Start download with status message
            download_msg = await message.reply_text("Wᴇᴡ... Iᴀᴍ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")

            ph_path = None # Initialize ph_path

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
                        duration=convert(0), # Duration will be auto-calculated by Pyrogram on upload
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
                        except Exception as cleanup_e:
                            print(f"Error during file cleanup for {file_path}: {cleanup_e}")
                            pass # Log error but don't stop process
                
                # Remove from operations tracking
                if file_id in renaming_operations:
                    del renaming_operations[file_id]

        except Exception as e:
            if 'file_id' in locals() and file_id in renaming_operations:
                del renaming_operations[file_id]
            print(f"Concurrent rename outer error: {e}")

# Keep the original function for sequence mode (it now calls the concurrent one)
async def auto_rename_file(client, message, file_info, is_sequence=False, status_msg=None):
    """Original function for sequence mode - kept for compatibility"""
    return await auto_rename_file_concurrent(client, message, file_info)

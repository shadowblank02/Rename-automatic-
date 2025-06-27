import os
import re
import time
import shutil
import asyncio
import logging # Added for logging
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.types import Message
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Task queue for real concurrent auto renaming ---
class TaskQueue:
    def __init__(self, concurrency=3):
        self.semaphore = asyncio.Semaphore(concurrency)
        logger.info(f"TaskQueue initialized with concurrency: {concurrency}")

    async def add(self, coro):
        # Launch each task as a background task, limited by the semaphore
        asyncio.create_task(self.worker(coro))

    async def worker(self, coro):
        async with self.semaphore:
            try:
                await coro
            except Exception as e:
                logger.error(f"Task error: {e}", exc_info=True) # Log full traceback

task_queue = TaskQueue(concurrency=3)  # adjust as needed

from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

active_sequences = {}
message_ids = {}
renaming_operations = {}

# --- Task queue for real concurrent auto renaming ---
class TaskQueue:
    def init(self, concurrency=3):
        self.semaphore = asyncio.Semaphore(concurrency)

    async def add(self, coro):
        # Launch each task as a background task, limited by the semaphore
        asyncio.create_task(self.worker(coro))

    async def worker(self, coro):
        async with self.semaphore:
            try:
                await coro
            except Exception as e:
                print(f"Task error: {e}")

task_queue = TaskQueue(concurrency=3)  # adjust as needed

def detect_quality(file_name):
    quality_order = {"480p": 1, "720p": 2, "1080p": 3}
    match = re.search(r"(480p|720p|1080p)", file_name)
    return quality_order.get(match.group(1), 4) if match else 4  # Default priority = 4

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
    file_info = {"file_id": file_id, "file_name": file_name if file_name else "Unknown"}

    if user_id in active_sequences:
        active_sequences[user_id].append(file_info)
        await message.reply_text("Wᴇᴡ...ғɪʟᴇs ʀᴇᴄᴇɪᴠᴇᴅ ɴᴏᴡ ᴜsᴇ /end_sequence ᴛᴏ ɢᴇᴛ ʏᴏᴜʀ ғɪʟᴇs...!!")
        return

    # Not in sequence: add to concurrent task queue for auto renaming
    await task_queue.add(auto_rename_file(client, message, file_info))

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
        await message.reply_text(f"Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ.Nᴏᴡ sᴇɴᴅɪɴɢ ʏᴏᴜʀ {count} ғɪʟᴇ(s) Bᴀᴄᴋ ɪɴ ᴀ sᴇǫᴜᴇɴᴄᴇ...!!")
        # Send all files back in the order they were received
        for file in file_list:
            try:
                await client.send_document(
                    message.chat.id,
                    file["file_id"],
                    caption=file.get("file_name", "")
                )
            except Exception as e:
                await message.reply_text(f"Fᴀɪʟᴇᴅ ᴛᴏ sᴇɴᴅ ғɪʟᴇ: {file.get('file_name', '')}\n{e}")

    try:
        await client.delete_messages(chat_id=message.chat.id, message_ids=delete_messages)
    except Exception as e:
        print(f"Error deleting messages: {e}")

pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
patternX = re.compile(r'(\d+)')
pattern5 = re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE)
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

def extract_episode_number(filename):
    match = re.search(pattern1, filename)
    if match:
        return match.group(2)
    match = re.search(pattern2, filename)
    if match:
        return match.group(2)
    match = re.search(pattern3, filename)
    if match:
        return match.group(1)
    match = re.search(pattern3_2, filename)
    if match:
        return match.group(1)
    match = re.search(pattern4, filename)
    if match:
        return match.group(2)
    match = re.search(patternX, filename)
    if match:
        return match.group(1)
    return None

async def process_thumb(ph_path):
    # Offload PIL image work to a thread for real concurrency
    def _resize_thumb(path):
        try:
            img = Image.open(path).convert("RGB")
            img = img.resize((320, 320))
            img.save(path, "JPEG")
            logger.info(f"Thumbnail resized: {path}")
        except Exception as e:
            logger.error(f"Error resizing thumbnail {path}: {e}", exc_info=True)
    await asyncio.to_thread(_resize_thumb, ph_path)

async def auto_rename_file_single(client, message, file_info):
    user_id = message.from_user.id
    file_id = file_info["file_id"]
    file_name = file_info["file_name"]
    downloaded_file_path = None
    metadata_output_path = None
    ph_path = None # Thumbnail path

    try:
        format_template = await codeflixbots.get_format_template(user_id)
        media_preference = await codeflixbots.get_media_preference(user_id)

        if not format_template:
            logger.warning(f"User {user_id} has no rename format set.")
            return await message.reply_text(
                "Please Set An Auto Rename Format First Using /autorename"
            )

        media_type = media_preference or "document"
        if file_name.endswith(".mp4"):
            media_type = "video"
        elif file_name.endswith(".mp3"):
            media_type = "audio"

        if await check_anti_nsfw(file_name, message):
            logger.warning(f"NSFW content detected for file {file_name} from user {user_id}.")
            return await message.reply_text("NSFW content detected. File upload rejected.")

        if file_id in renaming_operations:
            elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
            if elapsed_time < 10: # Prevent reprocessing too quickly
                logger.info(f"File {file_name} for user {user_id} is already being processed or was recently processed. Skipping.")
                return

        renaming_operations[file_id] = datetime.now()
        logger.info(f"Starting auto-rename for file {file_name} from user {user_id}.")

        episode_number = extract_episode_number(file_name)
        logger.info(f"Extracted Episode Number: {episode_number} for {file_name}")

        template = format_template
        if episode_number:
            placeholders = ["episode", "Episode", "EPISODE", "{episode}"]
            for placeholder in placeholders:
                template = template.replace(placeholder, str(episode_number), 1)
        
        quality_placeholders = ["quality", "Quality", "QUALITY", "{quality}"]
        for quality_placeholder in quality_placeholders:
            if quality_placeholder in template:
                extracted_qualities = extract_quality(file_name)
                if extracted_qualities == "Unknown":
                    logger.warning(f"Could not extract quality for {file_name}. Renaming as 'Unknown'.")
                    await message.reply_text("I Was Not Able To Extract The Quality Properly. Renaming As 'Unknown'...")
                    del renaming_operations[file_id]
                    return
                template = template.replace(quality_placeholder, "".join(extracted_qualities))

        _, file_extension = os.path.splitext(file_name)
        renamed_file_name = f"{template}{file_extension}"
        downloaded_file_path = f"downloads/{renamed_file_name}"
        metadata_output_path = f"Metadata/{renamed_file_name}" # This will be the final path if metadata is added
        
        os.makedirs(os.path.dirname(downloaded_file_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_output_path), exist_ok=True)

        download_msg = await message.reply_text("Wᴇᴡ... Iᴀᴍ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")
        logger.info(f"Downloading {file_name} to {downloaded_file_path}")

        try:
            path = await client.download_media(
                message,
                file_name=downloaded_file_path,
                progress=progress_for_pyrogram,
                progress_args=("Dᴏᴡɴʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ....!!", download_msg, time.time()),
            )
            logger.info(f"Successfully downloaded {file_name} to {path}")
        except Exception as e:
            logger.error(f"Download Error for {file_name}: {e}", exc_info=True)
            del renaming_operations[file_id]
            return await download_msg.edit(f"Download Error: {e}")

        # --- Metadata Injection ---
        await download_msg.edit("Nᴏᴡ ᴀᴅᴅɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ ᴅᴜᴅᴇ...!!")
        logger.info(f"Adding metadata to {path}")

        ffmpeg_cmd = shutil.which('ffmpeg')
        if not ffmpeg_cmd:
            logger.error("ffmpeg not found in system PATH.")
            await download_msg.edit("FFmpeg is not installed or not in PATH. Cannot add metadata.")
            del renaming_operations[file_id]
            return

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
            '-y', # Overwrite output files without asking
            metadata_output_path
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *metadata_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                error_message = stderr.decode()
                logger.error(f"FFmpeg Metadata Error for {file_name}: {error_message}")
                await download_msg.edit(f"Metadata Error:\n{error_message}\nProceeding with original file.")
                # If metadata failed, upload the original downloaded file
                final_upload_path = path
            else:
                logger.info(f"Successfully added metadata to {file_name}.")
                final_upload_path = metadata_output_path
            
        except Exception as e:
            logger.error(f"Error during metadata processing for {file_name}: {e}", exc_info=True)
            await download_msg.edit(f"Metadata Processing Error: {e}\nProceeding with original file.")
            final_upload_path = path # Fallback to original downloaded file if metadata failed

        # --- Uploading ---
        upload_msg = await download_msg.edit("Wᴇᴡ... Iᴀᴍ Uᴘʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇ...!!")
        logger.info(f"Starting upload for {renamed_file_name}")

        c_caption = await codeflixbots.get_caption(message.chat.id)
        c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

        caption = (
            c_caption.format(
                filename=renamed_file_name,
                filesize=humanbytes(message.document.file_size if message.document else message.video.file_size if message.video else message.audio.file_size), # Use correct file_size
                duration=convert(message.video.duration if message.video else message.audio.duration if message.audio else 0), # Use actual duration
            )
            if c_caption
            else f"{renamed_file_name}"
        )

        if c_thumb:
            try:
                ph_path = await client.download_media(c_thumb)
                logger.info(f"Downloaded custom thumbnail {c_thumb} to {ph_path}")
            except Exception as e:
                logger.warning(f"Could not download custom thumbnail {c_thumb}: {e}")
                ph_path = None
        elif media_type == "video" and getattr(message.video, "thumbs", None):
            try:
                ph_path = await client.download_media(message.video.thumbs[0].file_id)
                logger.info(f"Downloaded video thumbnail to {ph_path}")
            except Exception as e:
                logger.warning(f"Could not download video thumbnail: {e}")
                ph_path = None
        
        if ph_path:
            await process_thumb(ph_path)

        try:
            if media_type == "document":
                await client.send_document(
                    message.chat.id,
                    document=final_upload_path,
                    thumb=ph_path,
                    caption=caption,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
            elif media_type == "video":
                await client.send_video(
                    message.chat.id,
                    video=final_upload_path,
                    caption=caption,
                    thumb=ph_path,
                    duration=message.video.duration if message.video else 0, # Used actual duration
                    width=message.video.width if message.video else 0,
                    height=message.video.height if message.video else 0,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=final_upload_path,
                    caption=caption,
                    thumb=ph_path,
                    duration=message.audio.duration if message.audio else 0, # Used actual duration
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
            logger.info(f"Successfully uploaded {renamed_file_name} to user {user_id}.")
        except Exception as e:
            logger.error(f"Upload Error for {renamed_file_name}: {e}", exc_info=True)
            return await upload_msg.edit(f"Upload Error: {e}")

        await download_msg.delete() # Delete the "downloading" message
        await upload_msg.delete() # Delete the "uploading" message
        
    except Exception as e:
        logger.critical(f"An unhandled error occurred in auto_rename_file_single for user {user_id}, file {file_name}: {e}", exc_info=True)
        # Attempt to inform the user about the critical error
        try:
            await message.reply_text(f"An unexpected error occurred during processing: {e}")
        except Exception as reply_e:
            logger.error(f"Failed to send error message to user {user_id}: {reply_e}")
    finally:
        # Cleanup downloaded and processed files
        if downloaded_file_path and os.path.exists(downloaded_file_path):
            os.remove(downloaded_file_path)
            logger.debug(f"Cleaned up downloaded file: {downloaded_file_path}")
        if metadata_output_path and os.path.exists(metadata_output_path):
            os.remove(metadata_output_path)
            logger.debug(f"Cleaned up metadata output file: {metadata_output_path}")
        if ph_path and os.path.exists(ph_path):
            os.remove(ph_path)
            logger.debug(f"Cleaned up thumbnail file: {ph_path}")
        
        # Ensure renaming_operations entry is removed
        if file_id in renaming_operations:
            del renaming_operations[file_id]
            logger.debug(f"Removed {file_id} from renaming_operations.")

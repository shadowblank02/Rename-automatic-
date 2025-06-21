import os
import re
import time
import shutil
import asyncio
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

renaming_operations = {}
active_sequences = {}
message_ids = {}

# ----------- BATCH RENAME ADDITIONS --------------
BATCH_SIZE = 3  # Change this to your desired batch size (2, 3, etc.)
user_file_queue = {}  # user_id: list of (message, file_info)

async def process_files_in_batch(client, user_id, files):
    tasks = []
    for message, file_info in files:
        tasks.append(auto_rename_file(client, message, file_info))
    await asyncio.gather(*tasks)

async def auto_rename_file(client, message, file_info):
    user_id = message.from_user.id
    file_id = file_info["file_id"]
    file_name = file_info["file_name"]

    format_template = await codeflixbots.get_format_template(user_id)
    media_preference = await codeflixbots.get_media_preference(user_id)

    if not format_template:
        return await message.reply_text(
            "Please Set An Auto Rename Format First Using /autorename"
        )

    media_type = media_preference or "document"
    if file_name.endswith(".mp4"):
        media_type = "video"
    elif file_name.endswith(".mp3"):
        media_type = "audio"

    if await check_anti_nsfw(file_name, message):
        return await message.reply_text("NSFW content detected. File upload rejected.")

    if file_id in renaming_operations:
        elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
        if elapsed_time < 10:
            return

    renaming_operations[file_id] = datetime.now()

    episode_number = extract_episode_number(file_name)
    print(f"Extracted Episode Number: {episode_number}")

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
                    await message.reply_text("I Was Not Able To Extract The Quality Properly. Renaming As 'Unknown'...")
                    del renaming_operations[file_id]
                    return
                template = template.replace(quality_placeholder, "".join(extracted_qualities))

    _, file_extension = os.path.splitext(file_name)
    renamed_file_name = f"{template}{file_extension}"
    renamed_file_path = f"downloads/{renamed_file_name}"
    metadata_file_path = f"Metadata/{renamed_file_name}"
    os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)

    download_msg = await message.reply_text("Wᴇᴡ... Iᴀᴍ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇs...!!")

    ph_path = None

    try:
        path = await client.download_media(
            message,
            file_name=renamed_file_path,
            progress=progress_for_pyrogram,
            progress_args=("Dᴏᴡɴʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ....!!", download_msg, time.time()),
        )
    except Exception as e:
        del renaming_operations[file_id]
        return await download_msg.edit(f"Download Error: {e}")

    await download_msg.edit("Nᴏᴡ ᴀᴅᴅɪɴɢ ᴍᴇᴛᴀᴅᴀᴛᴀ ᴅᴜᴅᴇ...!!")

    # --------- METADATA SECTION WITH FIX --------------
    ffmpeg_cmd = shutil.which('ffmpeg')
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

    try:
        process = await asyncio.create_subprocess_exec(
            *metadata_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            error_message = stderr.decode()
            await download_msg.edit(f"Metadata Error:\n{error_message}")
            del renaming_operations[file_id]
            return

        path = metadata_file_path

        upload_msg = await download_msg.edit("Wᴇᴡ... Iᴀᴍ Uᴘʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ғɪʟᴇs...!!")

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
            img = Image.open(ph_path).convert("RGB")
            img = img.resize((320, 320))
            img.save(ph_path, "JPEG")

        try:
            if media_type == "document":
                await client.send_document(
                    message.chat.id,
                    document=path,
                    thumb=ph_path,
                    caption=caption,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
            elif media_type == "video":
                await client.send_video(
                    message.chat.id,
                    video=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Uᴘʟᴏᴀᴅ sᴛᴀʀᴛᴇᴅ ᴅᴜᴅᴇ...!!", upload_msg, time.time()),
                )
        except Exception as e:
            if os.path.exists(renamed_file_path):
                os.remove(renamed_file_path)
            if ph_path and os.path.exists(ph_path):
                os.remove(ph_path)
            del renaming_operations[file_id]
            return await upload_msg.edit(f"Error: {e}")

        await download_msg.delete()
        if os.path.exists(path):
            os.remove(path)
        if ph_path and os.path.exists(ph_path):
            os.remove(ph_path)

        # Clean up
        if os.path.exists(renamed_file_path):
            os.remove(renamed_file_path)
        if os.path.exists(metadata_file_path):
            os.remove(metadata_file_path)
        if ph_path and os.path.exists(ph_path):
            os.remove(ph_path)
        if file_id in renaming_operations:
            del renaming_operations[file_id]

    except Exception as e:
        del renaming_operations[file_id]
        return await download_msg.edit(f"Metadata/Processing Error: {e}")

# ------------ END BATCH RENAME ADDITIONS ------------

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
        msg = await message.reply_text("Sᴇǫᴜᴇɴᴄᴇ sᴛᴀʀᴛᴇᴅ! Sᴇɴᴅ ʏᴏᴜʀ ғɪʟᴇs Nᴏᴡ ʙʀᴏ....Fᴀsᴛ")
        message_ids[user_id].append(msg.message_id)

@Client.on_message(filters.command("end_sequence") & filters.private)
async def end_sequence(client, message: Message):
    user_id = message.from_user.id
    if user_id not in active_sequences:
        await message.reply_text("Wʜᴀᴛ ᴀʀᴇ ʏᴏᴜ ᴅᴏɪɴɢ ɴᴏ ᴀᴄᴛɪᴠᴇ sᴇǫᴜᴇɴᴄᴇ ғᴏᴜɴᴅ...!!")
        return

    file_list = active_sequences.pop(user_id, [])
    delete_messages = message_ids.pop(user_id, [])

    if not file_list:
        await message.reply_text("Nᴏ ғɪʟᴇs ᴡᴇʀᴇ sᴇɴᴛ ɪɴ ᴛʜɪs sᴇǫᴜᴇɴᴄᴇ....ʙʀᴏ...!!")
        return

    sorted_files = sorted(file_list, key=lambda f: (
        detect_quality(f["file_name"]) if "file_name" in f else 4,
        f["file_name"] if "file_name" in f else ""
    ))

    await message.reply_text(f"Sᴇǫᴜᴇɴᴄᴇ ᴇɴᴅᴇᴅ ɴᴏᴡ sᴇɴᴅɪɴɢ ʏᴏᴜʀ {len(sorted_files)} Fɪʟᴇs ʙᴀᴄᴋ...Sᴏ ᴡᴀɪᴛ...!!")

    for file in sorted_files:
        await client.send_document(message.chat.id, file["file_id"], caption=f"{file.get('file_name', '')}",)

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

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    user_id = message.from_user.id
    file_id = message.document.file_id if message.document else message.video.file_id if message.video else message.audio.file_id
    file_name = message.document.file_name if message.document else message.video.file_name if message.video else message.audio.file_name
    file_info = {"file_id": file_id, "file_name": file_name if file_name else "Unknown"}

    # ----------- BATCH MODE -----------
    if user_id not in user_file_queue:
        user_file_queue[user_id] = []
    user_file_queue[user_id].append((message, file_info))

    if len(user_file_queue[user_id]) >= BATCH_SIZE:
        batch = user_file_queue[user_id][:BATCH_SIZE]
        user_file_queue[user_id] = user_file_queue[user_id][BATCH_SIZE:]
        asyncio.create_task(process_files_in_batch(client, user_id, batch))
        await message.reply_text(f"Batch of {BATCH_SIZE} files is being auto-renamed now! 🚀")
    else:
        await message.reply_text(f"File received. Waiting for {BATCH_SIZE - len(user_file_queue[user_id])} more files to start batch auto-rename.")

    # If you want to keep the /sequence feature, keep the following block as well:
    if user_id in active_sequences:
        file_info = {
            "file_id": file_id,
            "file_name": file_name if file_name else "Unknown"
        }
        active_sequences[user_id].append(file_info)
        await message.reply_text(f"Wᴇᴡ...Fɪʟᴇ ʀᴇᴄᴇɪᴠᴇᴅ ɪɴ sᴇǫᴜᴇɴᴄᴇ...Nᴏᴡ ᴜsᴇ /end_sequence....Dᴜᴅᴇ...!!")
        return

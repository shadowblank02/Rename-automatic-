from config import Config, Txt
from helper.database import codeflixbots
from pyrogram.types import Message
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
import os, sys, time, asyncio, logging, datetime
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from functools import wraps

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ADMIN_USER_ID = Config.ADMIN

# Flag to indicate if the bot is restarting
is_restarting = False

# --- Ban Check Decorator ---
def check_ban(func):
    @wraps(func)
    async def wrapper(client, message, *args, **kwargs):
        user_id = message.from_user.id
        user = await codeflixbots.col.find_one({"_id": user_id})
        if user and user.get("ban_status", {}).get("is_banned", False):
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("📩 Contact Admin", url=ADMIN_URL)]]
            )
            return await message.reply_text(
                "🚫 You are banned from using this bot.\n\nIf you think this is a mistake, contact the admin.",
                reply_markup=keyboard
            )
        return await func(client, message, *args, **kwargs)
    return wrapper

@Client.on_message(filters.private & filters.command("restart") & filters.user(ADMIN_USER_ID))
async def restart_bot(b, m):
    global is_restarting
    if not is_restarting:
        is_restarting = True
        await m.reply_text("**Hᴇʏ...!! Oᴡɴᴇʀ/Aᴅᴍɪɴ Jᴜsᴛ ʀᴇʟᴀx ɪᴀᴍ ʀᴇsᴛᴀʀᴛɪɴɢ...!!**")
        # Gracefully stop the bot's event loop
        b.stop()
        time.sleep(2)
        # Restart the bot process
        os.execl(sys.executable, sys.executable, *sys.argv)
        

@Client.on_message(filters.private & filters.command(["tutorial"]))
async def tutorial(bot, message):
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    await message.reply_text(
        text=Txt.FILE_NAME_TXT.format(format_template=format_template),
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("•Sᴜᴘᴘᴏʀᴛ•", url="https://t.me/BOTSKINGDOMSGROUP"), InlineKeyboardButton("•⚡Main hub•", url="https://t.me/botskingdoms")]
        ])
    )

@Client.on_message(filters.command(["stats", "status"]) & filters.user(Config.ADMIN))
async def get_stats(bot, message):
    total_users = await codeflixbots.total_users_count()
    uptime = time.strftime("%Hh%Mm%Ss", time.gmtime(time.time() - bot.uptime))
    start_t = time.time()
    st = await message.reply('**Accessing The Details.....**')
    end_t = time.time()
    time_taken_s = (end_t - start_t) * 1000
    await st.edit(text=f"**--Bot Status--** \n\n**⌚️ Bot Uptime :** {uptime} \n**🐌 Current Ping :** `{time_taken_s:.3f} ms` \n**👭 Total Users :** `{total_users}`")

@Client.on_message(filters.command("broadcast") & filters.user(Config.ADMIN) & filters.reply)
async def broadcast_handler(bot: Client, m: Message):
    await bot.send_message(Config.LOG_CHANNEL, f"{m.from_user.mention} or {m.from_user.id} Is Started The Broadcast......")
    all_users = await codeflixbots.get_all_users()
    broadcast_msg = m.reply_to_message
    sts_msg = await m.reply_text("Broadcast Started..!") 
    done = 0
    failed = 0
    success = 0
    start_time = time.time()
    total_users = await codeflixbots.total_users_count()
    async for user in all_users:
        sts = await send_msg(user['_id'], broadcast_msg)
        if sts == 200:
           success += 1
        else:
           failed += 1
        if sts == 400:
           await codeflixbots.delete_user(user['_id'])
        done += 1
        if not done % 20:
           await sts_msg.edit(f"Broadcast In Progress: \n\nTotal Users {total_users} \nCompleted : {done} / {total_users}\nSuccess : {success}\nFailed : {failed}")
    completed_in = datetime.timedelta(seconds=int(time.time() - start_time))
    await sts_msg.edit(f"Bʀᴏᴀᴅᴄᴀꜱᴛ Cᴏᴍᴩʟᴇᴛᴇᴅ: \nCᴏᴍᴩʟᴇᴛᴇᴅ Iɴ `{completed_in}`.\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nFailed: {failed}")
           
async def send_msg(user_id, message):
    try:
        await message.copy(chat_id=int(user_id))
        return 200
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return send_msg(user_id, message)
    except InputUserDeactivated:
        logger.info(f"{user_id} : Deactivated")
        return 400
    except UserIsBlocked:
        logger.info(f"{user_id} : Blocked The Bot")
        return 400
    except PeerIdInvalid:
        logger.info(f"{user_id} : User ID Invalid")
        return 400
    except Exception as e:
        logger.error(f"{user_id} : {e}")
        return 500

# --- Ban User Command ---
@Client.on_message(filters.command("ban"))
async def ban_user(bot, message):
    if message.from_user.id not in Config.ADMIN:
        return await message.reply_text("🚫 You are not authorized to use this command.\nOnly Admins can use `/ban`.", parse_mode="markdown")

    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 2 or not parts[1].isdigit():
            return await message.reply_text("❌ Usage: `/ban <user_id> <reason>`", parse_mode="markdown")
        
        user_id = int(parts[1])
        reason = parts[2] if len(parts) > 2 else "No reason provided"

        await codeflixbots.col.update_one(
            {"_id": user_id},
            {"$set": {
                "ban_status.is_banned": True,
                "ban_status.ban_reason": reason,
                "ban_status.banned_on": datetime.date.today().isoformat()
            }},
            upsert=True
        )
        await message.reply_text(f"✅ User `{user_id}` has been banned.\nReason: {reason}")
    except Exception as e:
        await message.reply_text(f"❌ Error while banning:\n{e}")

# --- Unban User Command ---
@Client.on_message(filters.command("unban"))
async def unban_user(bot, message):
    if message.from_user.id not in Config.ADMIN:
        return await message.reply_text("🚫 You are not authorized to use this command.\nOnly Admins can use `/unban`.", parse_mode="markdown")

    try:
        user_id = int(message.text.split()[1])
        await codeflixbots.col.update_one(
            {"_id": user_id},
            {"$set": {
                "ban_status.is_banned": False,
                "ban_status.ban_reason": "",
                "ban_status.banned_on": None
            }}
        )
        await message.reply_text(f"✅ User `{user_id}` has been unbanned.")
    except Exception as e:
        await message.reply_text(f"❌ Usage: `/unban <user_id>`\nError: {e}")

# --- Banned Users List ---
@Client.on_message(filters.command("banned"))
async def banned_list(bot, message):
    if message.from_user.id not in Config.ADMIN:
        return await message.reply_text("🚫 You are not authorized to use this command.\nOnly Admins can use `/banned`.", parse_mode="markdown")

    msg = await message.reply("🔄 Fetching banned users...")
    cursor = codeflixbots.col.find({"ban_status.is_banned": True})
    lines = []
    async for user in cursor:
        uid = user['_id']
        reason = user.get('ban_status', {}).get('ban_reason', '')
        try:
            user_obj = await bot.get_users(uid)
            name = user_obj.mention
        except PeerIdInvalid:
            name = f"`{uid}` (Name not found)"
        lines.append(f"👤 {name} - {reason}")

    if not lines:
        await msg.edit("✅ No users are currently banned.")
    else:
        if len(lines) >= 50:
            lines.append("...and more.")
        await msg.edit("🚫 **Banned Users:**\n\n" + "\n".join(lines[:50]), parse_mode="markdown")


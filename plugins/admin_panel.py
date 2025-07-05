from config import Config, Txt
from helper.database import codeflixbots
from pyrogram.types import Message
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
import os, sys, time, asyncio, logging, datetime
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ADMIN_USER_ID = Config.ADMIN

# Flag to indicate if the bot is restarting
is_restarting = False

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

@Client.on_message(filters.command("ban") & filters.user(Config.ADMIN))
async def ban_user(bot: Client, message: Message):
    try:
        args = message.text.split(maxsplit=2)
        if len(args) < 2:
            return await message.reply_text("**/ban @username/userid [reason]**")
        
        user_ref = args[1]
        reason = args[2] if len(args) > 2 else "No reason provided"

        if user_ref.startswith("@"):
            user = await codeflixbots.col.find_one({"username": user_ref[1:]})
        else:
            user = await codeflixbots.col.find_one({"_id": int(user_ref)})
        
        if not user:
            return await message.reply_text("**Usᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!**")
        
        await codeflixbots.col.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "ban_status.is_banned": True,
                "ban_status.banned_on": datetime.now(pytz.utc).isoformat(),
                "ban_status.ban_reason": reason
            }}
        )
        await message.reply_text(f"**🗸 Usᴇʀ {user['_id']} ʜᴀs ʙᴇᴇɴ ʙᴀɴɴᴇᴅ.**\n**Rᴇᴀsᴏɴ:** {reason}")
    except Exception as e:
        await message.reply_text(f"**/ban @username/userid [reason]**")

@Client.on_message(filters.command("unban") & filters.user(Config.ADMIN))
async def unban_user(bot: Client, message: Message):
    try:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            return await message.reply_text("**/unban @username/userid**")
        
        user_ref = args[1]

        if user_ref.startswith("@"):
            user = await codeflixbots.col.find_one({"username": user_ref[1:]})
        else:
            user = await codeflixbots.col.find_one({"_id": int(user_ref)})
        
        if not user:
            return await message.reply_text("**Wᴛғ Usᴇʀ ɴᴏᴛ ғᴏᴜɴᴅ!**")
        
        await codeflixbots.col.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "ban_status.is_banned": False,
                "ban_status.banned_on": None,
                "ban_status.ban_reason": None
            }}
        )
        await message.reply_text(f"**🗸 Usᴇʀ {user['_id']} ʜᴀs ʙᴇᴇɴ ᴜɴʙᴀɴɴᴇᴅ.**")
    except Exception as e:
        await message.reply_text(f"**/unban @username/userid**")

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

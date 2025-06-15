from pyrogram import Client, filters, enmus
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import UserNotParticipant
from config import Config
from helper.database import codeflixbots

async def not_subscribed(_, client, message):
    await codeflixbots.add_user(client, message)
    if not Config.FORCE_SUB:
        return False
    try:             
        user = await client.get_chat_member(Config.FORCE_SUB, message.from_user.id) 
        if user.status == enums.ChatMemberStatus.BANNED:
            return True 
        else:
            return False                
    except UserNotParticipant:
        pass
    return True


@Client.on_message(filters.private & filters.create(not_subscribed))
async def forces_sub(client, message):
    buttons = [
    [InlineKeyboardButton(text="•ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ•", url=f"https://t.me/{Config.FORCE_SUB}")]
]

text = "<b>Yᴏᴜ Bᴀᴋᴋᴀᴀ...!! \n<blockquote>Jᴏɪɴ ᴍʏ ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴜsᴇ ᴍʏ\n\nᴏᴛʜᴇʀᴡɪsᴇ Yᴏᴜ ᴀʀᴇ ɪɴ ʙɪɢ sʜɪᴛ...!!</blockquote>\nAғᴛᴇʀ Jᴏɪɴɪɴɢ Cʜᴀɴɴᴇʟ ᴄʟɪᴄᴋ ᴏɴ ᴄʟɪᴄᴋ ʜᴇʀᴇ </b>"

try:
    buttons.append([InlineKeyboardButton(text="Cʟɪᴄᴋ ʜᴇʀᴇ", url=f"https://t.me/{bot_username}?start")])
except Exception as e:
    print(f"An error occurred: {e}")

try:
    user = await client.get_chat_member(Config.FORCE_SUB, message.from_user.id)
    if user.status == enums.ChatMemberStatus.BANNED:
        return await client.send_message(message.from_user.id, text="Sorry You Are Banned To Use Me")
except UserNotParticipant:
    return await message.reply_text(text=text, reply_markup=InlineKeyboardMarkup(buttons))

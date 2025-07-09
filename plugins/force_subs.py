from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import UserNotParticipant
from config import Config
from helper.database import Botskingdom

async def not_subscribed(_, client, message):
    await Botskingdom.add_user(client, message)
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
        [InlineKeyboardButton(text="•ᴊᴏɪɴ ᴄʜᴀɴɴᴇʟ•", url=f"https://t.me/botskingdoms")]
    ]
    text = "<b>Yᴏᴜ Bᴀᴋᴋᴀᴀ...!! \n<blockquote>Jᴏɪɴ ᴍʏ ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴜsᴇ ᴍʏ\n\nᴏᴛʜᴇʀᴡɪsᴇ Yᴏᴜ ᴀʀᴇ ɪɴ ʙɪɢ sʜɪᴛ...!!</blockquote>\n<blockquote></b>"
    try:
        user = await client.get_chat_member(Config.FORCE_SUB, message.from_user.id)
        if user.status == enums.ChatMemberStatus.BANNED:
            return await client.send_message(message.from_user.id, text="Sorry You Are Banned To Use Me")
        buttons.append([InlineKeyboardButton(text="Cʟɪᴄᴋ ʜᴇʀᴇ", url=f"https://t.me/bot_kingdoms_auto_renamerbot?start=true")])
    except UserNotParticipant:
        if Config.FSUB_PIC:
            await message.reply_photo(
                Config.FSUB_PIC,
                caption=text,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            await message.reply_text(
                f"Yᴏᴜ ʙᴀᴋᴋᴀᴀ...!! Jᴏɪɴ ɪᴛ...",
                reply_markup=InlineKeyboardMarkup(buttons)
            )

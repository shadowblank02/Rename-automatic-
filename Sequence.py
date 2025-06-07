from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
import re

TOKEN = "7807784141:AAGFLYW5Iyl7Vi5deJ_xcGsxqYv8WoTPZVA"
user_file_sequences = {}

def start(update: Update, context: CallbackContext):
    update.message.reply_text("Welcome! Use /ssequence to start file sequencing, /esequence to finish, and /cancel to cancel.")

def start_sequence(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id in user_file_sequences:
        update.message.reply_text("You already have an active sequencing session. Use /esequence to complete it.")
        return
    user_file_sequences[user_id] = []
    update.message.reply_text("File sequencing started. Send documents and videos. Use /esequence to finish.")

def detect_quality(file_name):
    """ Detects video quality from filename """
    quality_order = {"480p": 1, "720p": 2, "1080p": 3}
    match = re.search(r"(480p|720p|1080p)", file_name)
    return quality_order.get(match.group(1), 4) if match else 4  # Default priority = 4

def process_file(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in user_file_sequences:
        update.message.reply_text("Start a sequence first using /ssequence.")
        return
    file = update.message.document or update.message.video
    if file:
        user_file_sequences[user_id].append(file)
        update.message.reply_text("File received and added to the sequence.")
    else:
        update.message.reply_text("Unsupported file type. Send documents or videos only.")

def end_sequence(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in user_file_sequences or not user_file_sequences[user_id]:
        update.message.reply_text("No files to sequence. Use /ssequence first.")
        return
    
    sorted_files = sorted(user_file_sequences[user_id], key=lambda f: (
        detect_quality(f.file_name) if hasattr(f, 'file_name') else 4,  # Sort by quality
        f.file_name if hasattr(f, 'file_name') else ""
    ))

    for file in sorted_files:
        if hasattr(file, 'file_id'):
            if hasattr(file, 'file_name') and file.file_name.endswith(('.mp4', '.mov', '.avi')):
                update.message.reply_video(file.file_id)
            else:
                update.message.reply_document(file.file_id)
    
    del user_file_sequences[user_id]
    update.message.reply_text("File sequencing completed.")

def cancel_sequence(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id in user_file_sequences:
        del user_file_sequences[user_id]
        update.message.reply_text("File sequencing process canceled.")
    else:
        update.message.reply_text("No active sequencing process to cancel.")

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("ssequence", start_sequence))
    dp.add_handler(MessageHandler(Filters.document | Filters.video, process_file))
    dp.add_handler(CommandHandler("esequence", end_sequence))
    dp.add_handler(CommandHandler("cancel", cancel_sequence))
    updater.start_polling()
    updater.idle()

if name == "__main__":
    main()

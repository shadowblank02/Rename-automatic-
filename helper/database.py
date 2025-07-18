import motor.motor_asyncio, datetime, pytz
from config import Config
import logging  # Added for logging errors and important information
from .utils import send_log


class Database:
    def __init__(self, uri, database_name):
        try:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self._client.server_info()  # This will raise an exception if the connection fails
            logging.info("Successfully connected to MongoDB")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise e  # Re-raise the exception after logging it
        self.Botskingdom = self._client[database_name]
        self.col = self.Botskingdom.user

    def new_user(self, id, username=None):
        return dict(
            _id=int(id),
            username=username.lower() if username else None,
            join_date=datetime.date.today().isoformat(),
            file_id=None,
            caption=None,
            metadata=True,
            metadata_code="Telegram : @botskingdoms",
            format_template=None,
            ban_status=dict(
                is_banned=False,
@@ -37,7 +36,7 @@ def new_user(self, id, username=None):
    async def add_user(self, b, m):
        u = m.from_user
        if not await self.is_user_exist(u.id):
            user = self.new_user(u.id, u.username)
            try:
                await self.col.insert_one(user)
                await send_log(b, u)
@@ -143,74 +142,59 @@ async def set_metadata(self, user_id, metadata):

    async def get_title(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('title', 'Bots kingdom')

    async def set_title(self, user_id, title):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'title': title}})

    async def get_author(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('author', 'Botskingdoms')

    async def set_author(self, user_id, author):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'author': author}})

    async def get_artist(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('artist', 'Botskingdoms')

    async def set_artist(self, user_id, artist):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'artist': artist}})

    async def get_audio(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('audio', 'Bots kingdom')

    async def set_audio(self, user_id, audio):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'audio': audio}})

    async def get_subtitle(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('subtitle', "Botskingdoms")

    async def set_subtitle(self, user_id, subtitle):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'subtitle': subtitle}})

    async def get_video(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('video', 'Botskingdoms')

    async def set_video(self, user_id, video):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'video': video}})

    async def get_encoded_by(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('encoded_by', "Botskingdoms")

    async def set_encoded_by(self, user_id, encoded_by):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'encoded_by': encoded_by}})

    async def get_custom_tag(self, user_id):
        user = await self.col.find_one({'_id': int(user_id)})
        return user.get('customtag', "Botskingdoms")

    async def set_custom_tag(self, user_id, custom_tag):
        await self.col.update_one({'_id': int(user_id)}, {'$set': {'custom_tag': custom_tag}})

    # Example methods to add in helper/database.py

    async def ban_user(user_id):
        await db.banned_users.update_one({"_id": user_id}, {"$set": {"_id": user_id}}, upsert=True)
 
    async def unban_user(user_id):
        await db.banned_users.delete_one({"_id": user_id})

    async def is_banned(user_id):
        return await db.banned_users.find_one({"_id": user_id}) is not None

    async def get_banned_users():
        return db.banned_users.find()



Botskingdom = Database(Config.DB_URL, Config.DB_NAME)

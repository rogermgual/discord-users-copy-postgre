import os
import asyncio
import discord
import asyncpg
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

intents = discord.Intents.default()
intents.members = True

class BulkUserFetcher(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.db_pool = None

    async def setup_database(self):
        """Starts the database connection pool"""
        self.db_pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT
        )

    async def on_ready(self):
        print(f"Bot conectado como {self.user}")
        guild = self.get_guild(GUILD_ID)

        if guild is None:
            print("Could not find the guild.")
            await self.close()
            return

        print(f"Obtain members from {guild.name}")
        await self.store_members_in_db(guild)
        print("Users stored in the database.")
        await self.close()

    async def store_members_in_db(self, guild):
        """Stores all the members of a guild in the database"""
        async with self.db_pool.acquire() as conn:
            async for member in guild.fetch_members(limit=None): 
                discord_id = member.id
                name_discord = member.name
                print(f"Procesando: {discord_id} - {name_discord}")

                # Insert or update the user
                await conn.execute("""
                    INSERT INTO users (discord_id, name_discord)
                    VALUES ($1, $2)
                    ON CONFLICT (discord_id) DO UPDATE SET name_discord = EXCLUDED.name_discord;
                """, discord_id, name_discord)

async def main():
    bot = BulkUserFetcher()
    await bot.setup_database()
    await bot.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())

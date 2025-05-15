import asyncio
from app.storage.postgres_manager import AnnouncementStorage
from config.settings import settings

async def init_database():
    storage = AnnouncementStorage(settings.DATABASE_URL)
    await storage.connect()
    
    # Create tables if not exist
    with open('schema.sql') as f:
        schema = f.read()
    
    async with storage.pool.acquire() as conn:
        await conn.execute(schema)
    
    print("Database initialized successfully")
    await storage.pool.close()

if __name__ == "__main__":
    asyncio.run(init_database())
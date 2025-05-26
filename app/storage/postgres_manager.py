import asyncpg
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union, Tuple
import hashlib
import base64
import json
import logging
from config.settings import settings

class AnnouncementStorage:
    def __init__(self):
        self.conn_string = settings.DATABASE_URL
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        """Initialize database connection pool and ensure tables exist"""
        self.pool = await asyncpg.create_pool(
            self.conn_string,
            min_size=5,
            max_size=20,
            command_timeout=60,
            server_settings={
                'application_name': 'news_analyzer',
                'statement_timeout': '30000'
            }
        )
        await self._ensure_tables_exist()

    async def _ensure_tables_exist(self):
        """Create database schema with proper constraints and indexes"""
        async with self.pool.acquire() as conn:
            # Create announcements table with all constraints
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS announcements (
                    id TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    announcement_id TEXT NOT NULL,
                    source_id TEXT,
                    url TEXT,
                    title TEXT NOT NULL,
                    content TEXT,
                    announcement_type TEXT,
                    tags JSONB,
                    publish_time TIMESTAMPTZ NOT NULL,
                    raw_data JSONB NOT NULL,
                    classification TEXT,
                    confidence FLOAT,
                    user_label TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    CONSTRAINT unique_exchange_announcement UNIQUE (exchange, announcement_id)
                )
            """)

            # Add updated_at column if it doesn't exist (for existing tables)
            await conn.execute("""
                ALTER TABLE announcements 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()
            """)

            # Create classifications table without the WHERE clause
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS announcement_classifications (
                    id SERIAL PRIMARY KEY,
                    announcement_id TEXT NOT NULL REFERENCES announcements(id) ON DELETE CASCADE,
                    label TEXT NOT NULL,
                    confidence FLOAT,
                    classified_at TIMESTAMPTZ DEFAULT NOW(),
                    is_user BOOLEAN DEFAULT FALSE
                )
            """)

            # Create all necessary indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_announcements_publish_time ON announcements(publish_time DESC)",
                "CREATE INDEX IF NOT EXISTS idx_announcements_exchange ON announcements(exchange)",
                "CREATE INDEX IF NOT EXISTS idx_announcements_user_label ON announcements(user_label)",
                "CREATE INDEX IF NOT EXISTS idx_announcements_exchange_publish ON announcements(exchange, publish_time DESC)",
                "CREATE INDEX IF NOT EXISTS idx_classifications_announcement_id ON announcement_classifications(announcement_id)",
                "CREATE INDEX IF NOT EXISTS idx_classifications_is_user ON announcement_classifications(is_user)",
                "CREATE INDEX IF NOT EXISTS idx_announcements_type ON announcements(announcement_type) WHERE announcement_type IS NOT NULL",
                # Add the partial unique index separately
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_non_user_classification 
                ON announcement_classifications(announcement_id, is_user) 
                WHERE NOT is_user"""
            ]
            
            for index in indexes:
                try:
                    await conn.execute(index)
                except Exception as e:
                    self.logger.warning(f"Failed to create index: {e}")
                    
    async def _parse_datetime(self, dt_value: Union[int, str, datetime]) -> datetime:
        """Robust datetime parser that handles multiple input formats"""
        if dt_value is None:
            raise ValueError("Datetime cannot be None")
        
        try:
            if isinstance(dt_value, int):
                # Handle both seconds and milliseconds
                if dt_value > 1e12:  # Likely milliseconds
                    return datetime.fromtimestamp(dt_value / 1000, timezone.utc)
                return datetime.fromtimestamp(dt_value, timezone.utc)
            elif isinstance(dt_value, str):
                if dt_value.endswith('Z'):
                    dt_value = dt_value[:-1] + '+00:00'
                dt = datetime.fromisoformat(dt_value)
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            elif isinstance(dt_value, datetime):
                return dt_value.astimezone(timezone.utc) if dt_value.tzinfo else dt_value.replace(tzinfo=timezone.utc)
            raise ValueError(f"Unsupported datetime format: {type(dt_value)}")
        except Exception as e:
            self.logger.error(f"Datetime parsing error for value {dt_value}: {e}")
            raise ValueError(f"Failed to parse datetime: {dt_value}") from e

    async def save_announcement(self, exchange: str, announcement: dict, classification=None, db_id: Optional[str] = None) -> bool:
        """Save an announcement with optional classification"""
        required_fields = ['id', 'publish_time', 'title']
        for field in required_fields:
            if field not in announcement:
                self.logger.error(f"Missing required field '{field}' in announcement")
                return False

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Generate deterministic ID if not provided
                    publish_time = await self._parse_datetime(announcement['publish_time'])
                    announcement_id = announcement['id']
                    db_id = db_id or self._generate_id(exchange, announcement_id, publish_time)
                    tags = announcement.get('tags')
                    if tags is not None and not isinstance(tags, str):
                        tags = json.dumps(tags)

                    raw_data = announcement.get('raw_data', announcement)
                    if raw_data is not None and not isinstance(raw_data, str):
                        raw_data = json.dumps(raw_data)


                    # Upsert the announcement
                    await conn.execute("""
                        INSERT INTO announcements (
                            id, exchange, announcement_id, publish_time, 
                            title, content, source_id, url,
                            announcement_type, tags, raw_data
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (exchange, announcement_id) DO UPDATE SET
                            publish_time = EXCLUDED.publish_time,
                            title = EXCLUDED.title,
                            content = EXCLUDED.content,
                            source_id = EXCLUDED.source_id,
                            url = EXCLUDED.url,
                            announcement_type = EXCLUDED.announcement_type,
                            tags = EXCLUDED.tags,
                            raw_data = EXCLUDED.raw_data,
                            updated_at = NOW()
                    """, 
                    db_id, exchange, announcement_id, publish_time,
                    announcement['title'], announcement.get('content'),
                    announcement.get('source_id'), announcement.get('url'),
                    self._extract_type(announcement),
                    tags,
                    raw_data)

                    # Handle classification if provided
                    if classification:
                        await self._save_classification(
                            conn=conn,
                            announcement_id=db_id,
                            classification=classification,
                            is_user=False
                        )
                    return True

                except Exception as e:
                    self.logger.error(f"Failed to save announcement: {e}", exc_info=True)
                    return False

    async def _save_classification(self, conn, announcement_id: str, classification: Union[dict, object], is_user: bool):
        """Helper method to save classification data"""
        try:
            if hasattr(classification, 'label'):
                label = classification.label
                confidence = float(getattr(classification, 'confidence', 0.0))
            elif isinstance(classification, dict):
                label = classification.get('label')
                confidence = float(classification.get('confidence', 0.0))
            else:
                self.logger.error(f"Unsupported classification type: {type(classification)}")
                return

            if not label:
                self.logger.warning("Classification missing label - skipping save")
                return

            await conn.execute("""
                INSERT INTO announcement_classifications (
                    announcement_id, label, confidence, is_user
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (announcement_id, is_user) WHERE NOT is_user DO UPDATE SET
                    label = EXCLUDED.label,
                    confidence = EXCLUDED.confidence,
                    classified_at = NOW()
            """, announcement_id, label, confidence, is_user)

        except Exception as e:
            self.logger.error(f"Failed to save classification: {e}", exc_info=True)

    async def update_user_classification(self, announcement_id: str, user_label: str) -> bool:
        """Update user classification for an announcement"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Update the announcement's user_label
                    await conn.execute("""
                        UPDATE announcements 
                        SET user_label = $1, updated_at = NOW()
                        WHERE id = $2
                    """, user_label, announcement_id)

                    # Insert or update the classification record
                    await self._save_classification(
                        conn=conn,
                        announcement_id=announcement_id,
                        classification={'label': user_label},
                        is_user=True
                    )
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to update user classification: {e}")
                    return False

    async def bulk_check_new(self, exchange: str, announcements: List[Dict]) -> List[Dict]:
        """Check which announcements are new to the system"""
        if not announcements:
            return []

        async with self.pool.acquire() as conn:
            try:
                # Prepare ID pairs with proper error handling
                id_pairs = []
                for a in announcements:
                    try:
                        publish_time = await self._parse_datetime(a['publish_time'])
                        storage_id = self._generate_id(exchange, a['id'], publish_time)
                        id_pairs.append((a, storage_id))
                    except Exception as e:
                        self.logger.warning(f"Skipping announcement due to error: {e}")
                        continue

                if not id_pairs:
                    return []

                # Check existing IDs in a single query
                ids = [sid for _, sid in id_pairs]
                existing = await conn.fetch("""
                    SELECT id FROM announcements 
                    WHERE id = ANY($1)
                """, ids)

                existing_ids = {r['id'] for r in existing}
                return [
                    {'announcement': ann, 'storage_id': sid}
                    for ann, sid in id_pairs if sid not in existing_ids
                ]

            except Exception as e:
                self.logger.error(f"Error in bulk_check_new: {e}", exc_info=True)
                return []

    @staticmethod
    def _generate_id(exchange: str, source_id: str, publish_time: datetime) -> str:
        """Generate a deterministic ID for announcements"""
        if publish_time.tzinfo is None:
            publish_time = publish_time.replace(tzinfo=timezone.utc)

        raw = f"{exchange}|{source_id}|{publish_time.isoformat()}"
        digest = hashlib.sha256(raw.encode()).digest()
        return base64.urlsafe_b64encode(digest)[:12].decode("utf-8")  # Increased length for better uniqueness

    @staticmethod
    def _extract_type(announcement: Dict) -> Optional[str]:
        """Extract announcement type from different possible fields"""
        if isinstance(announcement.get('type'), dict):
            return announcement['type'].get('title')
        return announcement.get('type') or announcement.get('annType')
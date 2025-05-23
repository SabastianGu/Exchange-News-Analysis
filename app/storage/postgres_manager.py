import asyncpg
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union
import hashlib
import json
from app.utilities.json_helpers import safe_json_dumps, ensure_serializable
from config.settings import settings

class AnnouncementStorage:
    def __init__(self):
        self.conn_string = settings.DATABASE_URL
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            self.conn_string,
            command_timeout=60,
            server_settings={'application_name': 'news_analyzer'}
        )
        await self._ensure_tables_exist()

    async def _ensure_tables_exist(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS announcements (
                    id TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    url TEXT,
                    title TEXT NOT NULL,
                    content TEXT,
                    announcement_type TEXT,
                    tags JSONB,
                    publish_time TIMESTAMPTZ NOT NULL,
                    raw_data JSONB NOT NULL,
                    classification TEXT,
                    confidence FLOAT,
                    user_label TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS announcement_classifications (
                    id SERIAL PRIMARY KEY,
                    announcement_id TEXT REFERENCES announcements(id),
                    label TEXT NOT NULL,
                    confidence FLOAT,
                    classified_at TIMESTAMP DEFAULT NOW(),
                    is_user BOOLEAN DEFAULT FALSE
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_publish_time 
                ON announcements(publish_time)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_exchange 
                ON announcements(exchange)
            """)

    async def _parse_datetime(self, dt_value: Union[int, str, datetime]) -> datetime:
        try:
            if isinstance(dt_value, int):
                return datetime.fromtimestamp(dt_value / 1000, timezone.utc)
            elif isinstance(dt_value, str):
                if dt_value.endswith('Z'):
                    dt_value = dt_value[:-1] + '+00:00'
                dt = datetime.fromisoformat(dt_value)
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            elif isinstance(dt_value, datetime):
                return dt_value.replace(tzinfo=timezone.utc) if dt_value.tzinfo is None else dt_value.astimezone(timezone.utc)
            raise ValueError(f"Unsupported datetime format: {type(dt_value)}")
        except Exception as e:
            print(f"Datetime parsing error: {e}")
            return datetime.now(timezone.utc)

    async def save_announcement(self, exchange: str, announcement: Dict, classification=None) -> bool:
        async with self.pool.acquire() as conn:
            try:
                publish_time = await self._parse_datetime(announcement['publish_time'])

                classification_label = None
                classification_confidence = None
                if classification:
                    if hasattr(classification, 'label'):
                        classification_label = classification.label
                        classification_confidence = float(classification.confidence)
                    elif isinstance(classification, dict):
                        classification_label = classification.get('label')
                        classification_confidence = float(classification.get('confidence', 0.0))

                announcement_id = self._generate_id(exchange, announcement['id'], publish_time)

                await conn.execute("""
                    INSERT INTO announcements (
                        id, exchange, source_id, url, title, content, 
                        announcement_type, tags, publish_time, raw_data, 
                        classification, confidence
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, 
                        $7, $8, $9::timestamptz, $10, 
                        $11, $12
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        raw_data = EXCLUDED.raw_data
                """, announcement_id, exchange, announcement['id'], announcement.get('url', ''),
                     announcement['title'], announcement.get('content') or announcement.get('description', ''),
                     self._extract_type(announcement), safe_json_dumps(announcement.get('tags', [])),
                     publish_time, safe_json_dumps(ensure_serializable(announcement)),
                     classification_label, classification_confidence)

                if classification_label:
                    await conn.execute("""
                        INSERT INTO announcement_classifications (
                            announcement_id, label, confidence, is_user
                        ) VALUES ($1, $2, $3, $4)
                    """, announcement_id, classification_label, classification_confidence, False)

                return True
            except Exception as e:
                print(f"Postgres save error: {e}")
                return False

    async def update_user_classification(self, announcement_id: str, user_label: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE announcements SET user_label = $1 WHERE id = $2
            """, user_label, announcement_id)

            await conn.execute("""
                INSERT INTO announcement_classifications (
                    announcement_id, label, is_user
                ) VALUES ($1, $2, TRUE)
            """, announcement_id, user_label)

    async def save_announcements_batch(self, exchange: str, new_announcements: List[Dict], classifications: Union[List, object]) -> bool:
        try:
            if hasattr(classifications, 'results'):
                classifications = classifications.results

            if len(new_announcements) != len(classifications):
                print(f"❌ Count mismatch: {len(new_announcements)} anns vs {len(classifications)} classifs")
                return False

            results = []
            for i, (item, classification) in enumerate(zip(new_announcements, classifications), 1):
                try:
                    success = await self.save_announcement(exchange=exchange, announcement=item["announcement"], classification=classification)
                    results.append(success)
                    if i % 5 == 0:
                        print(f"Processed {i}/{len(new_announcements)}")
                except Exception as e:
                    print(f"⚠️ Failed item {i}: {e}")
                    results.append(False)
            return all(results)
        except Exception as e:
            print(f"Batch processing failed: {e}")
            return False

    async def bulk_check_new(self, exchange: str, announcements: List[Dict]) -> List[Dict]:
        if not announcements:
            return []
        async with self.pool.acquire() as conn:
            ids = []
            for a in announcements:
                try:
                    publish_time = await self._parse_datetime(a['publish_time'])
                    ids.append(self._generate_id(exchange, a['id'], publish_time))
                except Exception as e:
                    print(f"Skipping announcement due to datetime error: {e}")
                    continue

            existing = await conn.fetch("SELECT id FROM announcements WHERE id = ANY($1)", ids)
            existing_ids = {r['id'] for r in existing}
            return [{'announcement': ann, 'storage_id': id_} for ann, id_ in zip(announcements, ids) if id_ not in existing_ids]

    @staticmethod
    def _generate_id(exchange: str, source_id: str, publish_time: datetime) -> str:
        import base64
        if publish_time.tzinfo is None:
            publish_time = publish_time.replace(tzinfo=timezone.utc)

        raw = f"{exchange}|{source_id}|{publish_time.isoformat()}"
        digest = hashlib.sha256(raw.encode()).digest()
        return base64.urlsafe_b64encode(digest)[:7].decode("utf-8")

    @staticmethod
    def _extract_type(announcement: Dict) -> Optional[str]:
        if isinstance(announcement.get('type'), dict):
            return announcement['type'].get('title')
        return announcement.get('type') or announcement.get('annType')
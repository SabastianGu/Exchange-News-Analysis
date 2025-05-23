import asyncpg
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union
import hashlib
import json
from app.utilities.json_helpers import safe_json_dumps, ensure_serializable
import asyncpg
from datetime import datetime
from typing import List, Dict, Optional
from config.settings import settings
import hashlib
import json

class AnnouncementStorage:
    def __init__(self):
        # Modified connection string with your credentials
        self.conn_string = settings.DATABASE_URL
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            self.conn_string,
            command_timeout=60,
            server_settings={
                'application_name': 'news_analyzer'
            }
        )
        await self._ensure_tables_exist()

    async def _ensure_tables_exist(self):
        """Create tables if they don't exist"""
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
                    confidence FLOAT
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS announcement_classifications (
                    id SERIAL PRIMARY KEY,
                    announcement_id TEXT REFERENCES announcements(id),
                    label TEXT NOT NULL,
                    confidence FLOAT,
                    classified_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create indexes if they don't exist
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_publish_time 
                ON announcements(publish_time)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_announcements_exchange 
                ON announcements(exchange)
            """)
    async def _parse_datetime(self, dt_value: Union[int, str, datetime]) -> datetime:
        """Normalize various datetime formats to timezone-aware UTC datetime"""
        try:
            if isinstance(dt_value, int):
                # Handle millisecond timestamps - ensure UTC
                return datetime.fromtimestamp(dt_value / 1000, timezone.utc)
            elif isinstance(dt_value, str):
                # Handle ISO format strings
                if dt_value.endswith('Z'):
                    dt_value = dt_value[:-1] + '+00:00'
                dt = datetime.fromisoformat(dt_value)
                return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            elif isinstance(dt_value, datetime):
                # Ensure existing datetimes are timezone-aware UTC
                if dt_value.tzinfo is None:
                    return dt_value.replace(tzinfo=timezone.utc)
                return dt_value.astimezone(timezone.utc)
            raise ValueError(f"Unsupported datetime format: {type(dt_value)}")
        except Exception as e:
            print(f"Datetime parsing error: {e}")
            return datetime.now(timezone.utc) # Fallback to current UTC time

    async def save_announcement(self, exchange: str, announcement: Dict, classification=None) -> bool:
        """Save full announcement data with proper datetime handling"""
        async with self.pool.acquire() as conn:
            try:
                # Normalize publish_time
                publish_time = await self._parse_datetime(announcement['publish_time'])
                if publish_time.tzinfo != timezone.utc:
                    publish_time = publish_time.astimezone(timezone.utc)

                # Handle classification data
                classification_label = None
                classification_confidence = None
                
                if classification is not None:
                    # Direct access for PredictionResponse objects
                    if hasattr(classification, 'label') and hasattr(classification, 'confidence'):
                        classification_label = classification.label
                        classification_confidence = float(classification.confidence)
                    
                    # Fallback for dictionary format (though shouldn't be needed now)
                    elif isinstance(classification, dict):
                        classification_label = classification.get('label')
                        classification_confidence = float(classification.get('confidence', 0.0))
                    
                    print(f"Classification: {classification_label} ({classification_confidence})")

                # Generate ID and save to database
                announcement_id = self._generate_id(exchange, announcement['id'], publish_time)
                
                await conn.execute("""
                    INSERT INTO announcements (
                        id, exchange, source_id, url,
                        title, content, announcement_type, tags,
                        publish_time, raw_data,
                        classification, confidence
                    ) VALUES (
                        $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9::timestamptz, $10,
                        $11, $12
                    ) ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        raw_data = EXCLUDED.raw_data
                """,
                announcement_id,
                exchange,
                announcement['id'],
                announcement.get('url', ''),
                announcement['title'],
                announcement.get('content') or announcement.get('description', ''),
                self._extract_type(announcement),
                safe_json_dumps(announcement.get('tags', [])),
                publish_time,
                safe_json_dumps(ensure_serializable(announcement)),
                classification_label,
                classification_confidence
                )

                # Save to classifications table
                if classification_label:
                    await conn.execute("""
                        INSERT INTO announcement_classifications (
                            announcement_id, label, confidence
                        ) VALUES ($1, $2, $3)
                    """,
                    announcement_id,
                    classification_label,
                    classification_confidence
                    )

                return True
            except Exception as e:
                print(f"Postgres save error: {e}")
                return False
            

    async def save_announcements_batch(
        self, 
        exchange: str, 
        new_announcements: List[Dict], 
        classifications: Union[List, object]  # Accepts both raw list and results object
    ) -> bool:
        """
        Handles both:
        - classifications.results (from test case)
        - raw classification lists/tuples
        """
        try:
            # Normalize classifications
            if hasattr(classifications, 'results'):
                classifications = classifications.results  # Extract from results object
                
            if len(new_announcements) != len(classifications):
                print(f"❌ Count mismatch: {len(new_announcements)} anns vs {len(classifications)} classifs")
                return False

            results = []
            for i, (item, classification) in enumerate(zip(new_announcements, classifications), 1):
                try:
                    success = await self.save_announcement(
                        exchange=exchange,
                        announcement=item["announcement"],
                        classification=classification
                    )
                    results.append(success)
                    
                    # Progress tracking
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
        """Filter only new announcements with proper datetime handling"""
        if not announcements:
            return []

        async with self.pool.acquire() as conn:
            # Generate IDs with normalized datetimes
            ids = []
            for a in announcements:
                try:
                    publish_time = await self._parse_datetime(a['publish_time'])
                    ids.append(self._generate_id(exchange, a['id'], publish_time))
                except Exception as e:
                    print(publish_time)
                    print(f"Skipping announcement due to datetime error: {e}")
                    continue

            # Check existing IDs
            existing = await conn.fetch(
                "SELECT id FROM announcements WHERE id = ANY($1)", 
                ids
            )
            existing_ids = {r['id'] for r in existing}
            
            # Return only new announcements
            return [
                {'announcement': ann, 'storage_id': id_}
                for ann, id_ in zip(announcements, ids)
                if id_ not in existing_ids
            ]

    @staticmethod
    def _generate_id(exchange: str, source_id: str, publish_time: datetime) -> str:
        """Generate consistent ID using UTC-normalized datetime"""
        if publish_time.tzinfo is None:
            publish_time = publish_time.replace(tzinfo=timezone.utc)
        unique_str = f"{exchange}|{source_id}|{publish_time.isoformat()}"
        return hashlib.sha256(unique_str.encode()).hexdigest()

    @staticmethod
    def _extract_type(announcement: Dict) -> Optional[str]:
        """Handle different type formats from exchanges"""
        if isinstance(announcement.get('type'), dict):
            return announcement['type'].get('title')
        return announcement.get('type') or announcement.get('annType')
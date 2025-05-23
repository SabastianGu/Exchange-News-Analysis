from typing import List, Dict, Tuple
import numpy as np
from datetime import datetime, timedelta, timezone
from app.storage.postgres_manager import AnnouncementStorage

class TrainingDataPipeline:
    def __init__(self, storage: AnnouncementStorage):
        self.storage = storage
    
    async def get_labeled_data(self, min_confidence: float = 0.9, lookback_days: int = 90) -> List[Dict]:
        """
        Fetch high-confidence labeled data from database for training
        
        Args:
            min_confidence: Minimum classification confidence score to include
            lookback_days: How many days of historical data to include
            
        Returns:
            List of training examples with 'text' and 'label' fields
        """
        async with self.storage.pool.acquire() as conn:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            
            records = await conn.fetch("""
                SELECT 
                    a.id,
                    a.title,
                    a.content,
                    ac.label,
                    ac.confidence
                FROM announcements a
                JOIN announcement_classifications ac ON a.id = ac.announcement_id
                WHERE 
                    ac.confidence >= $1 AND
                    a.publish_time >= $2 AND
                    ac.label IS NOT NULL
                ORDER BY ac.confidence DESC
            """, min_confidence, cutoff_date)
            
            return [{
                'id': r['id'],
                'text': self._combine_text_fields(r['title'], r['content']),
                'label': self._label_to_index(r['label']),
                'confidence': r['confidence']
            } for r in records]
    
    async def get_human_labeled_data(self, lookback_days: int = 180) -> List[Dict]:
        """
        Fetch data that has been manually labeled/verified by humans
        """
        async with self.storage.pool.acquire() as conn:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            
            records = await conn.fetch("""
                SELECT 
                    a.id,
                    a.title,
                    a.content,
                    a.classification as label
                FROM announcements a
                WHERE 
                    a.publish_time >= $1 AND
                    a.classification IS NOT NULL AND
                    (a.confidence IS NULL OR a.confidence < 0.7)  # Likely human-labeled
            """, cutoff_date)
            
            return [{
                'id': r['id'],
                'text': self._combine_text_fields(r['title'], r['content']),
                'label': self._label_to_index(r['label']),
            } for r in records]
    
    @staticmethod
    def _combine_text_fields(title: str, content: str) -> str:
        """Combine title and content into a single training text"""
        content = content or ""
        return f"{title}\n\n{content}".strip()
    
    @staticmethod
    def _label_to_index(label: str) -> int:
        """Convert label string to numeric index"""
        label_map = {
            "trading": 0,
            "engineering": 1,
            "irrelevant": 2
        }
        return label_map.get(label.lower(), 2)  # Default to irrelevant
    
    async def get_train_val_split(self, test_size: float = 0.2) -> Tuple[List[Dict], List[Dict]]:
        """
        Get training and validation datasets with balanced classes
        
        Args:
            test_size: Fraction of data to use for validation
            
        Returns:
            Tuple of (train_data, val_data)
        """
        # Get both model-labeled and human-labeled data
        model_data = await self.get_labeled_data()
        human_data = await self.get_human_labeled_data()
        all_data = model_data + human_data
        
        # Shuffle and split
        np.random.shuffle(all_data)
        split_idx = int(len(all_data) * (1 - test_size))
        return all_data[:split_idx], all_data[split_idx:]
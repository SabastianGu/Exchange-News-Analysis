import tensorflow as tf
from tf_keras.layers import Input, Dense, Dropout
from tf_keras.models import Model
from transformers import BertTokenizer, TFBertModel
from app.core.schemas import PredictionResponse, BatchPredictionResponse, PredictionDetails
import json
from typing import List, Dict
from datetime import datetime
import os
import numpy as np


class AnnouncementClassifier:
    # После первого запуска модель будет сохранена в папку model_assets и можно будет подгружать ее локально various_models/bert-base-uncased
    def __init__(self, model_name: str = 'model_assets/bert-base-uncased'): #download bert-base-uncased to model_assets folder, or use it from the web and leave just 'bert-base-uncased'
        self.tokenizer = BertTokenizer.from_pretrained(model_name)
        self.bert = TFBertModel.from_pretrained(model_name)
        self._build_model()
        self.label_map = {
            0: "trading",
            1: "engineering",
            2: "irrelevant"
        }

    def _build_model(self):
        input_ids = Input(shape=(None,), dtype=tf.int32)
        attention_mask = Input(shape=(None,), dtype=tf.int32)

        bert_output = self.bert(input_ids, attention_mask=attention_mask)
        pooled_output = bert_output.pooler_output
        x = Dropout(0.3)(pooled_output)
        outputs = Dense(3, activation='softmax')(x)

        self.model = Model(inputs=[input_ids, attention_mask], outputs=outputs)

    def _preprocess_text(self, announcement: Dict) -> str:
        if not isinstance(announcement, dict):
            raise ValueError(f"Expected announcement as dict, got {type(announcement)}")

        title = announcement.get("title", "")
        description = announcement.get("description") or announcement.get("content", "")
        ann_type = (
            announcement.get("type", {}).get("title", "")
            if isinstance(announcement.get("type"), dict)
            else announcement.get("type", "")
        )
        tags = ", ".join(announcement.get("tags", []))

        return f"Title: {title}\nContent: {description}\nType: {ann_type}\nTags: {tags}"

    async def predict_announcement(self, announcement: Dict) -> PredictionResponse:
        if isinstance(announcement, str):
            try:
                announcement = json.loads(announcement)
            except json.JSONDecodeError:
                # If it's not JSON, treat as raw content
                announcement = {"content": announcement}

        if not isinstance(announcement, dict):
            raise ValueError(f"Expected announcement as dict, got {type(announcement)}")

        text = self._preprocess_text(announcement)
        inputs = self.tokenizer(
            text,
            return_tensors='tf',
            truncation=True,
            max_length=512
        )
        probs = self.model.predict([inputs['input_ids'], inputs['attention_mask']])[0]

        # Handle type field which might be a dictionary
        ann_type = ""
        if isinstance(announcement.get('type'), dict):
            ann_type = announcement['type'].get('title', '')
        elif announcement.get('type') is not None:
            ann_type = str(announcement['type'])

        return PredictionResponse(
            text=announcement.get('title', announcement.get('content', '')),
            label=self.label_map[np.argmax(probs)],
            confidence=float(np.max(probs)),
            details=PredictionDetails(
                type=ann_type,
                tags=announcement.get('tags', []),
                url=announcement.get('url', "")
            )
        )

    async def predict_batch(self, announcements: List) -> BatchPredictionResponse:
        clean_announcements = []
        for a in announcements:
            if isinstance(a, dict):
                clean_announcements.append(a)
            elif isinstance(a, str):
                try:
                    parsed = json.loads(a)
                    clean_announcements.append(parsed if isinstance(parsed, dict) else {"content": a})
                except json.JSONDecodeError:
                    clean_announcements.append({"content": a})
            else:
                raise ValueError(f"Invalid announcement in batch: expected dict or JSON string, got {type(a)}")

        texts = [self._preprocess_text(a) for a in clean_announcements]
        inputs = self.tokenizer(
            texts,
            return_tensors='tf',
            truncation=True,
            padding=True,
            max_length=512
        )
        probs = self.model.predict([inputs['input_ids'], inputs['attention_mask']])

        results = []
        for ann, prob in zip(clean_announcements, probs):
            # Handle type field which might be a dictionary
            ann_type = ""
            if isinstance(ann.get('type'), dict):
                ann_type = ann['type'].get('title', '')
            elif ann.get('type') is not None:
                ann_type = str(ann['type'])

            results.append(
                PredictionResponse(
                    text=ann.get('title', ann.get('content', '')),
                    label=self.label_map[np.argmax(prob)],
                    confidence=float(np.max(prob)),
                    details=PredictionDetails(
                        type=ann_type,
                        tags=ann.get('tags', []),
                        url=ann.get('url', "")
                    )
                )
            )

        return BatchPredictionResponse(results=results)

    def save_pretrained(self, save_dir: str):
        save_dir = './config/various_models'
        os.makedirs(save_dir, exist_ok=True)
        self.model.save(save_dir, save_format='tf')
        self.tokenizer.save_pretrained(save_dir)

        metadata = {
            "model_type": "TFBertForSequenceClassification",
            "classes": list(self.label_map.values()),
            "timestamp": datetime.now().isoformat()
        }
        with open(f"{save_dir}/metadata.json", "w") as f:
            json.dump(metadata, f)

        print(f"Model saved to {save_dir}")

    @classmethod
    def from_pretrained(cls, save_dir: str):
        required_files = [
            "saved_model.pb",
            "variables/variables.index",
            "tokenizer_config.json"
        ]
        for f in required_files:
            if not os.path.exists(f"{save_dir}/{f}"):
                raise ValueError(f"Missing required file: {f}")

        tokenizer = BertTokenizer.from_pretrained(save_dir)
        model = tf.keras.models.load_model(save_dir)

        instance = cls.__new__(cls)
        instance.tokenizer = tokenizer
        instance.model = model
        instance.label_map = {
            0: "trading",
            1: "engineering",
            2: "irrelevant"
        }
        return instance
import tensorflow as tf
from tf_keras.layers import Input, Dense, Dropout, BatchNormalization
from tf_keras.models import Model
from tf_keras.optimizers import Adam
from tf_keras.callbacks import ModelCheckpoint, EarlyStopping
from transformers import BertTokenizer, TFBertModel
from typing import Dict, List
from app.core.model import AnnouncementClassifier
from app.Future_model.data_pipeline import TrainingDataPipeline
from app.storage.postgres_manager import AnnouncementStorage

class TrainableAnnouncementClassifier(AnnouncementClassifier):
    def __init__(self, model_name: str = 'bert-base-uncased', learning_rate: float = 2e-5):
        super().__init__(model_name)
        self.learning_rate = learning_rate
        self._compile_model()
    
    def _build_model(self):
        """Enhanced model architecture with dropout and batch normalization"""
        input_ids = Input(shape=(None,), dtype=tf.int32, name='input_ids')
        attention_mask = Input(shape=(None,), dtype=tf.int32, name='attention_mask')
        
        # Freeze BERT layers initially (can be unfrozen later)
        bert_output = self.bert(input_ids, attention_mask=attention_mask)
        pooled_output = bert_output.pooler_output
        
        # Enhanced classification head
        x = Dropout(0.3)(pooled_output)
        x = Dense(256, activation='relu')(x)
        x = BatchNormalization()(x)
        x = Dropout(0.2)(x)
        outputs = Dense(3, activation='softmax')(x)
        
        self.model = Model(inputs=[input_ids, attention_mask], outputs=outputs)
    
    def _compile_model(self):
        """Compile with appropriate loss and metrics"""
        self.model.compile(
            optimizer=Adam(self.learning_rate),
            loss='sparse_categorical_crossentropy',
            metrics=['accuracy']
        )
    
    async def train_from_storage(self, storage: AnnouncementStorage, epochs=3, batch_size=16):
        """
        Complete training workflow using data from storage
        
        Args:
            storage: AnnouncementStorage instance
            epochs: Number of training epochs
            batch_size: Batch size for training
        """
        # Initialize data pipeline
        pipeline = TrainingDataPipeline(storage)
        
        # Get training data
        train_data, val_data = await pipeline.get_train_val_split()
        
        # Prepare TensorFlow datasets
        train_dataset = self._prepare_dataset(train_data, batch_size)  # Pass batch_size here
        val_dataset = self._prepare_dataset(val_data, batch_size)     # And here
        
        # Train the model
        history = self._train_model(train_dataset, val_dataset, epochs, batch_size)
        
        return history
    
    def _prepare_dataset(self, data: List[Dict], batch_size: int) -> tf.data.Dataset:
        """Convert list of training examples to TF Dataset
        
        Args:
            data: List of training examples with 'text' and 'label'
            batch_size: Batch size for the dataset
            
        Returns:
            Configured TF Dataset ready for training
        """
        texts = [d['text'] for d in data]
        labels = [d['label'] for d in data]
        
        # Tokenize all texts
        tokenized = self.tokenizer(
            texts,
            padding='max_length',
            truncation=True,
            max_length=512,
            return_tensors='tf'
        )
        
        # Create dataset
        dataset = tf.data.Dataset.from_tensor_slices((
            {
                'input_ids': tokenized['input_ids'],
                'attention_mask': tokenized['attention_mask']
            },
            labels
        ))
        
        return dataset.shuffle(len(data)).batch(batch_size).prefetch(tf.data.AUTOTUNE)
    
    def _train_model(self, train_dataset, val_dataset, epochs, batch_size):
        """Internal training method with callbacks"""
        callbacks = [
            ModelCheckpoint(
                'best_model.h5',
                save_best_only=True,
                monitor='val_loss',
                mode='min'
            ),
            EarlyStopping(
                patience=2,
                restore_best_weights=True
            )
        ]
        
        history = self.model.fit(
            train_dataset,
            validation_data=val_dataset,
            epochs=epochs,
            batch_size=batch_size,
            callbacks=callbacks
        )
        
        return history
    
    async def evaluate_on_storage(self, storage: AnnouncementStorage):
        """Evaluate model performance on validation data from storage"""
        pipeline = TrainingDataPipeline(storage)
        _, val_data = await pipeline.get_train_val_split()
        val_dataset = self._prepare_dataset(val_data)
        
        results = self.model.evaluate(val_dataset)
        return dict(zip(self.model.metrics_names, results))
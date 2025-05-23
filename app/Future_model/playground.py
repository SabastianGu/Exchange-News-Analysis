from app.storage.postgres_manager import AnnouncementStorage
from .model_training import TrainableAnnouncementClassifier

async def main():
    storage = AnnouncementStorage()
    await storage.connect()
    # Initialize classifier - either new or pretrained
    classifier = TrainableAnnouncementClassifier.from_pretrained('./config/various_models/')
    training_history = await classifier.train_from_storage(storage, epochs=3)
    evaluation_results = await classifier.evaluate_on_storage(storage)
    print(f"Validation accuracy: {evaluation_results['accuracy']:.2f}")
    # Save the trained model
    classifier.save_pretrained('./config/various_models')

import asyncio
asyncio.run(main())
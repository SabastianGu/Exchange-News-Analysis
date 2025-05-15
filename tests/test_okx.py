import asyncio
import os
from app.services.ByBitclient import BybitClient
from app.core.model import AnnouncementClassifier
from app.services.tlg_notifier import Notifier
from app.storage.postgres_manager import AnnouncementStorage
from config.settings import settings
import traceback
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_okx_client():
    """Test OKX client independently"""
    print("\n🔍 Testing OKX Client...")
    
    # Initialize with environment variables
    okx = OKXClient()
    
    try:
        announcements = await okx.fetch_announcements()
        print(f"✅ Retrieved {len(announcements)} announcements")
        
        if announcements:
            print("\n📋 Sample announcement structure:")
            sample = announcements[0]['announcement']
            print(f"Title: {sample['title']}")
            print(f"Content preview: {sample['content'][:100]}...")
            print(f"URL: {sample['url']}")
            print(f"Published: {sample['publish_time']}")
            print(f"Tags: {sample['tags']}")
            print(f"Type: {sample['type']}")
            
            # Verify required fields
            required_fields = ['title', 'content', 'url', 'publish_time']
            for field in required_fields:
                assert field in sample, f"Missing required field: {field}"
            print("✅ All required fields present")
            
    except Exception as e:
        logger.error(f"❌ OKX Client test failed: {e}")
        logger.debug(traceback.format_exc())
    finally:
        await okx.close()

async def compare_exchange_clients():
    """Compare OKX and Bybit client outputs"""
    print("\n🔍 Comparing exchange clients...")
    
    clients = {
        "OKX": OKXClient(),
        "Bybit": BybitClient()
    }
    
    results = {}
    
    for name, client in clients.items():
        try:
            print(f"\n🔄 Fetching {name} announcements...")
            results[name] = await client.fetch_announcements()
            print(f"✅ {name}: {len(results[name])} announcements")
            
            if results[name]:
                print(f"Sample keys: {list(results[name][0]['announcement'].keys())}")
        except Exception as e:
            logger.error(f"❌ {name} client failed: {e}")
            results[name] = []
    
    return results

async def main():
    print("🚀 Starting Exchange API Tests")
    
    # Test OKX client independently
    await test_okx_client()
    
    # Compare both clients
    results = await compare_exchange_clients()
    
    # Verify compatibility with analyzer
    if results.get("OKX"):
        print("\n🔧 Testing OKX output with Analyzer...")
        try:
            model = AnnouncementClassifier()
            texts = [
                f"{item['announcement']['title']}\n{item['announcement'].get('content', '')}"
                for item in results["OKX"][:3]  # Test with first 3 announcements
            ]
            
            classifications = await model.predict_batch(texts)
            print(f"✅ Successfully classified {len(classifications.results)} OKX announcements")
            
            # Show sample classification
            if classifications.results:
                sample = classifications.results[0]
                print("\n📋 Sample classification:")
                print(f"Text: {sample.text[:100]}...")
                print(f"Label: {sample.label}")
                print(f"Confidence: {sample.confidence:.2f}")
                print(f"Details: {sample.details}")
                
        except Exception as e:
            logger.error(f"❌ Analyzer test failed: {e}")
            logger.debug(traceback.format_exc())
    
    print("\n🧪 Testing complete")

if __name__ == "__main__":
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    # Verify required environment variables

    
    asyncio.run(main())
import asyncio
from app.services.okx_client import OKXClient
from app.services.ByBitclient import BybitClient
from app.core.model import AnnouncementClassifier
from app.services.tlg_notifier import Notifier
from app.storage.postgres_manager import AnnouncementStorage
import traceback

async def main():
    print("🔧 Connecting to DB...")
    storage = AnnouncementStorage()
    await storage.connect()

    print("✅ DB connected")

    print("📡 Fetching announcements...")
    bybit = BybitClient()
    okx = OKXClient()

    # Fetch announcements
    bybit_results, okx_results = await asyncio.gather(
        bybit.fetch_announcements(),
        okx.fetch_announcements()
    )

    print(f"Bybit announcements fetched: {len(bybit_results)}")
    print(f"OKX announcements fetched: {len(okx_results)}")

    all_results = [
        ("bybit", bybit_results),
        ("okx", okx_results)
    ]

    model = AnnouncementClassifier()
    notifier = Notifier()

    for exchange, announcements in all_results:
        print(f"\n🔍 Checking {exchange} batch")

        if not announcements:
            print(f"⚠️ No announcements from {exchange}")
            continue

        # Deduplication
        new_announcements = await storage.bulk_check_new(exchange, announcements)
        print(f"🆕 New announcements: {len(new_announcements)}")

        if not new_announcements:
            continue

        # Prepare texts for classification
        texts = [
            f"{item['announcement']['title']}\n{item['announcement'].get('content', '')}"
            for item in new_announcements
        ]

        print("🔍 Previewing texts before classification:")
        for i, t in enumerate(texts):
            safe_text = t[:100].replace('\n', ' ')
            print(f"  {i}: {safe_text}...")

        try:
            classifications = await model.predict_batch(texts)
            
            # Debug print to see the structure of classifications
            print("\n🔎 Classification results structure:")
            print(f"Type of classifications: {type(classifications)}")
            if hasattr(classifications, 'results'):
                print(f"Number of results: {len(classifications.results)}")
                print("First classification structure:")
                print(dir(classifications.results[0]))
        except Exception as e:
            print(f"❌ Error in model prediction batch: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            print("🔎 Debug info:")
            for i, t in enumerate(texts):
                print(f"  {i}: {repr(t)}")
            continue

        for item, classification in zip(new_announcements, classifications.results):
            announcement = item["announcement"]

            # Debug print classification object
            print("\n🔍 Full classification object:")
            print(dir(classification))
            print(f"Available attributes: {vars(classification)}")

            print(f"\n📝 Title: {announcement['title']}")
            
            # Proper way to access classification attributes
            try:
                print(f"🔢 Label: {classification.label} | Confidence: {classification.confidence:.2f}")
                print(f"ℹ️ Details: {classification.details}")
                
                # Save to DB
                try:
                    await storage.save_announcement(
                        exchange=exchange,
                        announcement=announcement,
                        classification=classification
                    )
                    print("✅ Saved to DB")
                except Exception as e:
                    print(f"❌ Failed to save: {e}")

                # Optional: test notifier
                if classification.confidence > 0.7:
                    try:
                        await notifier.send(
                            department=classification.label,
                            message=(
                                f"🚨 New {classification.label} announcement\n"
                                f"📌 {announcement['title']}\n"
                                f"📊 Confidence: {classification.confidence:.0%}\n"
                                f"⏰ {announcement['publish_time']}\n"
                                f"🔗 Raw data: {announcement['raw_data']}"
                            )
                        )
                        print("📬 Notified")
                    except Exception as e:
                        print(f"❌ Notification failed: {e}")
                        
            except AttributeError as e:
                print(f"❌ Error accessing classification attributes: {e}")
                print("Available attributes:")
                if hasattr(classification, '__dict__'):
                    print(vars(classification))
                else:
                    print(dir(classification))
                continue

    await storage.pool.close()
    print("🔌 DB connection closed")

if __name__ == "__main__":
    asyncio.run(main())
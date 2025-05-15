import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from app.storage.postgres_manager import AnnouncementStorage
import json

@pytest.mark.asyncio
async def test_json_serialization_errors():
    """Test all JSON serialization points in storage operations"""
    storage = AnnouncementStorage()
    storage.pool = AsyncMock()  # Mock the database connection

    # Test data with various datetime formats
    test_announcement = {
        "id": "test123",
        "title": "Test Announcement",
        "content": "Test content",
        "publish_time": datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
        "url": "https://example.com",
        "type": {"title": "test_type"},
        "tags": ["tag1", "tag2"],
        "raw_data": {
            "nested_datetime": datetime.now(timezone.utc),  # This could cause issues
            "regular_field": "value"
        }
    }

    test_classification = {
        "label": "test_label",
        "confidence": 0.95,
        "details": {
            "type": "test",
            "timestamp": datetime.now(timezone.utc)  # Potential issue
        }
    }

    # 1. Test raw_data serialization
    try:
        print("\nTesting raw_data serialization...")
        json.dumps(test_announcement['raw_data'])
        print("✅ raw_data serialization successful")
    except TypeError as e:
        print(f"❌ raw_data serialization failed: {e}")
        pytest.fail(f"raw_data contains non-serializable objects: {e}")

    # 2. Test classification details serialization
    try:
        print("\nTesting classification details serialization...")
        json.dumps(test_classification['details'])
        print("✅ classification details serialization successful")
    except TypeError as e:
        print(f"❌ classification details serialization failed: {e}")
        pytest.fail(f"classification details contains non-serializable objects: {e}")

    # 3. Test full save_announcement flow
    try:
        print("\nTesting complete save_announcement flow...")
        with patch.object(storage, '_parse_datetime', return_value=datetime.now(timezone.utc)):
            result = await storage.save_announcement(
                exchange="test",
                announcement=test_announcement,
                classification=test_classification
            )
            assert result is True
            print("✅ save_announcement executed successfully")
            
            # Verify the parameters passed to execute
            args, kwargs = storage.pool.execute.call_args
            print("\nInspecting database query parameters:")
            for i, arg in enumerate(args):
                try:
                    json.dumps(arg)  # Test each argument for JSON serialization
                    print(f"✅ Argument {i} is JSON serializable")
                except TypeError as e:
                    print(f"❌ Argument {i} is NOT JSON serializable: {e}")
                    print(f"Problematic argument: {arg}")
                    pytest.fail(f"Non-serializable argument passed to execute: {e}")
                    
    except Exception as e:
        print(f"❌ save_announcement failed: {e}")
        pytest.fail(f"save_announcement failed with: {e}")

    # 4. Test bulk_check_new with datetime handling
    try:
        print("\nTesting bulk_check_new datetime handling...")
        announcements = [{
            "id": "test123",
            "publish_time": datetime.now(timezone.utc),
            "title": "Test",
            "content": "Test content"
        }]
        
        with patch.object(storage, '_generate_id', return_value="mock_id"):
            result = await storage.bulk_check_new("test", announcements)
            assert len(result) == 1
            print("✅ bulk_check_new handled datetimes successfully")
    except Exception as e:
        print(f"❌ bulk_check_new failed: {e}")
        pytest.fail(f"bulk_check_new failed with: {e}")

@pytest.fixture
def datetime_encoder():
    """Custom JSON encoder for datetime objects"""
    class DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return super().default(obj)
    return DateTimeEncoder

@pytest.mark.asyncio
async def test_with_custom_encoder(datetime_encoder):
    """Test with custom JSON encoder"""
    storage = AnnouncementStorage()
    storage.pool = AsyncMock()

    test_data = {
        "timestamp": datetime.now(timezone.utc),
        "nested": {
            "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)
        }
    }

    try:
        print("\nTesting with custom datetime encoder...")
        serialized = json.dumps(test_data, cls=datetime_encoder)
        print("✅ Custom encoder worked successfully")
        print(f"Serialized output: {serialized[:100]}...")
    except Exception as e:
        print(f"❌ Custom encoder failed: {e}")
        pytest.fail(f"Custom encoder failed: {e}")
import pytest
import asyncio
from pprint import pprint
from app.services.forex_factory import ForexFactoryService


@pytest.mark.asyncio
async def test_get_today_events_raw_data():
    service = ForexFactoryService()
    data = await service.get_today_events()

    assert data is not None, "API returned no data"
    assert isinstance(data, list), f"Expected list but got {type(data)}"

    print("\n\n=== 🔍 Raw API response ===")
    for i, item in enumerate(data):
        print(f"\n--- Event #{i+1} ---")
        pprint(item)


@pytest.mark.asyncio
async def test_get_formatted_events_output():
    service = ForexFactoryService()
    message = await service.get_formatted_events()

    print("\n\n=== 📦 Formatted message ===")
    print(message)

    assert isinstance(message, str)
    assert "*Today's Forex Factory Calendar*" in message

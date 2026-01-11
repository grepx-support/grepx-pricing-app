"""
Simple test for database server basic operations
Tests INSERT and SELECT operations
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import setup_paths
import asyncio
from core.session import Session
from core import Model, IntegerField, StringField, BooleanField

# Load environment
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load env.common to get GREPX_MASTER_DB_URL
env_common = project_root / "env.common"
if env_common.exists():
    with open(env_common) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                # Expand ${PROJECT_ROOT} if present
                if '${PROJECT_ROOT}' in value:
                    value = value.replace('${PROJECT_ROOT}', str(project_root))
                os.environ[key] = value

# Get database URL
db_url = os.getenv('GREPX_MASTER_DB_URL')
if not db_url:
    print("ERROR: GREPX_MASTER_DB_URL not set")
    sys.exit(1)

print(f"Testing with database: {db_url}")


# Define a simple test model
class TestItem(Model):
    id = IntegerField(primary_key=True)
    name = StringField(max_length=100)
    value = StringField(max_length=200)
    is_active = BooleanField()
    
    @classmethod
    def get_table_name(cls) -> str:
        return 'test_items'


async def test_insert_and_select():
    """Test basic INSERT and SELECT operations"""
    
    print("\n=== Starting Database Test ===\n")
    
    # Connect to database
    async with Session.from_connection_string(db_url) as session:
        print("✓ Connected to database")
        
        # Create table
        await session.create_table(TestItem)
        print("✓ Created table: test_items")
        
        # INSERT operation
        print("\n--- Testing INSERT ---")
        item1 = TestItem(id=1, name="Test Item 1", value="Value 1", is_active=True)
        item2 = TestItem(id=2, name="Test Item 2", value="Value 2", is_active=True)
        item3 = TestItem(id=3, name="Test Item 3", value="Value 3", is_active=False)
        
        await session.add(item1)
        print("✓ Inserted item 1")
        await session.add(item2)
        print("✓ Inserted item 2")
        await session.add(item3)
        print("✓ Inserted item 3")
        
        # SELECT operation - get all items
        print("\n--- Testing SELECT (all items) ---")
        all_items = await TestItem.query().all()
        print(f"✓ Found {len(all_items)} items:")
        for item in all_items:
            data = item.to_dict()
            print(f"  - ID: {data['id']}, Name: {data['name']}, Value: {data['value']}, Active: {data['is_active']}")
        
        # SELECT operation - filter by active
        print("\n--- Testing SELECT (filtered by is_active=True) ---")
        active_items = await TestItem.query().filter(is_active=True).all()
        print(f"✓ Found {len(active_items)} active items:")
        for item in active_items:
            data = item.to_dict()
            print(f"  - ID: {data['id']}, Name: {data['name']}")
        
        # SELECT operation - get single item
        print("\n--- Testing SELECT (single item by ID) ---")
        single_item = await TestItem.query().filter(id=2).first()
        if single_item:
            data = single_item.to_dict()
            print(f"✓ Found item: ID={data['id']}, Name={data['name']}, Value={data['value']}")
        
        # Clean up - drop test table
        print("\n--- Cleanup ---")
        await session.drop_table(TestItem)
        print("✓ Dropped test table")
    
    print("\n=== Test Complete ===\n")


if __name__ == "__main__":
    try:
        asyncio.run(test_insert_and_select())
        print("✅ All tests passed!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

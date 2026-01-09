"""
EXAMPLE 4: Resources
====================
Resources are objects that assets can use (databases, APIs, etc.)
They're defined once and shared across all assets.
"""

from dagster import (
    asset, 
    Definitions, 
    ConfigurableResource,
    AssetExecutionContext
)
import pandas as pd
import requests
from typing import List, Dict


# Define a custom resource
class APIClient(ConfigurableResource):
    """
    A resource that fetches data from an API
    """
    base_url: str = "https://jsonplaceholder.typicode.com"
    
    def get_users(self) -> List[Dict]:
        """Fetch users from API"""
        response = requests.get(f"{self.base_url}/users")
        return response.json()
    
    def get_posts(self, user_id: int) -> List[Dict]:
        """Fetch posts for a specific user"""
        response = requests.get(
            f"{self.base_url}/posts",
            params={"userId": user_id}
        )
        return response.json()


class DatabaseClient(ConfigurableResource):
    """
    A mock database client
    (In production, this would connect to a real database)
    """
    db_path: str = "./mock_db.json"
    
    def save(self, table: str, data: pd.DataFrame):
        """Save dataframe to 'database'"""
        import json
        
        # In real code, this would be: df.to_sql(table, connection)
        data_dict = data.to_dict(orient='records')
        print(f"Saved {len(data_dict)} records to table '{table}'")
        
        # Save to JSON file (simulating database)
        with open(self.db_path, 'w') as f:
            json.dump({table: data_dict}, f, indent=2)
    
    def load(self, table: str) -> pd.DataFrame:
        """Load dataframe from 'database'"""
        import json
        
        try:
            with open(self.db_path, 'r') as f:
                data = json.load(f)
                return pd.DataFrame(data.get(table, []))
        except FileNotFoundError:
            return pd.DataFrame()


# Assets using resources
@asset
def users_from_api(api_client: APIClient) -> pd.DataFrame:
    """
    Fetch users from API using the api_client resource
    """
    users = api_client.get_users()
    df = pd.DataFrame(users)
    
    # Keep only relevant columns
    df = df[['id', 'name', 'email', 'username']]
    
    print(f"Fetched {len(df)} users from API")
    return df


@asset
def user_posts(api_client: APIClient, users_from_api: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch posts for each user
    """
    all_posts = []
    
    for user_id in users_from_api['id'].head(3):  # Just first 3 users
        posts = api_client.get_posts(user_id)
        all_posts.extend(posts)
    
    df = pd.DataFrame(all_posts)
    df = df[['userId', 'id', 'title']]
    
    print(f"Fetched {len(df)} posts")
    return df


@asset
def save_to_database(
    context: AssetExecutionContext,
    db_client: DatabaseClient,
    users_from_api: pd.DataFrame,
    user_posts: pd.DataFrame
):
    """
    Save data to database using db_client resource
    """
    db_client.save("users", users_from_api)
    db_client.save("posts", user_posts)
    
    context.add_output_metadata({
        'users_saved': len(users_from_api),
        'posts_saved': len(user_posts)
    })
    
    return {"status": "success", "tables": ["users", "posts"]}


# Define resources and assets
defs = Definitions(
    assets=[users_from_api, user_posts, save_to_database],
    resources={
        "api_client": APIClient(
            base_url="https://jsonplaceholder.typicode.com"
        ),
        "db_client": DatabaseClient(
            db_path="./data/mock_db.json"
        )
    }
)


"""
HOW TO RUN:
-----------
1. Install requests: pip install requests
2. Run: dagster dev -f example_04_resources.py
3. Materialize all assets

WHAT YOU'LL SEE:
----------------
- Real data fetched from a public API
- Resources shared across multiple assets
- Data saved to a mock database (JSON file)
- Resource configuration in one place

KEY CONCEPTS:
-------------
1. Resources are configured once and injected into assets
2. Great for database connections, API clients, etc.
3. Makes testing easier (can swap in mock resources)
4. Resources can be configured per environment

PRODUCTION USE:
---------------
In production, you might have:
- DatabaseClient connecting to PostgreSQL
- APIClient with authentication tokens
- S3Client for cloud storage
- SlackClient for notifications
"""

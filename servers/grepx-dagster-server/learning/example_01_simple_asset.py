"""
EXAMPLE 1: Simple Asset
========================
This is the simplest possible Dagster example.
An asset is a piece of data that Dagster will materialize (create/update).
"""

from dagster import asset, Definitions


@asset
def my_first_asset():
    """
    This asset returns a simple string.
    When you materialize it, Dagster will execute this function.
    """
    return "Hello from Dagster! 🚀"


@asset
def my_number_asset():
    """
    Assets can return any Python object.
    """
    return 42


# Bundle all assets into Definitions
# This is what Dagster loads when you run `dagster dev`
defs = Definitions(
    assets=[my_first_asset, my_number_asset],
)


"""
HOW TO RUN:
-----------
1. Save this file as example_01_simple_asset.py
2. Run: dagster dev -f example_01_simple_asset.py
3. Open http://localhost:3000
4. Go to "Assets" tab
5. Click "Materialize all" button

WHAT YOU'LL SEE:
----------------
- Two assets in the UI: my_first_asset and my_number_asset
- You can click on each to see details
- When you materialize them, they'll turn green (success)
- Check the logs to see the execution
"""

# debug_db_simple.py - Place in src/ directory
import sys
import os
import asyncio


# ========== LOAD ENV.COMMON FILE ==========
def load_env_common():
    """Load env.common file and set environment variables"""
    env_file = r"C:\Users\USER\development\grepx-pricing-app\env.common"

    if not os.path.exists(env_file):
        print(f"❌ env.common not found at: {env_file}")
        return False

    print(f"📄 Loading environment from: {env_file}")

    # Set PROJECT_ROOT first (needed for expansion)
    project_root = r"C:\Users\USER\development\grepx-pricing-app"
    os.environ['PROJECT_ROOT'] = project_root
    print(f"   PROJECT_ROOT: {project_root}")

    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # Expand ${PROJECT_ROOT}
                if '${PROJECT_ROOT}' in value:
                    value = value.replace('${PROJECT_ROOT}', project_root)

                # Set environment variable
                os.environ[key] = value
                print(f"   {key}: {value}")

    return True


# Load environment
load_env_common()

print(f"\n✅ Environment loaded:")
print(f"   PROJECT_ROOT: {os.environ.get('PROJECT_ROOT', 'NOT SET')}")
print(f"   GREXPX_MASTER_DB_URL: {os.environ.get('GREXPX_MASTER_DB_URL', 'NOT SET')}")
print(f"   GREPX_MASTER_DB_URL: {os.environ.get('GREPX_MASTER_DB_URL', 'NOT SET')}")

# ========== FIX PYTHON PATHS ==========
current_dir = os.path.dirname(os.path.abspath(__file__))  # src/
parent_dir = os.path.dirname(current_dir)  # project root

# Add ALL necessary paths
sys.path.insert(0, current_dir)  # src/
sys.path.insert(0, os.path.join(current_dir, 'main'))  # src/main/
sys.path.insert(0, parent_dir)  # project root
sys.path.insert(0, os.path.join(parent_dir, 'libs', 'grepx-orm', 'src'))  # grepx-orm
sys.path.insert(0, os.path.join(parent_dir, 'libs', 'grepx-connection-registry', 'src'))  # connection registry

print(f"\n🔍 Python paths (first 10):")
for i, path in enumerate(sys.path[:10]):
    print(f"  {i}: {path}")


# ========== DEBUG FUNCTION ==========
async def debug_database():
    """Debug database initialization"""
    print("\n" + "=" * 50)
    print("🔧 DEBUGGING DATABASE INITIALIZATION...")

    try:
        # Step 1: Try to import dbManager first (to check if it's the issue)
        print("\n1️⃣ Testing imports...")
        try:
            import dbManager
            print("   ✅ dbManager imported successfully")
        except ImportError as e:
            print(f"   ❌ dbManager import failed: {e}")
            print("   Looking for dbManager module...")

            # Search for dbManager
            for path in sys.path:
                dbm_path = os.path.join(path, 'dbManager')
                if os.path.exists(dbm_path) or os.path.exists(dbm_path + '.py'):
                    print(f"   Found dbManager at: {dbm_path}")
                    break
            else:
                print("   Could not find dbManager anywhere!")

        # Step 2: Import DatabaseServer
        print("\n2️⃣ Importing DatabaseServer...")
        try:
            from main.database_server import DatabaseServer
            print("   ✅ DatabaseServer imported successfully")
        except ImportError as e:
            print(f"   ❌ DatabaseServer import failed: {e}")
            print("   Trying alternative import...")

            # Try direct import
            import importlib.util
            db_server_path = os.path.join(current_dir, 'main', 'database_server.py')
            if os.path.exists(db_server_path):
                print(f"   Found database_server.py at: {db_server_path}")

                # Manually load the module
                spec = importlib.util.spec_from_file_location("database_server", db_server_path)
                module = importlib.util.module_from_spec(spec)
                sys.modules["database_server"] = module
                spec.loader.exec_module(module)

                DatabaseServer = module.DatabaseServer
                print("   ✅ Manually loaded DatabaseServer")
            else:
                print(f"   ❌ Could not find database_server.py")
                return None

        # Step 3: Create instance
        print("\n3️⃣ Creating DatabaseServer instance...")
        server = DatabaseServer()
        print(f"   ✅ Created: {type(server).__name__}")

        # Step 4: Check config
        print("\n4️⃣ Checking configuration...")
        if hasattr(server, 'config'):
            config = server.config

            # Try to get as dict
            if hasattr(config, 'get'):
                conn_str = config.get('connection_string')
                print(f"   connection_string from config.get(): {conn_str}")
            elif isinstance(config, dict):
                conn_str = config.get('connection_string')
                print(f"   connection_string from dict: {conn_str}")
            else:
                print(f"   Config is type: {type(config)}")
                print(f"   Config: {config}")
        else:
            print("   ❌ Server has no 'config' attribute")

        # Step 5: Initialize
        print("\n5️⃣ Initializing server...")
        print("   ⚠️ SET BREAKPOINT ON NEXT LINE! ⚠️")

        # PUT YOUR BREAKPOINT HERE IN PYCHARM!
        await server.initialize()

        print("   ✅ Initialization complete!")
        return server

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None


# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    print("\n🚀 STARTING DEBUG SESSION")

    # Run the debug
    result = asyncio.run(debug_database())

    print("\n" + "=" * 50)
    if result:
        print("🎉 DEBUG COMPLETE - Server is working!")
        print(f"Final server object: {result}")
    else:
        print("💥 DEBUG FAILED")

    print("\nPress Enter to exit...")
    input()
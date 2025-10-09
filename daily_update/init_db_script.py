import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from StockDownloader.src.utils.db_utils import initialize_database_if_needed
from StockDownloader.src.core.config import config # Ensure config is loaded for DB_URL

print("Initializing database...")
initialize_database_if_needed()
print("Database initialization complete.")
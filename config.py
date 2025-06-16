import os
import logging
from typing import Optional, Dict, Any
import asyncio

# Get base URL from environment variable or use default
API_BASE_URL = os.environ.get("API_BASE_URL", "https://simulation-app.delightfultree-1857026e.westus.azurecontainerapps.io")
API_BASE_URL = API_BASE_URL.rstrip("/")

# Get the simulation ID from the environment variable
SIMULATION_ID = os.environ.get("SIMULATION_ID", "1234-5678")

# Simulation status constants
STATUS_FRESH = "FRESH"
STATUS_IDLE = "IDLE"
STATUS_STOPPED = "STOPPED"

# EventHub connection singleton
_EVENT_HUB_CLIENT = None
_EVENT_HUB_LOCK = asyncio.Lock()  # Lock to prevent race conditions when creating client

# EventHub configuration
EVENT_HUB_CONN_STR = os.environ.get("EventHubConnectionString")
EVENT_HUB_NAME = os.environ.get("EventHubName")

# Cache to store if we're in a development environment
_IS_DEV_ENV = None

def is_dev_environment() -> bool:
    """
    Check if the function is running in a development environment.
    This is useful for conditionally skipping certain operations in development.
    """
    global _IS_DEV_ENV
    if _IS_DEV_ENV is None:
        import sys
        _IS_DEV_ENV = ("local" in sys.executable.lower() or 
                       "development" in os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT", "").lower())
    return _IS_DEV_ENV

async def get_eventhub_client():
    """
    Get or create the Event Hub Producer Client singleton.
    This avoids creating a new connection for each function call.
    Thread-safe with asyncio lock to prevent race conditions.
    """
    global _EVENT_HUB_CLIENT
    
    # If we already have a client, return it
    if _EVENT_HUB_CLIENT:
        return _EVENT_HUB_CLIENT
    
    # Skip if we don't have connection details
    if not EVENT_HUB_CONN_STR or not EVENT_HUB_NAME:
        if is_dev_environment():
            # In development, log a warning but don't fail
            logging.warning("EventHub connection string or name not provided. Skipping EventHub initialization.")
            return None
        else:
            # In production, log an error
            logging.error("EventHub connection string or name not provided. EventHub will not be available.")
            return None
    
    # Use lock to prevent multiple clients being created at once
    async with _EVENT_HUB_LOCK:
        # Check again inside the lock
        if not _EVENT_HUB_CLIENT:
            try:
                # Import here to avoid circular imports
                from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient
                
                _EVENT_HUB_CLIENT = AsyncEventHubProducerClient.from_connection_string(
                    conn_str=EVENT_HUB_CONN_STR,
                    eventhub_name=EVENT_HUB_NAME
                )
                
                logging.info("EventHub client initialized successfully")
            except Exception as ex:
                logging.error(f"Failed to initialize EventHub client: {str(ex)}")
                # Re-raise in production, swallow in development
                if not is_dev_environment():
                    raise
    
    return _EVENT_HUB_CLIENT

async def cleanup_eventhub_client():
    """
    Properly close the Event Hub client when shutting down.
    This should be called during function app shutdown.
    """
    global _EVENT_HUB_CLIENT
    if _EVENT_HUB_CLIENT:
        try:
            await _EVENT_HUB_CLIENT.close()
            logging.info("EventHub client closed successfully")
        except Exception as ex:
            logging.error(f"Error closing EventHub client: {str(ex)}")
        finally:
            _EVENT_HUB_CLIENT = None

import os
from typing import Optional

# Get base URL from environment variable or use default
API_BASE_URL = os.environ.get("API_BASE_URL", "https://simulation-app.delightfultree-1857026e.westus.azurecontainerapps.io")
API_BASE_URL = API_BASE_URL.rstrip("/")

# Get the simulation ID from the environment variable
SIMULATION_ID = os.environ.get("SIMULATION_ID", "1234-5678")

# Simulation status
STATUS_FRESH = "FRESH"
STATUS_IDLE = "IDLE"
STATUS_STOPPED = "STOPPED"

# EventHub connection singleton
_EVENT_HUB_CLIENT = None

# EventHub configuration
EVENT_HUB_CONN_STR = os.environ.get("EventHubConnectionString")
EVENT_HUB_NAME = os.environ.get("EventHubName")

async def get_eventhub_client():
    """
    Get or create the Event Hub Producer Client singleton.
    This avoids creating a new connection for each function call.
    """
    global _EVENT_HUB_CLIENT
    
    # Import here to avoid circular imports
    from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient
    
    if not _EVENT_HUB_CLIENT and EVENT_HUB_CONN_STR and EVENT_HUB_NAME:
        _EVENT_HUB_CLIENT = AsyncEventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONN_STR,
            eventhub_name=EVENT_HUB_NAME
        )
    
    return _EVENT_HUB_CLIENT

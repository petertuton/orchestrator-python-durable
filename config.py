import os

# Get base URL from environment variable or use default
API_BASE_URL = os.environ.get("API_BASE_URL", "https://simulation-app.delightfultree-1857026e.westus.azurecontainerapps.io")
API_BASE_URL = API_BASE_URL.rstrip("/")

# Get the simulation ID from the environment variable
SIMULATION_ID = os.environ.get("SIMULATION_ID", "1234-5678")

# Simulation status
STATUS_FRESH = "FRESH"
STATUS_IDLE = "IDLE"
STATUS_STOPPED = "STOPPED"

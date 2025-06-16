import logging
import aiohttp
import json
import os
import sys
import asyncio
from typing import Dict, Any, Optional, Callable, Union, TypeVar
from functools import wraps

# Import app from function_app instead of creating a new instance
from config import API_BASE_URL, get_eventhub_client, EVENT_HUB_CONN_STR, EVENT_HUB_NAME

# Import app and logger from function_app after they are defined there
# This circular import is intentional and will be resolved at runtime
from function_app import app, logger

# Import Azure Event Hub SDK
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient

# Create a shared aiohttp session for reuse across functions
# Will be initialized lazily when needed
_http_session = None

# Type variable for generic return type
T = TypeVar('T')

# Maximum number of retries for HTTP requests
MAX_RETRIES = 3
# Base delay for exponential backoff (in seconds)
BASE_DELAY = 1

async def get_http_session() -> aiohttp.ClientSession:
    """
    Returns a shared aiohttp ClientSession to reuse across function calls.
    This improves performance by reusing connections.
    """
    global _http_session
    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
        _http_session = aiohttp.ClientSession(timeout=timeout)
    return _http_session

async def with_retry(func: Callable[[], T]) -> T:
    """
    Wrapper for implementing retry logic with exponential backoff
    for transient failures in HTTP requests.
    """
    retry_count = 0
    last_exception = None
    
    while retry_count < MAX_RETRIES:
        try:
            return await func()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retry_count += 1
            last_exception = e
            
            if retry_count < MAX_RETRIES:
                # Calculate backoff time with jitter to avoid thundering herd
                delay = BASE_DELAY * (2 ** (retry_count - 1)) * (0.9 + 0.2 * asyncio.get_event_loop().time() % 1)
                logger.warning(f"Request failed, retrying in {delay:.2f}s (attempt {retry_count}/{MAX_RETRIES}): {str(e)}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Request failed after {MAX_RETRIES} attempts: {str(e)}")
                raise
        except Exception as e:
            # Don't retry for non-transient errors
            logger.error(f"Non-transient error: {str(e)}")
            raise
    
    # This should not be reached, but just in case
    if last_exception:
        raise last_exception
    raise Exception(f"Request failed after {MAX_RETRIES} retries")

async def make_api_request(
    method: str,
    url: str,
    json_data: Optional[Dict] = None,
    headers: Optional[Dict] = None
) -> Dict:
    """
    Reusable function to make API requests with retry logic
    """
    session = await get_http_session()
    default_headers = {'Content-Type': 'application/json'}
    if headers:
        default_headers.update(headers)
    
    async def _make_request():
        async with getattr(session, method.lower())(
            url,
            json=json_data,
            headers=default_headers
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_msg = f"API request failed: {response.status}"
                try:
                    error_details = await response.text()
                    error_msg += f" - {error_details}"
                except:
                    pass
                raise Exception(error_msg)
    
    return await with_retry(_make_request)

################################
# Activity Triggers
################################

@app.activity_trigger(input_name="simulationId")
async def get_schematic_graph(simulationId: str):
    """
    Activity to retrieve the schematic graph for a simulation.
    """
    logger.info(f"Getting schematic graph for simulation: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/schematic/graph"
        data = await make_api_request("get", url)
        logger.info(f"Successfully retrieved schematic graph for: {simulationId}")
        return data
    except Exception as ex:
        logger.error(f"Exception in get_schematic_graph: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def start_simulation(simulationId: str):
    """
    Activity to start a simulation.
    """
    logger.info(f"Starting simulation: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/start"
        data = await make_api_request("put", url)
        result = data.get('result')
        logger.info(f"Successfully started simulation: {simulationId}")
        return result
    except Exception as ex:
        logger.error(f"Exception in start_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="context")
async def transform_event_data(context: Dict[str, Any]):
    """
    Activity to transform event data with schematic graph information.
    """
    event_data = context["cloud_event"]
    schematic_graph = context.get("schematic_graph", {})
    logger.info(f"Transform event data - SimulationInformation value: {schematic_graph}")
    
    try:
        # TODO: Implement transformation logic
        transformed_data = {
            "data": event_data.get('data', {}),
            "schematic_graph": schematic_graph
        }
        return transformed_data
    except Exception as ex:
        logger.error(f"Error transforming event data: {str(ex)}")
        raise

@app.activity_trigger(input_name="context")
async def input_data_activity(context: Dict[str, Any]):
    """
    Activity to input data to a simulation.
    """
    logger.info(f"Inputting data to simulation")
    try:
        simulation_id = context.get('simulationId')
        input_data = context.get('inputData')
        if not simulation_id or not input_data:
            raise ValueError("Missing required data: simulationId or inputData")
            
        url = f"{API_BASE_URL}/simulations/{simulation_id}/input/data"
        data = await make_api_request("put", url, json_data=input_data)
        logger.info(f"Successfully input data to simulation: {simulation_id}")
        return True
    except Exception as ex:
        logger.error(f"Exception in input_data: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def resume_simulation(simulationId: str):
    """
    Activity to resume a paused simulation.
    """
    logger.info(f"Resuming simulation: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/resume"
        data = await make_api_request("put", url)
        result = data.get('result')
        logger.info(f"Successfully resumed simulation: {simulationId}")
        return result
    except Exception as ex:
        logger.error(f"Exception in resume_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def get_simulation_status(simulationId: str):
    """
    Activity to check the status of a simulation.
    """
    logger.info(f"Getting status for simulation: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/status"
        data = await make_api_request("get", url)
        status = data.get('status')
        logger.info(f"Simulation {simulationId} status: {status}")
        return status
    except Exception as ex:
        logger.error(f"Exception in get_simulation_status: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def get_simulation_results(simulationId: str):
    """
    Activity to retrieve results from a simulation.
    """
    logger.info(f"Getting simulation results: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/results"
        data = await make_api_request("get", url)
        logger.info(f"Successfully retrieved simulation results: {simulationId}")
        return data
    except Exception as ex:
        logger.error(f"Exception in get_simulation_results: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def stop_simulation(simulationId: str):
    """
    Activity to stop a running simulation.
    """
    logger.info(f"Stopping simulation: {simulationId}")
    try:
        url = f"{API_BASE_URL}/simulations/{simulationId}/stop"
        data = await make_api_request("put", url)
        result = data.get('result')
        logger.info(f"Successfully stopped simulation: {simulationId}")
        return result
    except Exception as ex:
        logger.error(f"Exception in stop_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="results")
async def send_to_event_hub(results: Dict[str, Any]):
    """
    Activity to send simulation results to Azure Event Hub using the Azure SDK.
    Uses a singleton Event Hub client for better performance.
    """
    from config import is_dev_environment
    
    logger.info("Sending simulation results to Azure Event Hub")
    try:
        # Get the singleton Event Hub client
        producer = await get_eventhub_client()
        
        if not producer:
            logger.error("EventHub client could not be initialized. Check connection string and event hub name.")
            # Don't raise error for missing connection in development environment
            if is_dev_environment():
                logger.warning("Running in local/development environment - continuing without Event Hub")
                return True
            raise ValueError("EventHub client could not be initialized")

        # Format the results as JSON string for sending
        result_json = json.dumps(results)
        
        # Create a batch and add event - more efficient than sending individual events
        event_data_batch = await producer.create_batch()
        
        # Add event to the batch
        event_data_batch.add(EventData(result_json))
        
        # Send the batch of events to the event hub
        await producer.send_batch(event_data_batch)
        
        logger.info("Successfully sent simulation results to Event Hub")
        return True
            
    except ValueError as ve:
        logger.error(f"ValueError in send_to_event_hub: {str(ve)}")
        # Don't raise error for missing connection string in development
        if is_dev_environment():
            logger.warning("Running in local/development environment - continuing without Event Hub")
            return True
        raise
    except Exception as ex:
        logger.error(f"Error sending results to Event Hub: {str(ex)}")
        raise

# Cleanup function to properly close shared resources
async def cleanup_resources():
    """
    Clean up resources like HTTP sessions when the application shuts down.
    This should be called during function app shutdown.
    """
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None


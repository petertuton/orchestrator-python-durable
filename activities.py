import logging
import aiohttp
import json
import os
import sys
from typing import Dict, Any

# Import app from function_app instead of creating a new instance
from config import API_BASE_URL

# Import app and logger from function_app after they are defined there
# This circular import is intentional and will be resolved at runtime
from function_app import app, logger

# Import Azure Event Hub SDK
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient as AsyncEventHubProducerClient

################################
# Activity Triggers
################################

@app.activity_trigger(input_name="simulationId")
async def get_schematic_graph(simulationId: str):
    logger.info(f"Getting schematic graph for simulation: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/simulations/{simulationId}/schematic/graph") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved schematic graph for: {simulationId}")
                    return data
                else:
                    error_msg = f"Failed to get schematic graph: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in get_schematic_graph: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def start_simulation(simulationId: str):
    logger.info(f"Starting simulation: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{API_BASE_URL}/simulations/{simulationId}/start") as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get('result')
                    logger.info(f"Successfully started simulation: {simulationId}")
                    return result
                else:
                    error_msg = f"Failed to start simulation: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in start_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="context")
async def transform_event_data(context: Dict[str, Any]):
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
async def input_data(context: Dict[str, Any]):
    logger.info(f"Inputting data to simulation")
    try:
        simulation_id = context.get('simulationId')
        input_data = context.get('inputData')
        if not simulation_id or not input_data:
            raise ValueError("Missing required data: simulationId or inputData")
            
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{API_BASE_URL}/simulations/{simulation_id}/input/data", json=input_data) as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get('result')
                    logger.info(f"Successfully input data to simulation: {simulation_id}")
                    return result
                else:
                    error_msg = f"Failed to input data to simulation: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in input_data: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def resume_simulation(simulationId: str):
    logger.info(f"Resuming simulation: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{API_BASE_URL}/simulations/{simulationId}/resume") as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get('result')
                    logger.info(f"Successfully resumed simulation: {simulationId}")
                    return result
                else:
                    error_msg = f"Failed to resume simulation: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in resume_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def get_simulation_status(simulationId: str):
    logger.info(f"Getting status for simulation: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/simulations/{simulationId}/status") as response:
                if response.status == 200:
                    data = await response.json()
                    status = data.get('status')
                    logger.info(f"Simulation {simulationId} status: {status}")
                    return status
                else:
                    error_msg = f"Failed to get simulation status: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in get_simulation_status: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def get_simulation_results(simulationId: str):
    logger.info(f"Getting simulation results: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API_BASE_URL}/simulations/{simulationId}/results") as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved simulation results: {simulationId}")
                    return data
                else:
                    error_msg = f"Failed to get simulation results: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in get_simulation_results: {str(ex)}")
        raise

@app.activity_trigger(input_name="simulationId")
async def stop_simulation(simulationId: str):
    logger.info(f"Stopping simulation: {simulationId}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{API_BASE_URL}/simulations/{simulationId}/stop") as response:
                if response.status == 200:
                    data = await response.json()
                    result = data.get('result')
                    logger.info(f"Successfully stopped simulation: {simulationId}")
                    return result
                else:
                    error_msg = f"Failed to stop simulation: {response.status}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
    except Exception as ex:
        logging.error(f"Exception in stop_simulation: {str(ex)}")
        raise

@app.activity_trigger(input_name="results")
async def send_to_event_hub(results: Dict[str, Any]):
    """
    Activity to send simulation results to Azure Event Hub using the Azure SDK
    """
    logger.info("Sending simulation results to Azure Event Hub")
    try:
        # Get connection string from environment variables or config
        connection_string = os.environ.get("EventHubConnectionString")
        event_hub_name = os.environ.get("EventHubName")
        
        if not connection_string:
            logger.error("EventHubConnectionString not found in environment variables")
            raise ValueError("EventHubConnectionString not found")

        # Format the results as JSON string for sending
        result_json = json.dumps(results)
        
        # Create an async Event Hub producer client
        async with AsyncEventHubProducerClient.from_connection_string(
            conn_str=connection_string,
            eventhub_name=event_hub_name
        ) as producer:
            # Create a batch
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
        if "local" in sys.executable.lower() or "development" in os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT", "").lower():
            logger.warning("Running in local/development environment - continuing without Event Hub")
            return True
        raise
    except Exception as ex:
        logger.error(f"Error sending results to Event Hub: {str(ex)}")
        raise

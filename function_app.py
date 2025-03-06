import azure.functions as func
import azure.durable_functions as df
import datetime
import requests
import os
import logging
import json
from azure.core.messaging import CloudEvent

# Get base URL from environment variable or use default
API_BASE_URL = os.environ.get("API_BASE_URL", "https://simulation-app.delightfultree-1857026e.westus.azurecontainerapps.io")
# Remove trailing slash if present
API_BASE_URL = API_BASE_URL.rstrip("/")

# Get the simulation ID from the environment variable
SIMULATION_ID = os.environ.get("SIMULATION_ID", "1234-5678")

# Constants
STATUS_FRESH = "FRESH"
STATUS_IDLE = "IDLE"
STATUS_STOPPED = "STOPPED"

# Create a Durable Functions object
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Get a logger instance
logger = logging.getLogger(__name__)

############################################################################################################
# HTTP triggers

# Start the simulation
# TODO: Remove variable function name
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    try:
        function_name = req.route_params.get('functionName')
        logger.info(f"Orchestrator function name from route params: {function_name}")
       
        instance_id = await client.start_new(function_name, SIMULATION_ID, None)
        logger.info(f"Started orchestration with ID: {instance_id}")
        response = client.create_check_status_response(req, instance_id)
        
        return response
    except Exception as e:
        logger.error(f"Error in http_start: {str(e)}")
        return func.HttpResponse(
            body=f"Error starting orchestration: {str(e)}",
            status_code=500
        )

# Stop the simulation
@app.route(route="simulations/{instance_id}/stop")
@app.durable_client_input(client_name="client")
async def http_stop(req: func.HttpRequest, client):
    # Get instance_id from route parameters
    instance_id = req.route_params.get('instance_id')
    logger.info(f"Received request to stop simulation: {instance_id}")
    try:
        # Raise the SimulationStopped event with the result
        await client.raise_event(instance_id, "stop")
        logger.info(f"Stop event sent to simulation instance: {instance_id}")

        # Delete the simulation information
        entityId = df.EntityId("SimulationInformation", "mySimulationInformation")
        client.signal_entity(entityId, "delete")
        logger.info(f"Cleared simulation information")

        # Delete IsFirstEvent flag

        # Terminate the orchestration instance
        await client.terminate(instance_id, "Stopped by user")
        
        return func.HttpResponse(
            body=f"Stop event sent to simulation instance: {instance_id}",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Failed to send stop event to {instance_id}: {str(e)}")
        return func.HttpResponse(
            body=f"Failed to send stop event: {str(e)}",
            status_code=500
        )

# Wwbhook to process simulation events from Event Grid
@app.route(route="SimulationLoopWebhook", methods=["POST", "OPTIONS"])
@app.durable_client_input(client_name="client")
@app.event_hub_output(arg_name="eventHubOutput", 
                     event_hub_name="digital-twin", 
                     connection="EventHubConnectionString")
async def simulation_loop_webhook(req: func.HttpRequest, client, eventHubOutput: func.Out[str]) -> func.HttpResponse:
    if req.method.upper() == "OPTIONS":
        return func.HttpResponse(
            status_code=200,
            headers={
                "WebHook-Allowed-Origin": "*",
                "WebHook-Allowed-Rate": "*"
            }
        )
    
    try:
        body = req.get_body().decode()
        if not body:
            return func.HttpResponse(
                json.dumps({"message": "Request body is empty"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Retrieve the simulation information from an entity
        entityId = df.EntityId("SimulationInformation", "mySimulationInformation")
        entity_state = await client.read_entity_state(entityId)
        simulation_information = entity_state.entity_state
        logger.info(f"Retrieved simulation information: {simulation_information}")

        # TODO: Transform the message

        # Check and set the IsFirstEvent flag
        isFirstEvent = await client.read_entity_state(df.EntityId("IsFirstEvent", "myIsFirstEvent"))
        if not isFirstEvent.entity_state:
            await client.signal_entity(df.EntityId("IsFirstEvent", "myIsFirstEvent"), "set", True)
            logger.info("Set IsFirstEvent flag")
            # TODO: Set the "Time" value to 0

        # Or, should we just use a counter, based on a provided inteval?? 

        # TODO: Process the simulation event
        # cloud_event = CloudEvent.from_json(body)
        # results = await process_simulation_event(cloud_event)
        results = body
        
        # Set the Event Hub output
        eventHubOutput.set(results)
        
        return func.HttpResponse(
            results,
            status_code=200,
            mimetype="application/json"
        )
    except Exception as ex:
        logger.error(f"Error in simulation_loop_webhook: {str(ex)}")
        return func.HttpResponse(
            json.dumps({"message": f"Error processing request: {str(ex)}"}), 
            status_code=500,
            mimetype="application/json"
        )

############################################################################################################
# Orchestrator

@app.orchestration_trigger(context_name="context")
def Simulation(context):
    logger.info(f"Starting simulation orchestration: {context.instance_id}")
    # simulation_id = yield context.call_activity("GetSimulationId", None)
    simulation_id = SIMULATION_ID
    # logger.info(f"Retrieved simulation ID: {simulation_id}")

    # Get the simulation information
    simulation_information = yield context.call_activity('GetSimulationInformation', simulation_id)
    logger.info(f"Retrieved simulation information for simulation: {simulation_id}")

    # Store the simulation information
    entityId = df.EntityId("SimulationInformation", "mySimulationInformation")
    context.signal_entity(entityId, "set", simulation_information)
    logger.info(f"Stored simulation information in entity: {simulation_information}")

    # Get the simulation status
    status = yield context.call_activity('GetSimulationStatus', simulation_id)
    logger.info(f"Current simulation status: {status}")

    # If the simulation is fresh, start it
    # TODO: Change this to STATUS_FRESH when using the proper API
    if status == STATUS_IDLE: 
        logger.info(f"Starting simulation: {simulation_id}")
        yield context.call_activity('StartSimulation', simulation_id)
        timeout = context.current_utc_datetime + datetime.timedelta(days=1)
        logger.info(f"Waiting for stop event or timeout until {timeout}")
        
        stop_event_task = context.wait_for_external_event("stop")
        timeout_task = context.create_timer(timeout)
        winning_task = yield context.task_any([stop_event_task, timeout_task])

        if not timeout_task.is_completed:
            timeout_task.cancel()
            logger.info("Timer canceled, stop event received")
        else:
            logger.info("Simulation timed out")

        status = "completed"
        logger.info(f"Stopping simulation: {simulation_id}")
        yield context.call_activity('StopSimulation', simulation_id)

    result = {
        "id": simulation_id,
        "schematic": simulation_information,
        "status": status
    }
    logger.info(f"Completed simulation orchestration: {context.instance_id}")
    return result

############################################################################################################
# Activities

# Get the simulation ID - not called
@app.activity_trigger(input_name="ignored")
def GetSimulationId(ignored: str):
    logger.info(f"Getting simulation ID")

    try:
        response = requests.get(f"{API_BASE_URL}/simulations")
        if response.status_code == 200:
            data = response.json()
            # Handle response format: array of objects with uuid field
            if isinstance(data, list) and len(data) > 0:
                simulation_id = data[0].get('uuid')
                logger.info(f"Retrieved simulation ID: {simulation_id}")
                return simulation_id
            else:
                error_msg = "Failed to get simulation's UUID: unexpected response format"
                logger.error(error_msg)
                raise Exception(error_msg)
        else:
            error_msg = f"Failed to get simulation's UUID: {response.status_code}"
            logger.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        logger.error(f"Exception in GetSimulationId: {str(e)}")
        raise

# Get the schematic information
@app.activity_trigger(input_name="simulationId")
def GetSimulationInformation(simulationId: str):
    logger.info(f"Getting schematic information for simulation: {simulationId}")
    try:
        response = requests.get(f"{API_BASE_URL}/simulations/{simulationId}/schematic/graph")
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully retrieved schematic information for: {simulationId}")
            return data
        else:
            error_msg = f"Failed to get schematic graph: {response.status_code}"
            logging.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception in GetSchematicInformation: {str(e)}")
        raise

# Get the simulation status
@app.activity_trigger(input_name="simulationId")
def GetSimulationStatus(simulationId: str):
    logger.info(f"Getting status for simulation: {simulationId}")
    try:
        response = requests.get(f"{API_BASE_URL}/simulations/{simulationId}/status")
        if response.status_code == 200:
            data = response.json()
            status = data.get('status')
            logger.info(f"Simulation {simulationId} status: {status}")
            return status
        else:
            error_msg = f"Failed to get simulation status: {response.status_code}"
            logging.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception in GetSimulationStatus: {str(e)}")
        raise

# Start the simulation
@app.activity_trigger(input_name="simulationId")
def StartSimulation(simulationId: str):
    logger.info(f"Starting simulation: {simulationId}")
    try:
        response = requests.put(f"{API_BASE_URL}/simulations/{simulationId}/start")
        if response.status_code == 200:
            data = response.json()
            result = data.get('result')
            logger.info(f"Successfully started simulation: {simulationId}")
            return result
        else:
            error_msg = f"Failed to start simulation: {response.status_code}"
            logging.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception in StartSimulation: {str(e)}")
        raise

# Stop the simulation
@app.activity_trigger(input_name="simulationId")
def StopSimulation(simulationId: str):
    logger.info(f"Stopping simulation: {simulationId}")
    try:
        response = requests.put(f"{API_BASE_URL}/simulations/{simulationId}/stop")
        if response.status_code == 200:
            data = response.json()
            result = data.get('result')
            logger.info(f"Successfully stopped simulation: {simulationId}")
            return result
        else:
            error_msg = f"Failed to stop simulation: {response.status_code}"
            logging.error(error_msg)
            raise Exception(error_msg)
    except Exception as e:
        logging.error(f"Exception in StopSimulation: {str(e)}")
        raise

############################################################################################################
# Entities

# Entity to store the simulation ID - not used
@app.entity_trigger(context_name="context")
def SimulationId(context):
    logger.info(f"SimulationId entity: {context.operation_name}")
    operation = context.operation_name
    if operation == "get":
        logger.info("Getting SimulationId")
        context.set_result(context.get_state(lambda: None))
    elif operation == "set":
        logger.info("Setting SimulationId")
        context.set_state(context.get_input())
    elif operation == "delete":
        logger.info("Deleting SimulationId")
        context.set_state(None)

# Entity to store the simulation information
@app.entity_trigger(context_name="context")
def SimulationInformation(context):
    logger.info(f"SimulationInformation entity: {context.operation_name}")
    operation = context.operation_name
    if operation == "get":
        logger.info("Getting SimulationInformation")
        context.set_result(context.get_state(lambda: None))
    elif operation == "set":
        logger.info("Setting SimulationInformation")
        context.set_state(context.get_input())
    elif operation == "delete":
        logger.info("Deleting SimulationInformation")
        context.set_state(None)

# Entity to store the IsFirstEvent flag
@app.entity_trigger(context_name="context")
def IsFirstEvent(context):
    logger.info(f"IsFirstEvent entity: {context.operation_name}")
    operation = context.operation_name
    if operation == "get":
        logger.info("Getting SimulationInformation")
        context.set_result(context.get_state(lambda: None))
    elif operation == "set":
        logger.info("Setting SimulationInformation")
        context.set_state(context.get_input())
    elif operation == "delete":
        logger.info("Deleting SimulationInformation")
        context.set_state(None)

############################################################################################################
# Samples...

# Orchestrator
@app.orchestration_trigger(context_name="context")
def hello_orchestrator(context):
    logger.info("Starting hello_orchestrator")
    result1 = yield context.call_activity("hello", "Seattle")
    result2 = yield context.call_activity("hello", "Tokyo")
    result3 = yield context.call_activity("hello", "London")
    logger.info("Completed hello_orchestrator")
    return [result1, result2, result3]

# Activity
@app.activity_trigger(input_name="city")
def hello(city: str):
    logger.info(f"Hello activity called with city: {city}")
    return f"Hello {city}"

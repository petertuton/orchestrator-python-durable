import azure.functions as func
import azure.durable_functions as df
import datetime
import logging
import json
import asyncio
import uuid
import functools
from typing import Dict, Any, Optional, Callable, TypeVar, Awaitable
from config import SIMULATION_ID, STATUS_FRESH, STATUS_IDLE, STATUS_STOPPED

################################
# Create a Durable Functions object
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Get a logger instance and configure more efficient logging
logger = logging.getLogger(__name__)

# Cache decorator for expensive operations
T = TypeVar('T')
def async_cache(ttl_seconds: int = 60):
    """
    Decorator to cache async function results for a specified time.
    
    Args:
        ttl_seconds: Time to live for cached results in seconds
    """
    cache = {}
    
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Create a cache key from the function name and arguments
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            now = datetime.datetime.now()
            
            # Check if result is in cache and not expired
            if key in cache:
                result, timestamp = cache[key]
                if (now - timestamp).total_seconds() < ttl_seconds:
                    logger.debug(f"Cache hit for {func.__name__}")
                    return result
            
            # Call the function and cache the result
            result = await func(*args, **kwargs)
            cache[key] = (result, now)
            return result
        return wrapper
    return decorator

# Import entities (must be done after app and logger are defined)
import entities

# Import activities (must be done after app and logger are defined)
import activities

##################################


################################
# HTTP triggers
################################

# HTTP request to start the simulation with the given simulation id
@app.route(route="simulations/{simulation_id}/start")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to start a simulation.
    
    Args:
        req: The HTTP request
        client: The durable client injected by the runtime
        
    Returns:
        HTTP response with status of the operation
    """
    # Assert the simulation id from the route is correct for this simulation
    simulation_id = req.route_params.get('simulation_id')
    logger.info(f"Received request to start simulation: {simulation_id}")
    
    if simulation_id != SIMULATION_ID:
        error_msg = f"Invalid simulation id: {simulation_id}. Expected: {SIMULATION_ID}"
        logger.warning(error_msg)
        return func.HttpResponse(
            body=json.dumps({"error": error_msg}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        # Check if the simulation is already running
        status = await client.get_status(simulation_id)
        if status and status.runtime_status == df.OrchestrationRuntimeStatus.Running:
            msg = f"Simulation {simulation_id} is already running"
            logger.info(msg)
            return func.HttpResponse(
                body=json.dumps({"message": msg}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Start the simulation orchestration
        instance_id = await client.start_new("simulation_orchestrator", simulation_id, None)
        logger.info(f"Started simulation orchestration with id: {instance_id}")

        # Create and return a response
        return client.create_check_status_response(req, instance_id)
    
    except Exception as ex:
        error_msg = f"Error starting simulation {simulation_id}: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return func.HttpResponse(
            body=json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )

# HTTP request to stop the simulation with the given simulation id
@app.route(route="simulations/{simulation_id}/stop")
@app.durable_client_input(client_name="client")
async def http_stop(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to stop a simulation.
    
    Args:
        req: The HTTP request
        client: The durable client injected by the runtime
        
    Returns:
        HTTP response with status of the operation
    """
    # Assert the simulation id from the route is correct for this simulation
    simulation_id = req.route_params.get('simulation_id')
    logger.info(f"Received request to stop simulation: {simulation_id}")
    
    if simulation_id != SIMULATION_ID:
        error_msg = f"Invalid simulation id: {simulation_id}. Expected: {SIMULATION_ID}"
        logger.warning(error_msg)
        return func.HttpResponse(
            body=json.dumps({"error": error_msg}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        # Delete the entity values
        deletion_tasks = [
            client.signal_entity(df.EntityId("schematic_graph", "thisSchematicGraph"), "delete"),
            client.signal_entity(df.EntityId("first_event_time", "thisFirstEventTime"), "delete"),
            client.signal_entity(df.EntityId("input_data", "thisInputData"), "delete")
        ]
        
        # Execute entity deletions concurrently
        await asyncio.gather(*deletion_tasks)
        logger.info(f"Deleted entity values for simulation: {simulation_id}")

        # First raise the stop event to allow for graceful shutdown
        try:
            await client.raise_event(simulation_id, "stop")
            logger.info(f"Stop event sent to simulation instance: {simulation_id}")
        except Exception as ex:
            logger.warning(f"Could not raise stop event: {str(ex)}")
        
        # Then force terminate the orchestration if required
        status = await client.get_status(simulation_id)
        if status and status.runtime_status not in [df.OrchestrationRuntimeStatus.Completed, df.OrchestrationRuntimeStatus.Terminated]:
            await client.terminate(simulation_id, "Stopped by user")
            logger.info(f"Terminated orchestration instance: {simulation_id}")

        # Return a response, using default timeout of 10 seconds
        return await client.wait_for_completion_or_create_check_status_response(req, simulation_id)

    except Exception as ex:
        error_msg = f"Failed to stop simulation {simulation_id}: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return func.HttpResponse(
            body=json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )

# Webhook to process simulation events from Event Grid
@app.route(route="simulation_loop_webhook", methods=["POST", "OPTIONS"])
@app.durable_client_input(client_name="client")
async def simulation_loop_webhook(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    Webhook to process simulation events from Event Grid.
    
    Args:
        req: The HTTP request
        client: The durable client injected by the runtime
        
    Returns:
        HTTP response with status of the operation
    """
    # Respond to Event Grid validation request
    if req.method.upper() == "OPTIONS":
        return func.HttpResponse(
            status_code=200,
            headers={
                "WebHook-Allowed-Origin": "*",
                "WebHook-Allowed-Rate": "*"
            }
        )
    
    # Get the simulation status efficiently with a short timeout
    try:
        status_task = asyncio.create_task(client.get_status(SIMULATION_ID))
        status = await asyncio.wait_for(status_task, timeout=2.0)  # 2-second timeout
        
        if not status or status.runtime_status != df.OrchestrationRuntimeStatus.Running:
            error_msg = f"Simulation {SIMULATION_ID} is not running (status: {status.runtime_status if status else 'None'}). Please start the simulation first."
            logger.warning(error_msg)
            return func.HttpResponse(
                json.dumps({"error": error_msg}),
                status_code=200,
                mimetype="application/json"
            )
    except asyncio.TimeoutError:
        error_msg = f"Timeout checking status for simulation {SIMULATION_ID}"
        logger.warning(error_msg)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=408,
            mimetype="application/json"
        )
    except Exception as ex:
        error_msg = f"Error checking simulation status: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )
    
    # Process the event
    try:
        # Write the event to the input_data entity
        event_data = req.get_json()
        logger.info(f"Received event data: {event_data}")

        # Store the event data in the input_data entity with a timeout
        entity_task = asyncio.create_task(
            client.signal_entity(df.EntityId("input_data", "thisInputData"), "set", event_data)
        )
        await asyncio.wait_for(entity_task, timeout=5.0)  # 5-second timeout

        # Return 200 ok
        return func.HttpResponse(
            json.dumps({"message": "Event received and processed successfully."}), 
            status_code=200,
            mimetype="application/json"
        )
    
    except asyncio.TimeoutError:
        error_msg = f"Timeout storing event data for simulation {SIMULATION_ID}"
        logger.warning(error_msg)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=408,
            mimetype="application/json"
        )
    except Exception as ex:
        error_msg = f"Error processing webhook event: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )

# Timer trigger that runs every second
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer")
@app.durable_client_input(client_name="client")
async def timer_function(timer: func.TimerRequest, client) -> None:
    """
    Timer trigger that runs every minute to drive the simulation loop.
    Uses throttling to prevent starting multiple instances if previous runs are still active.
    
    Args:
        timer: The timer request information
        client: The durable client injected by the runtime
    """
    try:
        # Log invocation with better timestamp format
        utc_timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        logger.info(f'Timer trigger executed at: {utc_timestamp} UTC')
        
        # Generate a unique ID for this iteration
        iteration_id = str(uuid.uuid4())
        
        # Check for existing running instances and limit the number of concurrent orchestrations
        # Get the status of all orchestrations of type simulation_loop_orchestrator
        instances = await client.get_status_all()
        running_loops = [
            instance for instance in instances 
            if instance.name == "simulation_loop_orchestrator" and 
            instance.runtime_status == df.OrchestrationRuntimeStatus.Running
        ]
        
        # If there are too many running instances, skip this iteration
        MAX_CONCURRENT_LOOPS = 3  # Allow up to 3 concurrent loop orchestrations
        if len(running_loops) >= MAX_CONCURRENT_LOOPS:
            logger.warning(f"Skipping timer iteration as {len(running_loops)} simulation loop orchestrations are already running")
            return
        
        # Start the simulation loop orchestrator with a unique ID
        current_time = datetime.datetime.now(datetime.timezone.utc)
        simulation_loop_id = await client.start_new(
            "simulation_loop_orchestrator", 
            iteration_id, 
            current_time.isoformat()
        )
        logger.info(f"Started simulation loop orchestrator with id: {simulation_loop_id}")
        
    except Exception as ex:
        logger.error(f"Error in timer_function: {str(ex)}", exc_info=True)

################################
# Orchestrators
################################

# Simulation orchestrator
@app.orchestration_trigger(context_name="context")
def simulation_orchestrator(context):
    """
    Orchestrator function for the simulation workflow.
    
    This orchestrator:
    1. Gets the schematic graph for the simulation
    2. Stores the graph in an entity
    3. Checks the simulation status
    4. Starts the simulation if needed
    5. Waits for the stop event or a timeout
    6. Stops the simulation
    
    Args:
        context: The orchestration context
        
    Returns:
        Result of the simulation orchestration
    """
    logger.info(f"Starting simulation orchestrator for simulation id: {context.instance_id}")
    simulation_id = SIMULATION_ID

    try:
        # Get the schematic graph from the simulation information
        schematic_graph = yield context.call_activity('get_schematic_graph', simulation_id)
        logger.info(f"Retrieved schematic graph for simulation: {simulation_id}")

        # Store the schematic graph for use in the simulation loop
        yield context.call_entity(df.EntityId("schematic_graph", "thisSchematicGraph"), "set", schematic_graph)
        logger.debug(f"Stored schematic graph in entity")

        # Get the simulation status
        status = yield context.call_activity('get_simulation_status', simulation_id)
        logger.info(f"Simulation status: {status}")

        # If the simulation is in the expected state, start it
        if status == STATUS_IDLE:  # Using STATUS_IDLE as per the comment in the original code
            logger.info(f"Starting simulation: {simulation_id}")
            start_result = yield context.call_activity('start_simulation', simulation_id)
            
            if not start_result:
                logger.warning(f"Failed to start simulation: {simulation_id}")
                return {
                    "simulation_id": simulation_id,
                    "status": "Failed to start simulation"
                }
            
            # The simulation is now running, waiting for events to be sent via the webhook
            logger.info(f"Simulation started successfully: {simulation_id}")

            # Wait for the simulation's stop request to be raised or for a timeout to occur
            timeout_days = 1  # Configurable timeout
            timeout = context.current_utc_datetime + datetime.timedelta(days=timeout_days)
            logger.info(f"Waiting for stop event or timeout until {timeout}")
            
            # Create tasks for stop event and timeout
            stop_event_task = context.wait_for_external_event("stop")
            timeout_task = context.create_timer(timeout)
            
            # Wait for either event to occur
            winning_task = yield context.task_any([stop_event_task, timeout_task])

            # Cancel the timer if the stop event was received
            if winning_task == stop_event_task:
                timeout_task.cancel()
                logger.info("Timer canceled, stop event received")
                stop_reason = "Stopped by external event"
            else:
                logger.info(f"Simulation timed out after {timeout_days} days")
                stop_reason = f"Timed out after {timeout_days} days"

            # Call the stop simulation activity
            logger.info(f"Stopping simulation: {simulation_id}")
            stop_result = yield context.call_activity('stop_simulation', simulation_id)
            
            if not stop_result:
                logger.warning(f"Failed to stop simulation: {simulation_id}")
            
            status = "Completed"
            completion_details = stop_reason
        else:
            # Simulation is not in the expected state
            logger.warning(f"Simulation is not in the expected state (status={status}), skipping start: {simulation_id}")
            status = f"Simulation not ready (status={status}, expected={STATUS_IDLE})"
            completion_details = "Skipped due to incorrect state"

        # Prepare and return the result
        result = {
            "simulation_id": simulation_id,
            "status": status,
            "details": completion_details,
            "completed_at": context.current_utc_datetime.isoformat()
        }
        logger.info(f"Completed simulation orchestrator: {context.instance_id}")
        return result
        
    except Exception as ex:
        # Log the exception and return an error result
        error_msg = f"Error in simulation orchestrator: {str(ex)}"
        logger.error(error_msg)
        return {
            "simulation_id": simulation_id,
            "status": "Error",
            "error": error_msg,
            "completed_at": context.current_utc_datetime.isoformat()
        }

# Simulation loop orchestrator
@app.orchestration_trigger(context_name="context")
def simulation_loop_orchestrator(context):
    """
    Orchestrator function for the simulation loop.
    
    This orchestrator:
    1. Checks if the main simulation orchestrator is running
    2. Gets the first event time and calculates seconds since that time
    3. Gets the input data from the entity
    4. Updates the input data with the time
    5. Inputs the data to the simulation
    6. Resumes the simulation and waits for it to reach IDLE state
    7. Gets and returns the simulation results
    
    Args:
        context: The orchestration context
        
    Returns:
        Result of the simulation loop
    """
    logger.info(f"Starting simulation loop orchestrator: {context.instance_id}")
    simulation_id = SIMULATION_ID
    
    try:
        # First check if the simulation_orchestrator with id SIMULATION_ID is running
        orchestration_status = yield context.call_activity("get_orchestration_status", simulation_id)
        
        # Check if the status is "Running"
        if orchestration_status != "OrchestrationRuntimeStatus.Running":
            logger.warning(f"Simulation orchestrator {simulation_id} is not running (status: {orchestration_status}). Exiting simulation loop orchestrator.")
            return {
                "message": f"Simulation orchestrator {simulation_id} is not running. No activities performed.",
                "status": orchestration_status,
                "completed_at": context.current_utc_datetime.isoformat()
            }
        
        # Get the input, which is date in string ISO format from when the trigger fired
        triggerdatetime_str = context.get_input()
        
        # Convert triggerdatetime from string to datetime object
        try:
            triggerdatetime = datetime.datetime.fromisoformat(triggerdatetime_str)
        except ValueError as e:
            error_msg = f"Invalid trigger datetime format: {triggerdatetime_str}. Error: {str(e)}"
            logger.error(error_msg)
            return {
                "message": error_msg,
                "error": str(e),
                "completed_at": context.current_utc_datetime.isoformat()
            }

        # Get the first event time from the entity
        first_event_time = yield context.call_entity(df.EntityId("first_event_time", "thisFirstEventTime"), "get")
        
        # If no first event time is set, use the trigger datetime
        if not first_event_time:
            first_event_time = triggerdatetime_str
            logger.info(f"No first event time found, using trigger datetime: {first_event_time}")
            yield context.call_entity(df.EntityId("first_event_time", "thisFirstEventTime"), "set", first_event_time)
            # Since we just set first_event_time to the current time, seconds_since_first_event is 0
            seconds_since_first_event = 0.0
        else:
            # Take the number of seconds since the first event time
            try:
                first_event_time_dt = datetime.datetime.fromisoformat(first_event_time)
                seconds_since_first_event = (triggerdatetime - first_event_time_dt).total_seconds()
                logger.debug(f"Seconds since first event: {seconds_since_first_event}")
            except ValueError as e:
                error_msg = f"Invalid first event time format: {first_event_time}. Error: {str(e)}"
                logger.error(error_msg)
                return {
                    "message": error_msg,
                    "error": str(e),
                    "completed_at": context.current_utc_datetime.isoformat()
                }
        
        # Get the input data from the entity
        input_data = yield context.call_entity(df.EntityId("input_data", "thisInputData"), "get")
        
        # Handle case where input_data is None
        if input_data is None:
            logger.warning("No input data found, creating default input data")
            input_data = {
                'time': seconds_since_first_event,
                'sensor1': "0",
                'sensor2': "0"
            }
        else:
            # Update the input data with the seconds since the first event
            input_data['time'] = seconds_since_first_event

        # Input the transformed data to the simulation
        activity_input_data = {
            'simulationId': simulation_id,
            'inputData': input_data
        }
        input_result = yield context.call_activity("input_data_activity", activity_input_data)
        
        # Check if the input was successful
        if not input_result:
            logger.warning(f"Failed to input data to simulation: {simulation_id}")
            return {
                "message": f"Failed to input data to simulation: {simulation_id}",
                "error": "Input data failure",
                "completed_at": context.current_utc_datetime.isoformat()
            }

        # Resume the simulation
        resumed = yield context.call_activity("resume_simulation", simulation_id)
        if not resumed:
            logger.error(f"Failed to resume simulation: {simulation_id}")
            return {
                "message": f"Failed to resume simulation: {simulation_id}",
                "error": "Resume failure",
                "completed_at": context.current_utc_datetime.isoformat()
            }
            
        # Loop, checking for status IDLE
        status = yield context.call_activity("get_simulation_status", simulation_id)
        
        # Use a counter to prevent infinite loops
        poll_count = 0
        max_polls = 60  # Maximum number of polling attempts (60 sec = 1 minute)
        poll_interval = 1  # Seconds between polls
        
        while status != STATUS_IDLE and poll_count < max_polls:
            # Add a timer to avoid tight polling
            yield context.create_timer(context.current_utc_datetime + datetime.timedelta(seconds=poll_interval))
            status = yield context.call_activity("get_simulation_status", simulation_id)
            poll_count += 1
            logger.debug(f"Poll {poll_count}/{max_polls}: Simulation status = {status}")
            
        # Check if we timed out waiting for IDLE status
        if poll_count >= max_polls:
            logger.warning(f"Timed out waiting for simulation {simulation_id} to reach IDLE status (current status: {status})")
            return {
                "message": f"Timed out waiting for simulation to reach IDLE status",
                "status": status,
                "error": "Polling timeout",
                "completed_at": context.current_utc_datetime.isoformat()
            }
            
        # Get the simulation results
        results = yield context.call_activity("get_simulation_results", simulation_id)
        
        # Write the results to Azure Event Hub
        yield context.call_activity("send_to_event_hub", results)
        
        # Add timestamp to results
        results["completed_at"] = context.current_utc_datetime.isoformat()
        
        # Return the results
        return results
        
    except Exception as ex:
        # Log the exception and return an error result
        error_msg = f"Error in simulation loop orchestrator: {str(ex)}"
        logger.error(error_msg)
        return {
            "simulation_id": simulation_id,
            "status": "Error",
            "error": error_msg,
            "completed_at": context.current_utc_datetime.isoformat()
        }

@app.activity_trigger(input_name="instance_id")
@app.durable_client_input(client_name="client")
@async_cache(ttl_seconds=5)  # Cache results for 5 seconds to reduce repeated calls
async def get_orchestration_status(instance_id: str, client) -> str:
    """
    Activity to get the status of an orchestration.
    Includes caching to reduce the number of status checks.
    
    Args:
        instance_id: The instance id of the orchestration to check
        client: The durable client injected by the runtime
        
    Returns:
        String representation of the runtime status of the orchestration
    """
    try:
        # Get the status of the orchestration with a timeout
        status_task = asyncio.create_task(client.get_status(instance_id))
        status = await asyncio.wait_for(status_task, timeout=3.0)  # 3-second timeout
        
        # Return the runtime status as a string if status exists, otherwise "NotFound"
        if status:
            # Convert the enum to a string to make it JSON serializable
            status_str = str(status.runtime_status)
            logger.debug(f"Status for orchestration {instance_id}: {status_str}")
            return status_str
        else:
            logger.debug(f"No status found for orchestration {instance_id}")
            return "NotFound"
    except asyncio.TimeoutError:
        error_msg = f"Timeout checking orchestration status for {instance_id}"
        logger.warning(error_msg)
        return f"Error: {error_msg}"
    except Exception as ex:
        error_msg = f"Error checking orchestration status for {instance_id}: {str(ex)}"
        logger.error(error_msg, exc_info=True)
        return f"Error: {str(ex)}"


import azure.functions as func
import azure.durable_functions as df
import datetime
import logging
import json
import asyncio
import uuid
from azure.core.messaging import CloudEvent
from config import SIMULATION_ID, STATUS_FRESH, STATUS_IDLE, STATUS_STOPPED

################################
# Create a Durable Functions object
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Get a logger instance
logger = logging.getLogger(__name__)

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
async def http_start(req: func.HttpRequest, client):
    # Assert the simulation id from the route is correct for this simulation
    simulation_id = req.route_params.get('simulation_id')
    logger.info(f"Received request to start simulation: {simulation_id}")
    if simulation_id != SIMULATION_ID:
        return func.HttpResponse(
            body=f"Invalid simulation id: {req.route_params.get('simulation_id')}",
            status_code=400
        )

    try:
        # Check if the simulation is already running
        status = await client.get_status(simulation_id)
        if status and status.runtime_status == df.OrchestrationRuntimeStatus.Running:
            return func.HttpResponse(
                body=f"Simulation {simulation_id} is already running",
                status_code=400
            )
        
        # Start the simulation orchestration
        await client.start_new("simulation_orchestrator", simulation_id, None)
        logger.info(f"Started simulation orchestration with id: {simulation_id}")

        # Create and return a response
        return client.create_check_status_response(req, simulation_id)
    
    except Exception as ex:
        logger.error(f"Error in http_start: {str(ex)}")
        return func.HttpResponse(
            body=f"Error starting orchestration: {str(ex)}",
            status_code=500
        )

# HTTP request to stop the simulation with the given simulation id
@app.route(route="simulations/{simulation_id}/stop")
@app.durable_client_input(client_name="client")
async def http_stop(req: func.HttpRequest, client):
    # Assert the simulation id from the route is correct for this simulation
    simulation_id = req.route_params.get('simulation_id')
    logger.info(f"Received request to stop simulation: {simulation_id}")
    if simulation_id != SIMULATION_ID:
        return func.HttpResponse(
            body=f"Invalid simulation id: {req.route_params.get('simulation_id')}",
            status_code=400
        )

    try:
        # Delete the entity values
        await client.signal_entity(df.EntityId("schematic_graph", "thisSchematicGraph"), "delete")
        await client.signal_entity(df.EntityId("last_event_time", "thisLastEventTime"), "delete")

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

        # Return a response, using default timeout of 10 seconds
        return await client.wait_for_completion_or_create_check_status_response(req, simulation_id)

    except Exception as ex:
        logging.error(f"Failed to stop simulation {simulation_id}: {str(ex)}")
        return func.HttpResponse(
            body=f"Failed to stop simulation: {str(ex)}",
            status_code=500
        )

# Webhook to process simulation events from Event Grid
@app.route(route="simulation_loop_webhook", methods=["POST", "OPTIONS"])
@app.durable_client_input(client_name="client")
async def simulation_loop_webhook(req: func.HttpRequest, client) -> func.HttpResponse:
    # Respond to Event Grid validation request
    if req.method.upper() == "OPTIONS":
        return func.HttpResponse(
            status_code=200,
            headers={
                "WebHook-Allowed-Origin": "*",
                "WebHook-Allowed-Rate": "*"
            }
        )
    
    # Ensure that the simulation orchestrator is running
    status = await client.get_status(SIMULATION_ID)
    if status and status.runtime_status != df.OrchestrationRuntimeStatus.Running:
        return func.HttpResponse(
            json.dumps({"message": f"Simulation {SIMULATION_ID} is not running. Please start the simulation first."}),
            status_code=400,
            mimetype="application/json"
        )
    
    # Simulation is running
    # Process the event
    try:
        # Write the event to the topic_values entity
        event_data = req.get_json()
        logger.info(f"Received event data: {event_data}")

        # Store the event data's data value for the event data's subject in the topic_values entity
        topic = event_data.get("subject")
        mqtt_data = event_data.get("data")
        await client.signal_entity(df.EntityId("topics", "topics"), "add", topic)
        await client.signal_entity(df.EntityId("topic_values", topic), "set", mqtt_data)
        
        # Start the simulation loop orchestrator
        # simulation_loop_id = await client.start_new("simulation_loop_orchestrator", client_input=json.dumps(req.get_json()))

        # Return a response, using default timeout of 10 seconds
        # return await client.wait_for_completion_or_create_check_status_response(req, simulation_loop_id)
        # return 200 ok
        return func.HttpResponse(
            json.dumps({"message": "Event received and processed successfully."}), 
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

# Timer trigger that runs every second
@app.timer_trigger(schedule="*/1 * * * * *", arg_name="timer")
@app.durable_client_input(client_name="client")
async def timer_function(timer: func.TimerRequest, client) -> None:
    try:
        # Log invocation
        utc_timestamp = datetime.datetime.utcnow()
        logger.info(f'Timer trigger function executed at: {utc_timestamp}')
        
        # Start the simulation loop orchestrator with a random simulation id 
        simulation_loop_id = await client.start_new("simulation_loop_orchestrator", str(uuid.uuid4()), {utc_timestamp})
        logger.info(f"Started simulation loop orchestrator with id: {simulation_loop_id}")
        
    except Exception as ex:
        logger.error(f"Error in timer_function: {str(ex)}")

################################
# Orchestrators
################################

# Simulation orchestrator
@app.orchestration_trigger(context_name="context")
def simulation_orchestrator(context):
    logger.info(f"Starting simulation orchestrator for simulation id: {context.instance_id}")
    simulation_id = SIMULATION_ID

    # Get the schematic graph from the simulation information
    schematic_graph = yield context.call_activity('get_schematic_graph', simulation_id)
    logger.info(f"Retrieved schematic graph for simulation: {simulation_id}")

    # Store the schematic graph for use in the simulation loop
    yield context.call_entity(df.EntityId("schematic_graph", "thisSchematicGraph"), "set", schematic_graph)
    logger.info(f"\tschematic graph: {schematic_graph}")

    # Get the simulation status
    status = yield context.call_activity('get_simulation_status', simulation_id)
    logger.info(f"Simulation status: {status}")

    # If the simulation is fresh, start it
    # TODO: Change this to STATUS_FRESH when using the proper API
    if status == STATUS_IDLE: 
        logger.info(f"Starting simulation: {simulation_id}")
        yield context.call_activity('start_simulation', simulation_id)
        
        # The simulation is now running, waiting for events to be sent via the webhook

        # Wait for the simulation's stop request to be raised or for a timeout to occur
        # TODO: Set your preferred timeout value
        timeout = context.current_utc_datetime + datetime.timedelta(days=1)
        logger.info(f"Waiting for stop event or timeout until {timeout}")
        stop_event_task = context.wait_for_external_event("stop")
        timeout_task = context.create_timer(timeout)
        winning_task = yield context.task_any([stop_event_task, timeout_task])

        # Cancel the timer if the stop event was received
        if winning_task == stop_event_task:
            timeout_task.cancel()
            logger.info("Timer canceled, stop event received")
        else:
            logger.info("Simulation timed out")

        # Call the stop simulation activity
        logger.info(f"Stopping simulation: {simulation_id}")
        yield context.call_activity('stop_simulation', simulation_id)
        status = "Completed"
    else:
        # TODO: What happens if the simulation is not reporting status of FRESH?
        logger.info(f"Simulation is not fresh, skipping start: {simulation_id}")
        status = "Simulation not ready (status!=FRESH)"

    result = {
        "simulation_id": simulation_id,
        "status": status
    }
    logger.info(f"Completed simulation orchestrator: {context.instance_id}")
    return result

# Simulation loop orchestrator
@app.orchestration_trigger(context_name="context")
def simulation_loop_orchestrator(context):
    logger.info(f"Starting simulation loop orchestrator for simulation id: {context.instance_id}")
    simulation_id = SIMULATION_ID

    # Get the input, which is a datetime object in ISO format from when the trigger fired
    triggerdatetime = context.get_input()
    
    # Handle the time field separately before creating CloudEvent
    # event_time = None
    # if "time" in event_dict and event_dict["time"]:
    #     # Extract time as string only, we'll convert after creating CloudEvent
    #     time_str = event_dict["time"]
    #     # Store original time string to convert later
    #     event_time = time_str
    
    # # Create CloudEvent without converting time to datetime yet
    # cloud_event = CloudEvent.from_dict(event_dict)
    
    # # Now handle time conversion properly, outside the CloudEvent object
    # if event_time:
    #     # Strip 'Z' if present and convert to datetime
    #     time_str = event_time.rstrip('Z')
    #     event_datetime = datetime.datetime.fromisoformat(time_str)
    # else:
    #     event_datetime = datetime.datetime.now()

    # # Get the schematic graph, required to transform the event data
    schematic_graph = yield context.call_entity(df.EntityId("schematic_graph", "thisSchematicGraph"), "get")
    if not schematic_graph:
        raise Exception("The schematic graph is not set. Check the simulation information was successfully retrieved.")
    
    # # Create event data dictionary, ensuring time is handled as string
    # event_data = {
    #     "id": cloud_event.id,
    #     "source": cloud_event.source,
    #     "type": cloud_event.type,
    #     "time": event_time,  # Use original time string
    #     "data": cloud_event.data
    # }

    # Get the topic values from the entity
    topics = yield context.call_entity(df.EntityId("topics", "topics"), "get")
    if not topics:
        raise Exception("No topics found. Ensure the simulation loop webhook has received events.")
    logger.info(f"Processing topics: {topics}")
    
    # Process each topic's values
    for topic in topics:
        topic_values = yield context.call_entity(df.EntityId("topic_values", topic), "get")

        # Transform the event data using the schematic graph
        transformed_data = yield context.call_activity("transform_event_data", {
            "cloud_event": topic_values,
            "schematic_graph": schematic_graph
        })
        logger.info(f"Transformed event data: {transformed_data}")

    # Get the last event time and calculate time difference
    # last_event_time_raw = yield context.call_entity(df.EntityId("last_event_time", "thisLastEventTime"), "get")
    # if last_event_time_raw:
    #     last_event_time = datetime.datetime.fromisoformat(last_event_time_raw)
    #     # Use our separately converted datetime object
    #     time_diff = event_datetime - last_event_time
    #     transformed_data["time"] = time_diff.total_seconds() / 60
    #     logger.info(f"Time difference between events: {transformed_data['time']} minutes")
    # else:
    #     transformed_data["time"] = 0
    #     logger.info("No previous event time found, setting time difference to 0")

    # # Update last_event_time with current event time as ISO string
    # yield context.call_entity(df.EntityId("last_event_time", "thisLastEventTime"), "set", event_datetime.isoformat())

    # Input the transformed data to the simulation
    input_data = {
        'simulationId': simulation_id,
        'inputData': transformed_data
    }
    yield context.call_activity("input_data", input_data)

    # Resume the simulation
    resumed = yield context.call_activity("resume_simulation", simulation_id)
    if not resumed:
        raise Exception(f"Failed to resume simulation: {simulation_id}")

    # Loop, checking for status IDLE
    status = yield context.call_activity("get_simulation_status", simulation_id)
    while status != STATUS_IDLE:
        # Add a timer to avoid tight polling
        yield context.create_timer(context.current_utc_datetime + datetime.timedelta(seconds=1))
        status = yield context.call_activity("get_simulation_status", simulation_id)

    # Get the simulation results
    results = yield context.call_activity("get_simulation_results", simulation_id)
    
    # Write the results to Azure Event Hub before returning
    yield context.call_activity("send_to_event_hub", results)
    
    # Return the results
    return results


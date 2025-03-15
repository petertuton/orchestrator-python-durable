# Import app from function_app instead of creating a new instance
from function_app import app, logger

################################
# Entity Definitions
################################

@app.entity_trigger(context_name="context")
def schematic_graph(context):
    """Entity for storing schematic graph"""
    logger.debug(f"Called schematic_graph entity: {context.operation_name}")
    operation = context.operation_name
    if operation == "get":
        logger.debug("Getting schematic_graph")
        context.set_result(context.get_state(lambda: None))
    elif operation == "set":
        logger.debug("Setting schematic_graph")
        context.set_state(context.get_input())
    elif operation == "delete":
        logger.debug("Deleting schematic_graph")
        context.set_state(None)

@app.entity_trigger(context_name="context")
def last_event_time(context):
    """Entity for storing last event time"""
    logger.debug(f"Called last_event_time entity: {context.operation_name}")
    operation = context.operation_name
    if operation == "get":
        logger.debug("Getting last_event_time")
        context.set_result(context.get_state(lambda: None))
    elif operation == "set":
        logger.debug("Setting last_event_time")
        context.set_state(context.get_input())
    elif operation == "delete":
        logger.debug("Deleting last_event_time")
        context.set_state(None)

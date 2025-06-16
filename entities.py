import base64
import json
import logging
from typing import Any, Callable, Dict, Optional, Union, TypeVar, cast
# Import app from function_app instead of creating a new instance
from function_app import app, logger

################################
# Generic Entity Function
################################

T = TypeVar('T')

def generic_entity_operation(
    context: Any, 
    operation: str, 
    entity_name: str, 
    default_value: Optional[T] = None,
    custom_set_handler: Optional[Callable[[Any, Any], Any]] = None
) -> None:
    """
    Generic entity operation handler to reduce code duplication.
    
    Args:
        context: The entity context
        operation: The operation name (get, set, delete)
        entity_name: The name of the entity for logging
        default_value: Default value to use if state is None
        custom_set_handler: Optional custom handler for 'set' operations
    """
    log_prefix = f"{entity_name}:{operation}"
    
    try:
        if operation == "get":
            logger.debug(f"{log_prefix} Getting state")
            context.set_result(context.get_state(lambda: default_value))
            
        elif operation == "set":
            logger.debug(f"{log_prefix} Setting state")
            if custom_set_handler:
                # Use custom handler if provided
                custom_set_handler(context, context.get_input())
            else:
                # Default set behavior
                context.set_state(context.get_input())
                
        elif operation == "delete":
            logger.debug(f"{log_prefix} Deleting state")
            context.set_state(None)
            
        else:
            logger.warning(f"{log_prefix} Unknown operation: {operation}")
            
    except Exception as e:
        logger.error(f"{log_prefix} Error: {str(e)}", exc_info=True)
        # Re-raise to ensure the orchestrator knows about the failure
        raise

################################
# Entity Definitions
################################

@app.entity_trigger(context_name="context")
def schematic_graph(context: Any) -> None:
    """
    Entity for storing schematic graph
    
    Operations:
        - get: Retrieve the current schematic graph
        - set: Update the schematic graph
        - delete: Remove the schematic graph
    """
    generic_entity_operation(
        context=context,
        operation=context.operation_name,
        entity_name="schematic_graph"
    )

@app.entity_trigger(context_name="context")
def first_event_time(context: Any) -> None:
    """
    Entity for storing first event time
    
    Operations:
        - get: Retrieve the first event time
        - set: Update the first event time
        - delete: Remove the first event time
    """
    generic_entity_operation(
        context=context,
        operation=context.operation_name,
        entity_name="first_event_time"
    )

def handle_input_data_set(context: Any, input_data_raw: Dict[str, Any]) -> None:
    """
    Custom handler for input_data entity's set operation
    
    Args:
        context: The entity context
        input_data_raw: The raw input data
    """
    # Get existing data or initialize with defaults
    current_data = context.get_state(lambda: {
        "time": 0.0,
        "sensor1": "123",
        "sensor2": "456"
    })
    
    # Process base64 encoded data if available
    data_base64 = input_data_raw.get("data_base64")
    data_json = None
    
    if data_base64:
        try:
            data_bytes = base64.b64decode(data_base64)
            data_json = json.loads(data_bytes)
            logger.debug(f"Decoded input data: {data_json}")
        except Exception as e:
            logger.error(f"Error decoding base64 data: {str(e)}", exc_info=True)
            # Continue with existing data
    
    # Update entity with new data if a subject is provided
    subject = input_data_raw.get("subject")
    
    if subject and data_json:
        current_data[subject] = data_json
        logger.debug(f"Updated input data for subject: {subject}")
    
    # Set the updated state
    context.set_state(current_data)

@app.entity_trigger(context_name="context")
def input_data(context: Any) -> None:
    """
    Entity for storing input data
    
    Operations:
        - get: Retrieve the current input data
        - set: Update the input data with new values
        - delete: Remove all input data
    """
    generic_entity_operation(
        context=context,
        operation=context.operation_name,
        entity_name="input_data",
        default_value={
            "time": 0.0,
            "sensor1": "123",
            "sensor2": "456"
        },
        custom_set_handler=handle_input_data_set
    )


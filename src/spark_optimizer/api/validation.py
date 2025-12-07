"""Validation utilities for Flask API endpoints."""

from functools import wraps
from flask import request, jsonify
from pydantic import BaseModel, ValidationError
from typing import Type, Callable, Any


def validate_request(schema: Type[BaseModel]) -> Callable:
    """Decorator to validate Flask request body against a Pydantic schema.

    Args:
        schema: Pydantic model class to validate against

    Returns:
        Decorated function

    Usage:
        @app.route("/endpoint", methods=["POST"])
        @validate_request(MyRequestSchema)
        def my_endpoint(validated_data):
            # validated_data is the parsed Pydantic model instance
            return jsonify({"result": "success"})
    """

    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            try:
                # Get request data
                data = request.get_json()

                if data is None:
                    return jsonify({"error": "Request body must be valid JSON"}), 400

                # Validate against schema
                validated = schema(**data)

                # Pass validated data to the endpoint
                return f(*args, validated_data=validated, **kwargs)

            except ValidationError as e:
                # Return validation errors
                errors = []
                for error in e.errors():
                    field = " -> ".join(str(x) for x in error["loc"])
                    errors.append({"field": field, "message": error["msg"]})

                return (
                    jsonify(
                        {
                            "error": "Validation failed",
                            "details": errors,
                        }
                    ),
                    400,
                )

            except Exception as e:
                return jsonify({"error": str(e)}), 500

        return wrapped

    return decorator


def validate_response(schema: Type[BaseModel]) -> Callable:
    """Decorator to validate Flask response against a Pydantic schema.

    Args:
        schema: Pydantic model class to validate against

    Returns:
        Decorated function

    Usage:
        @app.route("/endpoint", methods=["GET"])
        @validate_response(MyResponseSchema)
        def my_endpoint():
            return {"result": "success"}
    """

    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            try:
                # Call the original function
                result = f(*args, **kwargs)

                # If result is a tuple (response, status_code), validate the response part
                if isinstance(result, tuple):
                    response_data, status_code = result
                    validated = schema(**response_data)
                    return jsonify(validated.dict()), status_code
                else:
                    # Validate the response
                    validated = schema(**result)
                    return jsonify(validated.dict())

            except ValidationError as e:
                # Log validation error but don't expose to client
                import logging

                logger = logging.getLogger(__name__)
                logger.error(f"Response validation failed: {e}")

                return (
                    jsonify(
                        {
                            "error": "Internal server error",
                            "message": "Response validation failed",
                        }
                    ),
                    500,
                )

            except Exception as e:
                # Re-raise other exceptions
                raise e

        return wrapped

    return decorator


def parse_request(schema: Type[BaseModel]) -> Any:
    """Parse and validate current Flask request body.

    Args:
        schema: Pydantic model class to validate against

    Returns:
        Validated Pydantic model instance

    Raises:
        ValidationError: If validation fails

    Usage:
        @app.route("/endpoint", methods=["POST"])
        def my_endpoint():
            data = parse_request(MyRequestSchema)
            return jsonify({"result": data.some_field})
    """
    data = request.get_json()

    if data is None:
        raise ValueError("Request body must be valid JSON")

    return schema(**data)

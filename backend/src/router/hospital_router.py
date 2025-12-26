import asyncio
from uuid import UUID, uuid4

from fastapi import APIRouter
from fastapi import HTTPException, status
from fastapi import UploadFile, File, WebSocket, WebSocketDisconnect, BackgroundTasks

from config.logger import log
from config import constants as c
from . import schemas as schema
from src.lib.redis_service import redis_service as rs
from src.router.services.hospital_service import (
    validate_csv_file,
    parse_csv_file,
    validate_csv_data,
    remove_duplicates,
    store_csv_in_redis,
    initialize_batch_status,
    process_hospital_batch,
    get_batch_status,
    retry_failed_hospitals,
)
from src.lib.aio_http_service import get_batch_hospitals


hospital_router = APIRouter(
    prefix="/v1/hospital",
    tags=["hospital"],
)


@hospital_router.post(
    "/bulk",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schema.BulkCreateResponse,
)
async def bulk_create_hospitals(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Upload CSV file for bulk hospital creation.
    Returns 202 Accepted and processes hospitals in background.
    """
    # Validate file
    if not file.filename or not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a CSV file"
        )

    # Read file content
    try:
        file_content = await file.read()
    except Exception as e:
        log.error(f"Error reading file: {repr(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error reading file: {str(e)}"
        )

    # Validate CSV format
    if not validate_csv_file(file_content):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid CSV file format"
        )

    # Parse CSV
    try:
        rows, headers = parse_csv_file(file_content)
    except Exception as e:
        log.error(f"Error parsing CSV: {repr(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error parsing CSV: {str(e)}"
        )

    # Validate CSV data
    is_valid, errors = validate_csv_data(rows, headers)
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"validation_errors": errors}
        )

    # Remove duplicates
    unique_rows, duplicate_count, duplicate_rows = remove_duplicates(rows)

    # Generate batch ID
    batch_id = uuid4()

    # Store CSV and rows in Redis
    await store_csv_in_redis(batch_id, file_content, unique_rows)

    # Initialize status
    await initialize_batch_status(batch_id, len(unique_rows), duplicate_count)

    # Start background task
    background_tasks.add_task(process_hospital_batch, batch_id, unique_rows)

    # Format duplicate hospitals for response
    duplicate_hospitals = []
    for dup in duplicate_rows:
        duplicate_hospitals.append({
            "row": dup["row_number"],
            "name": dup["name"],
            "address": dup["address"],
            "phone": dup.get("phone")
        })

    return schema.BulkCreateResponse(
        batch_id=batch_id,
        status="processing",
        duplicates_removed=duplicate_count,
        total_hospitals=len(unique_rows),
        message="Batch processing started",
        duplicate_hospitals=duplicate_hospitals
    )


@hospital_router.websocket("/status")
async def websocket_status(websocket: WebSocket):
    """
    WebSocket endpoint for real-time status updates.
    Query parameter: batch_id (UUID string)
    """
    await websocket.accept()

    try:
        # Get batch_id from query parameters
        query_params = dict(websocket.query_params)
        batch_id = query_params.get("batch_id")

        if not batch_id:
            await websocket.send_json({
                "error": "batch_id query parameter is required"
            })
            await websocket.close()
            return

        # Validate batch_id
        try:
            batch_uuid = UUID(batch_id)
        except ValueError:
            await websocket.send_json({
                "error": "Invalid batch_id format"
            })
            await websocket.close()
            return

        # Check if batch exists
        status_data = await get_batch_status(batch_uuid)
        if not status_data:
            await websocket.send_json({
                "error": "Batch not found"
            })
            await websocket.close()
            return

        while True:
            await asyncio.sleep(1)  # Poll every second

            current_status = await get_batch_status(batch_uuid)
            if not current_status:
                await websocket.send_json({
                    "error": "Batch status no longer available"
                })
                break

            # Send update if processing
            if current_status.get("status") == "processing":
                await websocket.send_json(current_status)

            # Close connection if completed or failed
            if current_status.get("status") in ["completed", "failed"]:
                await websocket.send_json(current_status)
                break

    except WebSocketDisconnect:
        log.info(f"WebSocket disconnected for batch: {batch_id}")
    except Exception as e:
        log.error(f"Error in WebSocket: {repr(e)}")
        try:
            await websocket.send_json({
                "error": f"Internal error: {str(e)}"
            })
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


@hospital_router.post(
    "/retry",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=schema.RetryResponse,
)
async def retry_batch(
    request: schema.RetryRequest,
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Retry failed hospitals for a batch.
    Returns 202 Accepted and processes retry in background.
    """
    batch_id = request.batch_id

    # Check if retry is already in progress
    retry_in_progress = await rs.check_retry_lock(batch_id)
    if retry_in_progress:
        return schema.RetryResponse(
            batch_id=batch_id,
            status="retry_in_progress",
            message="Retry already in progress for this batch"
        )

    # Set retry lock
    lock_set = await rs.set_retry_lock(batch_id)
    if not lock_set:
        return schema.RetryResponse(
            batch_id=batch_id,
            status="retry_in_progress",
            message="Retry already in progress for this batch"
        )

    try:
        # Check if CSV exists
        csv_content = await rs.get_csv(batch_id)
        if not csv_content:
            await rs.delete_retry_lock(batch_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="CSV file not found or expired. Cannot retry."
            )

        # Get existing hospitals
        try:
            existing_hospitals = await get_batch_hospitals(batch_id)
        except Exception as e:
            log.error(f"Error getting existing hospitals: {repr(e)}")
            existing_hospitals = []

        # Get rows from Redis
        rows_data = await rs.get_rows(batch_id)
        if not rows_data:
            await rs.delete_retry_lock(batch_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Rows data not found. Cannot retry."
            )

        # Get current status
        status_data = await get_batch_status(batch_id)
        if not status_data:
            await rs.delete_retry_lock(batch_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Batch status not found. Cannot retry."
            )

        # Calculate failed rows
        existing_keys = set()
        for hospital in existing_hospitals:
            key = (hospital["name"].lower().strip(), hospital["address"].lower().strip())
            existing_keys.add(key)

        failed_rows = []
        processed_row_numbers = {h["row"] for h in status_data.get("hospitals", [])}

        # Find failed rows from status
        for hospital in status_data.get("hospitals", []):
            if hospital["status"] == "failed":
                for row in rows_data:
                    if row["row_number"] == hospital["row"]:
                        failed_rows.append(row)
                        break

        # Find rows not processed
        for row in rows_data:
            if row["row_number"] not in processed_row_numbers:
                key = (row["name"].lower().strip(), row["address"].lower().strip())
                if key not in existing_keys:
                    failed_rows.append(row)

        # Check if retry needed
        if not failed_rows:
            await rs.delete_retry_lock(batch_id)
            return schema.RetryResponse(
                batch_id=batch_id,
                status="completed",
                rows_to_retry=0,
                message="All hospitals already successful, no retry needed"
            )

        # Start background retry task
        background_tasks.add_task(retry_failed_hospitals, batch_id)

        return schema.RetryResponse(
            batch_id=batch_id,
            status="retrying",
            rows_to_retry=len(failed_rows),
            message="Retry processing started"
        )

    except HTTPException:
        raise
    except Exception as e:
        # Ensure retry lock is deleted on error
        await rs.delete_retry_lock(batch_id)
        log.error(f"Error in retry endpoint: {repr(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error: {str(e)}"
        )

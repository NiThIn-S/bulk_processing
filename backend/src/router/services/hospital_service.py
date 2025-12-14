import io
import csv
import asyncio
from uuid import UUID
from typing import List, Dict, Tuple, Optional

from fastapi import HTTPException, status

from config.logger import log
from config import constants as c
from src.lib.redis_service import redis_service as rs
from src.lib.aio_http_service import create_hospital, activate_batch, get_batch_hospitals


def validate_csv_file(file_content: bytes) -> bool:
    """Validate that file is actual CSV format."""
    try:
        # Try to decode as UTF-8
        content = file_content.decode('utf-8')
        # Try to parse as CSV
        csv.Sniffer().sniff(content)
        return True
    except (UnicodeDecodeError, csv.Error):
        return False


def parse_csv_file(file_content: bytes) -> Tuple[List[Dict], List[str]]:
    """
    Parse CSV file and return rows and headers.
    """
    content = file_content.decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    rows = []

    for row_num, row in enumerate(reader, start=1):
        name = row.get('name', '').strip()
        address = row.get('address', '').strip()
        phone = row.get('phone', '').strip()
        if not phone:
            phone = None
        rows.append({
            'row_number': row_num,
            'name': name,
            'address': address,
            'phone': phone
        })

    return rows, list(headers)


def validate_csv_data(rows: List[Dict], headers: List[str]) -> Tuple[bool, List[str]]:
    """
    Validate CSV data.
    """
    errors = []

    # Validate headers
    required_headers = ['name', 'address']
    for header in required_headers:
        if header not in headers:
            errors.append(f"Missing required header: {header}")

    # Validate row count
    if len(rows) > c.MAX_HOSPITALS:
        log.error(f"Maximum {c.MAX_HOSPITALS} hospitals allowed, found {len(rows)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Maximum {c.MAX_HOSPITALS} hospitals allowed, found {len(rows)}"
        )

    # Validate each row
    for row in rows:
        if not row['name'] or len(row['name']) < 1:
            errors.append(f"Row {row['row_number']}: name is required and must be non-empty")
        if not row['address'] or len(row['address']) < 1:
            errors.append(f"Row {row['row_number']}: address is required and must be non-empty")

    return len(errors) == 0, errors


def _create_hospital_key(name: str, address: str):
    return (name.lower().strip(), address.lower().strip())


def remove_duplicates(rows: List[Dict]) -> Tuple[List[Dict], int, List[Dict]]:
    """
    Remove duplicate rows based on (name, address) combination.
    Keep first occurrence.
    """
    seen = set()
    unique_rows = []
    duplicate_rows = []

    for row in rows:
        # Create key from name and address (case-insensitive)
        key = _create_hospital_key(row['name'], row['address'])

        if key in seen:
            duplicate_rows.append(row)
        else:
            seen.add(key)
            unique_rows.append(row)

    return unique_rows, len(duplicate_rows), duplicate_rows


async def store_csv_in_redis(batch_id: UUID, csv_content: bytes, rows: List[Dict]):
    """Store CSV and rows in Redis."""
    await rs.store_csv(batch_id, csv_content, ttl=c.CSV_TTL)
    await rs.store_rows(batch_id, rows, ttl=c.CSV_TTL)


async def initialize_batch_status(batch_id: UUID, total: int, duplicates: int):
    """Initialize status in Redis."""
    status = {
        "batch_id": str(batch_id),
        "status": "processing",
        "total_hospitals": total,
        "processed_hospitals": 0,
        "successful_hospitals": 0,
        "failed_hospitals": 0,
        "batch_activated": False,
        "hospitals": []
    }
    await rs.store_status(batch_id, status, ttl=c.STATUS_TTL)
    log.info(f"Initialized status for batch: {batch_id}, total: {total}, duplicates: {duplicates}")


async def process_hospital_row(row_data: Dict, batch_id: UUID, row_number: int) -> Dict:
    """
    Process a single hospital row.
    """
    result = {
        "row": row_number,
        "hospital_id": None,
        "name": row_data["name"],
        "address": row_data["address"],
        "phone": row_data.get("phone"),
        "status": "failed",
        "error": None
    }

    try:
        hospital_data = {
            "name": row_data["name"],
            "address": row_data["address"],
            "phone": row_data.get("phone")
        }

        hospital_response = await create_hospital(hospital_data, batch_id)
        result["hospital_id"] = hospital_response["id"]
        result["status"] = "success"
        log.info(f"Successfully created hospital {result['hospital_id']} for batch {batch_id}, row {row_number}")

    except Exception as e:
        error_msg = str(e)
        result["error"] = error_msg
        log.error(f"Failed to create hospital for batch {batch_id}, row {row_number}: {error_msg}")

    return result


async def update_redis_status(batch_id: UUID, hospital_result: Dict):
    """Update Redis status with a hospital result."""
    status = await rs.get_status(batch_id)
    if not status:
        log.warning(f"Status not found for batch: {batch_id}")
        return

    # Add hospital result
    status["hospitals"].append(hospital_result)

    # Update counters
    status["processed_hospitals"] = len(status["hospitals"])
    if hospital_result["status"] == "success":
        status["successful_hospitals"] = sum(1 for h in status["hospitals"] if h["status"] == "success")
    else:
        status["failed_hospitals"] = sum(1 for h in status["hospitals"] if h["status"] == "failed")

    await rs.store_status(batch_id, status, ttl=c.STATUS_TTL)


async def _mark_batch_completed(batch_id: UUID, status: Dict, batch_activated: bool = False):
    """Helper to mark batch as completed and store in Redis."""
    status["status"] = "completed"
    status["batch_activated"] = batch_activated
    await rs.store_status(batch_id, status, ttl=c.STATUS_TTL)


async def activate_batch_if_complete(batch_id: UUID) -> bool:
    """
    Check if all hospitals are successful and activate batch if so.
    """

    status = await rs.get_status(batch_id)
    if not status:
        log.warning(f"Status not found for batch: {batch_id}")
        return False

    total = status["total_hospitals"]
    successful = status["successful_hospitals"]

    if successful == total and total > 0:
        try:
            log.info(f"All {total} hospitals successful. Activating batch: {batch_id}")
            await activate_batch(batch_id)
            await _mark_batch_completed(batch_id, status, batch_activated=True)
            log.info(f"Successfully activated batch: {batch_id}")
            return True
        except Exception as e:
            log.error(f"Failed to activate batch {batch_id}: {repr(e)}")
            await _mark_batch_completed(batch_id, status, batch_activated=False)
            return False
    else:
        # Update status to completed even if not all successful
        await _mark_batch_completed(batch_id, status, batch_activated=False)
        return False


async def process_rows_in_chunks(
    batch_id: UUID, rows: List[Dict], context: str = "processing"
) -> List[Dict]:
    """
    Process hospital rows in chunks with MAX_CONCURRENT_WORKERS parallelism.
    Updates status in Redis after each chunk completes.
    """
    all_results = []

    # Process rows in chunks of MAX_CONCURRENT_WORKERS
    for i in range(0, len(rows), c.MAX_CONCURRENT_WORKERS):
        chunk = rows[i : i+c.MAX_CONCURRENT_WORKERS]
        chunk_num = i // c.MAX_CONCURRENT_WORKERS + 1
        log.info(f"Processing {context} chunk {chunk_num} with {len(chunk)} hospitals")

        # Create tasks for this chunk
        tasks = [
            process_hospital_row(row, batch_id, row["row_number"])
            for row in chunk
        ]

        # Wait for this chunk to complete before starting the next
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
        all_results.extend(chunk_results)

        # Update status immediately after each chunk completes
        for result in chunk_results:
            if isinstance(result, Exception):
                log.error(f"Exception in {context} processing: {repr(result)}")
                # Create a failed result
                failed_result = {
                    "row": 0,
                    "hospital_id": None,
                    "name": "Unknown",
                    "address": None,
                    "phone": None,
                    "status": "failed",
                    "error": str(result)
                }
                await update_redis_status(batch_id, failed_result)
            else:
                await update_redis_status(batch_id, result)

    return all_results


async def process_hospital_batch(batch_id: UUID, rows: List[Dict]):
    """
    Background task to process hospitals in parallel with specified number of workers.
    """
    log.info(f"Starting batch processing for {len(rows)} hospitals, batch_id: {batch_id}")

    await process_rows_in_chunks(batch_id, rows, context="processing")

    await activate_batch_if_complete(batch_id)

    log.info(f"Completed batch processing for batch_id: {batch_id}")


async def get_batch_status(batch_id: UUID) -> Optional[Dict]:
    """Get status from Redis."""
    return await rs.get_status(batch_id)


async def retry_failed_hospitals(batch_id: UUID):
    """
    Retry failed hospitals for a batch.
    Fetches CSV from Redis, gets existing hospitals, calculates delta, and retries only failed rows.
    """
    log.info(f"Starting retry for batch: {batch_id}")

    try:
        # Get CSV and rows from Redis
        csv_content = await rs.get_csv(batch_id)
        if not csv_content:
            log.error(f"CSV not found for batch: {batch_id}")
            return

        rows_data = await rs.get_rows(batch_id)
        if not rows_data:
            log.error(f"Rows data not found for batch: {batch_id}")
            return

        # Get existing hospitals from external API
        try:
            existing_hospitals = await get_batch_hospitals(batch_id)
        except Exception as e:
            log.error(f"Failed to get existing hospitals for batch {batch_id}: {repr(e)}")
            existing_hospitals = []

        # Create set of existing hospital keys (name, address)
        existing_keys = set()
        for hospital in existing_hospitals:
            key = _create_hospital_key(hospital["name"], hospital["address"])
            existing_keys.add(key)

        # Get current status to find failed rows
        status = await rs.get_status(batch_id)
        failed_rows = []
        if status:
            for hospital in status["hospitals"]:
                if hospital["status"] == "failed":
                    # Find the row in rows_data
                    for row in rows_data:
                        if row["row_number"] == hospital["row"]:
                            failed_rows.append(row)
                            break

        # Also check for rows that weren't processed (not in existing hospitals and not in status)
        processed_row_numbers = {h["row"] for h in status.get("hospitals", [])}
        for row in rows_data:
            if row["row_number"] not in processed_row_numbers:
                # Check if it exists in external API
                key = _create_hospital_key(row["name"], row["address"])
                if key not in existing_keys:
                    failed_rows.append(row)

        if not failed_rows:
            log.info(f"No failed rows to retry for batch: {batch_id}")
            return

        log.info(f"Retrying {len(failed_rows)} failed rows for batch: {batch_id}")

        # Process failed rows in chunks
        await process_rows_in_chunks(batch_id, failed_rows, context="retry")

        # Check if all successful and activate batch if so
        await activate_batch_if_complete(batch_id)

        log.info(f"Completed retry for batch: {batch_id}")

    except Exception as e:
        log.error(f"Error in retry_failed_hospitals for batch {batch_id}: {repr(e)}")
        raise
    finally:
        # Always delete retry lock
        await rs.delete_retry_lock(batch_id)

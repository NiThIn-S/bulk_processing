from typing import Optional, List
from pydantic import BaseModel, Field
from uuid import UUID


class BulkCreateResponse(BaseModel):
    batch_id: UUID
    status: str
    duplicates_removed: int
    total_hospitals: int
    message: str
    duplicate_hospitals: Optional[List[dict]] = None

# class HospitalResult(BaseModel):
#     row: int
#     hospital_id: Optional[int] = None
#     name: str
#     address: Optional[str] = None
#     phone: Optional[str] = None
#     status: str
#     error: Optional[str] = None


# class StatusResponse(BaseModel):
#     batch_id: UUID
#     status: str
#     total_hospitals: int
#     processed_hospitals: int
#     successful_hospitals: int
#     failed_hospitals: int
#     batch_activated: bool
#     hospitals: List[HospitalResult]


class RetryRequest(BaseModel):
    batch_id: UUID


class RetryResponse(BaseModel):
    batch_id: UUID
    status: str
    rows_to_retry: Optional[int] = None
    message: str

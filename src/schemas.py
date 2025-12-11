# src/schemas.py
from typing import Optional, List, Literal
from datetime import datetime
from pydantic import ConfigDict
from sqlmodel import SQLModel


class TaxiTripBase(SQLModel):
    vendor_id: Optional[int] = None
    tpep_pickup_datetime: Optional[datetime] = None
    tpep_dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[float] = None
    trip_distance: Optional[float] = None
    ratecode_id: Optional[float] = None
    store_and_fwd_flag: Optional[str] = None
    pu_location_id: Optional[int] = None
    do_location_id: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None
    cbd_congestion_fee: Optional[float] = None


class TaxiTripCreate(TaxiTripBase):
    vendor_id: int
    tpep_pickup_datetime: datetime
    tpep_dropoff_datetime: datetime
    trip_distance: float
    pu_location_id: int
    do_location_id: int
    payment_type: int
    total_amount: float


class TaxiTripUpdate(TaxiTripBase):
    pass


class TaxiTrip(TaxiTripBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


class TaxiTripList(SQLModel):
    total: int
    trips: List[TaxiTrip]


class Statistics(SQLModel):
    total: int
    min_amount: float
    max_amount: float
    avg_amount: float


class PipelineResponse(SQLModel):
    status: Literal["started", "done", "error"]
    downloaded: int
    imported_files: int
    imported_rows: int

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session
from sqlalchemy import text
from src.database import get_db
from src.schemas import (
    TaxiTripList,
    TaxiTrip,
    TaxiTripCreate,
    TaxiTripUpdate,
    Statistics,
    PipelineResponse,
)
from src.services import TaxiTripService, PipelineService

router = APIRouter()


# GET    /                      → Info API
# GET    /health                → Health check
# GET    /api/v1/trips          → Liste des trajets
# GET    /api/v1/trips/{id}     → Un trajet
# POST   /api/v1/trips          → Créer un trajet
# PUT    /api/v1/trips/{id}     → Modifier un trajet
# DELETE /api/v1/trips/{id}     → Supprimer un trajet
# GET    /api/v1/statistics     → Statistiques
# POST   /api/v1/pipeline/run   → Exécuter la pipeline


@router.get("/", tags=["Info"])
def root():
    return {
        "api_name": app.title,
        "version": app.version,
        "description": app.description,
    }


@router.get("/health", tags=["Health"])
def health_check(db: Session = Depends(get_db)):
    try:
        db.exec(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "database": str(e)}


@router.get("/api/v1/trips", response_model=TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    trips, total = TaxiTripService.get_trips(db, skip, limit)
    return TaxiTripList(total=total, trips=trips)


@router.get("/api/v1/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    return trip


@router.post("/api/v1/trips", response_model=TaxiTrip, tags=["Trips"])
def create_trip(trip: TaxiTripCreate, db: Session = Depends(get_db)):
    created_trip = TaxiTripService.create_trip(db, trip)
    return created_trip


@router.put("/api/v1/trips/{trip_id}", response_model=TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, trip: TaxiTripUpdate, db: Session = Depends(get_db)):
    updated_trip = TaxiTripService.update_trip(db, trip_id, trip)
    if not updated_trip:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    return updated_trip


@router.delete(
    "/api/v1/trips/{trip_id}", status_code=status.HTTP_200_OK, tags=["Trips"]
)
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    success = TaxiTripService.delete_trip(db, trip_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trip not found"
        )
    else:
        return {"message": f"Trip {trip_id} deleted successfully."}


@router.get("/api/v1/statistics", response_model=Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    stats = TaxiTripService.get_statistics(db)
    return stats


@router.post("/api/v1/pipeline/run", response_model=PipelineResponse, tags=["Pipeline"])
def run_pipeline():
    result = PipelineService.run_pipeline()
    return result

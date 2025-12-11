from typing import Optional, Tuple, List
from datetime import datetime
from sqlmodel import Session, select, func
from src.models import YellowTaxiTrip
from src.schemas import TaxiTripCreate, TaxiTripUpdate, Statistics, PipelineResponse
from src.download_data import main as download_data
from src.import_to_postgres import main as import_data_to_postgres
from src.database import init_db





class TaxiTripService:
    @staticmethod
    def get_trip(db: Session, trip_id: int):
        trip = db.get(YellowTaxiTrip, trip_id)
        return trip

    # Récupérer un trajet par ID
    # Retourner None si non trouvé

    @staticmethod
    def get_trips(db: Session, skip: int, limit: int):
        query = select(YellowTaxiTrip).offset(skip).limit(limit)
        trips = db.exec(query).all()
        total = db.exec(select(func.count()).select_from(YellowTaxiTrip)).one()
        return trips, total

    # Récupérer une liste de trajets avec pagination
    # Retourner (trips, total)

    @staticmethod
    def create_trip(db: Session, trip: TaxiTripCreate):
        row = YellowTaxiTrip(**trip.model_dump())
        db.add(row)
        db.commit()
        db.refresh(row)
        return row


    # Créer un nouveau trajet
    # Sauvegarder en base
    # Retourner le trajet créé

    @staticmethod
    def update_trip(db: Session, trip_id: int, trip: TaxiTripUpdate):
        statement = select(YellowTaxiTrip).where(YellowTaxiTrip.id == trip_id)
        results = db.exec(statement)
        existing_trip = results.first()
        if not existing_trip:
            return None
        trip_data = trip.model_dump(exclude_unset=True)
        for key, value in trip_data.items():
            setattr(existing_trip, key, value)
        db.add(existing_trip)
        db.commit()
        db.refresh(existing_trip)
        return existing_trip

    # Mettre à jour un trajet existant
    # Retourner None si non trouvé
    @staticmethod
    def delete_trip(db: Session, trip_id: int):
        statement = select(YellowTaxiTrip).where(YellowTaxiTrip.id == trip_id)
        results = db.exec(statement)
        existing_trip = results.first()
        if not existing_trip:
            return False
        db.delete(existing_trip)
        db.commit()
        return True

    # Supprimer un trajet
    # Retourner True/False
    @staticmethod
    def get_statistics(db: Session):
        statement = select(func.count(YellowTaxiTrip.vendor_id).label("total_trips"),
                           func.min(YellowTaxiTrip.total_amount).label("min_amount"),
                           func.max(YellowTaxiTrip.total_amount).label("max_amount"),
                           func.avg(YellowTaxiTrip.total_amount).label("avg_amount")
        )
        results = db.exec(statement).first()
        
        if not results:
            raise ValueError("No trips found")
        
        return Statistics(
            total=results.total_trips,
            min_amount=results.min_amount,
            max_amount=results.max_amount,
            avg_amount=results.avg_amount
        )

    # Calculer les statistiques (COUNT, MIN, MAX, AVG)
    # Retourner un objet Statistics

class PipelineService:
    @staticmethod
    def run_pipeline() -> PipelineResponse:
        # Logique pour exécuter la pipeline de traitement des données
        # Par exemple, ingérer des données depuis une source externe
        # Transformer et charger dans la base de données
        init_db()
        downloaded_files = download_data()
        imported_stats = import_data_to_postgres()
        return PipelineResponse(status = "done",
                                downloaded = downloaded_files,
                                imported_files = imported_stats["files_imported"],
                                imported_rows = imported_stats["total_rows"]
                                )
    # Retourner un objet PipelineResponse

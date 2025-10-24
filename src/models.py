from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime, timezone

class YellowTaxiTrip(SQLModel, table=True):
    __tablename__ = "yellow_taxi_trips"
    
    id: Optional[int] = Field(
        default=None,
        primary_key=True,
        index=True,
    )
    vendor_id: int = Field(description="ID du fournisseur (1=Creative, 2=VeriFone)")
    tpep_pickup_datetime: datetime = Field(
        description="Date/heure de prise en charge",
        index=True,
    )
    tpep_dropoff_datetime: datetime = Field(description="Date/heure de dépose")
    passenger_count: Optional[float] = Field(
        default=None, description="Nombre de passagers (peut être NULL)"
    )
    trip_distance: Optional[float] = Field(
        default=None,
        ge=0,
        description="Distance du trajet en miles",
    )
    ratecode_id: Optional[float] = Field(
        default=None, description="Code de tarif (1=Standard, 2=JFK, etc.)"
    )
    store_and_fwd_flag: Optional[str] = Field(
        default=None,
        max_length=1,
        description="Trajet enregistré avant envoi (Y/N)",
    )
    pu_location_id: int = Field(description="Zone de prise en charge (TLC Taxi Zone)")
    do_location_id: int = Field(description="Zone de dépose (TLC Taxi Zone)")
    payment_type: int = Field(description="Type de paiement (1=Carte, 2=Cash, etc.)")
    fare_amount: float = Field(description="Tarif de base (hors extras)")
    extra: float = Field(
        default=0.0, description="Suppléments (heures de pointe, nuit)"
    )
    mta_tax: float = Field(default=0.0, description="Taxe MTA (0.50 $)")
    tip_amount: float = Field(default=0.0, description="Pourboire (carte uniquement)")
    tolls_amount: float = Field(default=0.0, description="Péages")
    improvement_surcharge: float = Field(
        default=0.0, description="Surcharge d'amélioration (0.30 $)"
    )
    total_amount: float = Field(description="Montant total payé")
    congestion_surcharge: Optional[float] = Field(
        default=None, description="Surcharge de congestion (2.50/2.75 $)"
    )
    airport_fee: Optional[float] = Field(
        default=None, description="Frais aéroport (1.25 $)"
    )
    cbd_congestion_fee: Optional[float] = Field(
        default=None, description="Frais de congestion CBD (Central Business District)"
    )

class ImportLog(SQLModel, table=True):
    __tablename__ = "import_log"

    file_name: str = Field(description="Nom du fichier Parquet", primary_key=True)
    import_date: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Date et heure d'importation",
    )
    rows_imported: int = Field(ge=0, description="Nombre de lignes importées")

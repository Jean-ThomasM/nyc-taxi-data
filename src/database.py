import os
from typing import Generator
from sqlmodel import create_engine, Session, SQLModel


POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# TODO: En production, retirer les valeurs par défaut !
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Moteur SQLmodel
engine = create_engine(DATABASE_URL, echo=True)


def get_db() -> Generator[Session, None, None]:
    """
    Crée une session de base de données.
    Utilisée comme dépendance FastAPI.
    """
    with Session(engine) as session:
        yield session


def init_db() -> None:
    """
    Initialise toutes les tables définies dans les modèles SQLModel.
    À appeler au démarrage de l'application.
    """
    SQLModel.metadata.create_all(bind=engine)


# TEST (à supprimer après)
if __name__ == "__main__":
    print("DATABASE_URL:", DATABASE_URL)
    print("Engine créé:", engine)

    # Test de connexion
    try:
        with engine.connect() as conn:
            print("✅ Connexion à PostgreSQL réussie !")
    except Exception as e:
        print("❌ Erreur:", e)

from pathlib import Path
from datetime import datetime, timezone
import io
import pandas as pd
from sqlmodel import Session
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import engine, init_db
from models import ImportLog



def clean_df(file_path: Path) -> pd.DataFrame:
    print(f"Nettoyage des données pour {file_path.name}...")
    df = pd.read_parquet(file_path)

    column_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "ratecode_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
        "cbd_congestion_fee": "cbd_congestion_fee",
    }
    df = df.rename(columns=column_mapping)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    #ancrage du fuseau horaire en America/New_York
    df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].dt.tz_localize("America/New_York")
    df["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"].dt.tz_localize("America/New_York")

    return df


# --- Log en base (inchangé) ---
def log_import(file_name: str, rows_imported: int) -> None:
    with Session(engine) as session:
        log = ImportLog(
            file_name=file_name,
            import_date=datetime.now(timezone.utc),
            rows_imported=rows_imported,
        )
        session.add(log)
        session.commit()


# --- CORE: COPY au lieu de to_sql ---
def import_parquet_copy(file_path: Path) -> int | None:
    """
    Lit, nettoie et importe via COPY dans yellow_taxi_trips.
    Retourne le nombre de lignes importées, ou None si erreur.
    """
    try:
        df = clean_df(file_path)
        if df.empty:
            print(f"⚠️ {file_path.name}: dataframe vide, skip.")
            return 0

        # IMPORTANT: l'ordre des colonnes doit matcher la table cible
        target_cols = [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "ratecode_id",
            "store_and_fwd_flag",
            "pu_location_id",
            "do_location_id",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "cbd_congestion_fee",  # si la colonne n'existe pas dans la table, retire-la ici
        ]
        # Ne garder que les colonnes présentes et dans l'ordre
        cols = [c for c in target_cols if c in df.columns]
        df = df[cols]

        # CSV en mémoire (NULL → \N pour COPY)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)

        # COPY ... FROM STDIN
        columns_sql = ", ".join(cols)
        copy_sql = f"COPY yellow_taxi_trips ({columns_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"

        raw_conn = engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            try:
                cur.copy_expert(copy_sql, buf)
                raw_conn.commit()
            finally:
                cur.close()
        finally:
            raw_conn.close()

        print(f"✅ {file_path.name}: {len(df):,} lignes importées via COPY")
        return len(df)

    except Exception as e:
        print(f"❌ Erreur sur {file_path.name}: {e}")
        return None


def main():
    print("🔧 Initialisation de la base de données...")
    init_db()

    parquet_dir = Path.cwd() / "data" / "raw"
    files = sorted([p for p in parquet_dir.glob("*.parquet")])
    if not files:
        print("❌ Aucun .parquet trouvé dans data/raw")
        return

    print(f"📦 {len(files)} fichiers trouvés. Import en parallèle (4 workers)...")
    total_rows = 0
    ok = 0

    # ThreadPool: on lance plusieurs imports en parallèle
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(import_parquet_copy, f): f for f in files}

        for fut in as_completed(futures):
            fpath = futures[fut]
            try:
                rows = fut.result()
                if rows is not None and rows >= 0:
                    log_import(fpath.name, rows)
                    total_rows += rows
                    ok += 1
            except Exception as e:
                print(f"❌ Exception inattendue sur {fpath.name}: {e}")

    print("\n✅ Import terminé !")
    print(f"📊 Fichiers traités avec succès: {ok}/{len(files)}")
    print(f"📊 Total lignes importées: {total_rows:,}")

    return {
        "files_imported": ok,
        "total_rows": total_rows
    }

if __name__ == "__main__":
    main()

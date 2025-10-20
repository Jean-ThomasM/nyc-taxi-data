import requests
import os
from pathlib import Path
from datetime import datetime

class NYCTaxiDataDownloader:
    def __init__(self, base_url, data_dir, year):
        """Définir les constantes : BASE_URL, YEAR, DATA_DIR
        Créer le répertoire de destination si nécessaire"""
        self.base_url = base_url
        self.data_dir = data_dir
        self.year = year
        self.file_extension = ".parquet"
        self.file_name_base = "yellow_tripdata_"

    def get_file_name(self,month: int) -> str :
        file_name = f"{self.file_name_base}{self.year}-{month:02d}{self.file_extension}"
        return file_name
    
    def get_file_path(self, month: int) -> Path :
        """Construire le chemin du fichier pour un mois donné
        Format : yellow_tripdata_YYYY-MM.parquet"""
        file_path = Path(f"{self.data_dir}/{self.get_file_name(month)}")
        return file_path
    
    def file_exists(self, month: int) -> bool :
        """Vérifier si le fichier existe déjà localement
        Retourner True/False"""
        file_path = self.get_file_path(month)
        if file_path.exists():
            print(f"le fichier {file_path} existe déjà")
            return True
        else:
            return False
        
    def download_month(self, month: int) -> bool :
        """Vérifier si le fichier existe déjà (utiliser file_exists())
        Si oui, afficher un message et retourner True
        Sinon, télécharger le fichier depuis BASE_URL
        Utiliser requests.get() avec stream=True
        Afficher une barre de progression (optionnel)
        Gérer les erreurs avec try/except
        En cas d'erreur, supprimer le fichier partiel"""

        if self.file_exists(month):
            print(f"Le fichier pour le mois {month} existe déjà.")
            return True

        file_name = self.get_file_name(month)
        file_path = self.get_file_path(month)

        try:
            r = requests.get(f"{self.base_url}/{file_name}", stream=True, timeout=30)
            r.raise_for_status()

            if r.status_code == 200:
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Fichier {file_name} téléchargé avec succès")
                return True
            else:
                print("Erreur : code {r.status_code}")
        
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors du téléchargement de {file_name}: {e}")
            # Supprimer le fichier partiel s'il existe
            if file_path.exists():
                file_path.unlink()
            return False
        
        except Exception as e:
            print(f"Erreur inattendue lors du téléchargement de {file_name}: {e}")
            if file_path.exists():
                file_path.unlink()
            return False
    
    def download_all_available(self) -> list :
        """ Déterminer le mois actuel
            Boucler de janvier au mois actuel (si année 2025)
            Appeler download_month() pour chaque mois
            Retourner la liste des fichiers téléchargés
            Afficher un résumé"""
        
        if self.year != datetime.now().year:
            current_month=12
        else:
            current_month = datetime.now().month
        liste_fichiers = []

        if current_month != 1:
            month_list = list(range(1,current_month+1))
        elif current_month == 1:
            month_list = [1]
        for month in month_list:
            self.download_month(month)
            liste_fichiers.append(month)
        return liste_fichiers

        
def main():
    data_dir = Path.cwd() / "data" / "raw"
    if not data_dir.exists:
        os.mkdir(data_dir)
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    classe_nyctaxi_download = NYCTaxiDataDownloader(data_dir=data_dir,base_url=base_url, year=datetime.now().year)
    fichiers_telecharges = classe_nyctaxi_download.download_all_available()
    print(f"Les fichiers des mois {fichiers_telecharges} ont été téléchargés")


if __name__ == "__main__":
    main()
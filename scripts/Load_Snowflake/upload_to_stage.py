import os
import snowflake.connector

# --- Paramètres de connexion ---
conn = snowflake.connector.connect(
    user="NICOLAS.BOUTTIER",
    password="Nicolas070899@",
    account="nusqxoe-jk70019",  # identifiant du compte
    warehouse="COMPUTE_WH",
    database="TMDB_TEST_ETL",
    schema="BUCKET"
)

# --- Construction dynamique du chemin vers data/out ---
# __file__ = chemin du script en cours d'exécution
BASE_DIR = os.path.dirname(os.path.abspath(__file__))              # dossier scripts/Load_Snowflake
PROJECT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))           # remonte jusqu'à absolute_cinema_project
local_path = os.path.join(PROJECT_DIR, "data", "out")              # construit le chemin complet vers data/out

stage_name = "@TMDB_STAGE"

# --- Création d’un curseur ---
cs = conn.cursor()

try:
    # Vérifie que le dossier existe
    if not os.path.isdir(local_path):
        raise FileNotFoundError(f"Le dossier {local_path} est introuvable ❌")

    # Boucle sur tous les fichiers JSON du dossier
    for file_name in os.listdir(local_path):
        if file_name.endswith((".json", ".ndjson", ".tmp")):
            file_path = os.path.join(local_path, file_name)

            print(f"📤 Upload du fichier : {file_name}")

            # Commande PUT = envoie le fichier vers le stage
            put_cmd = f"PUT file://{file_path} {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cs.execute(put_cmd)

    print("✅ Tous les fichiers ont été envoyés dans le stage !")

finally:
    cs.close()
    conn.close()

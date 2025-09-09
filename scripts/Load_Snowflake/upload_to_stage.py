import os
import snowflake.connector

# --- Paramètres de connexion ---
conn = snowflake.connector.connect(
    user="NICOLAS.BOUTTIER",
    password="NICOLAS070899@",
    account="nusqxoe-jk70019",  # ex: abcd-xy12345
    warehouse="COMPUTE_WH",
    database="TMDB_TEST_ETL",
    schema="BUCKET"
)

# --- Chemin vers tes fichiers JSON ---
local_path = "projects/absolute_cinema_project/data/out"  # adapte selon ton arborescence
stage_name = "@TMDB_STAGE"

# --- Création d’un curseur ---
cs = conn.cursor()

try:
    # Boucle sur tous les fichiers JSON du dossier
    for file_name in os.listdir(local_path):
        if file_name.endswith(".json") or file_name.endswith(".ndjson") or file_name.endswith(".tmp"):
            file_path = os.path.join(local_path, file_name)

            print(f"📤 Upload du fichier : {file_name}")

            # Commande PUT = envoie le fichier vers le stage
            put_cmd = f"PUT file://{file_path} {stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cs.execute(put_cmd)

    print("✅ Tous les fichiers ont été envoyés dans le stage !")

finally:
    cs.close()
    conn.close()
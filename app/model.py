import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.impute import SimpleImputer
import joblib
import snowflake.connector
import os
from dotenv import load_dotenv

# Charger .env
load_dotenv()

# Connexion Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA_ML")
)

# Charger les données
df = pd.read_sql("SELECT * FROM GLD_FACT_MOVIE_ML_NUMERIC", conn)
conn.close()

# 🎯 Cible = la note moyenne directement (régression)
y = df["LABEL_VOTE_AVERAGE"]

# Colonnes à utiliser pour le modèle
features = ["BUDGET","NB_GENRES","NB_PROVIDERS","POPULARITY","RELEASE_YEAR","REVENUE","RUNTIME","VOTE_COUNT"]
X = df[features]

# Imputation des NaN
imputer = SimpleImputer(strategy="mean")
X_imputed = imputer.fit_transform(X)

# Split train/test
X_train, X_test, y_train, y_test = train_test_split(
    X_imputed, y, test_size=0.2, random_state=42
)

# Entraînement (régression au lieu de classification)
tree = DecisionTreeRegressor(max_depth=5, random_state=42)
tree.fit(X_train, y_train)

# Sauvegarder
joblib.dump(tree, "saved_model.pkl")
joblib.dump(imputer, "imputer.pkl")
joblib.dump(features, "features.pkl")

print("✅ Modèle de régression et fichiers régénérés avec succès !")


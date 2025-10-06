import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import snowflake.connector
import os
from dotenv import load_dotenv
import numpy as np

# ------------------------------------------------------------
# 1️⃣ Charger les variables d'environnement (.env)
# ------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------
# 2️⃣ Connexion à Snowflake
# ------------------------------------------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA_ML")
)

# ------------------------------------------------------------
# 3️⃣ Charger les données depuis Snowflake
# ------------------------------------------------------------
df = pd.read_sql("SELECT * FROM GLD_FACT_MOVIE_ML_NUMERIC", conn)
conn.close()

# ------------------------------------------------------------
# 4️⃣ Définir la cible (y) et les features (X)
# ------------------------------------------------------------
y = df["LABEL_VOTE_AVERAGE"]
features = ["BUDGET","NB_GENRES","NB_PROVIDERS","POPULARITY","RELEASE_YEAR","REVENUE","RUNTIME","VOTE_COUNT"]
X = df[features]

# ------------------------------------------------------------
# 5️⃣ Imputer les valeurs manquantes (par la moyenne)
# ------------------------------------------------------------
imputer = SimpleImputer(strategy="mean")
X_imputed = imputer.fit_transform(X)

# ------------------------------------------------------------
# 6️⃣ Séparer les données en train/test
# ------------------------------------------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X_imputed, y, test_size=0.2, random_state=42
)

# ------------------------------------------------------------
# 7️⃣ Entraîner un modèle de régression (arbre de décision)
# ------------------------------------------------------------
tree = DecisionTreeRegressor(max_depth=5, random_state=42)
tree.fit(X_train, y_train)

# ------------------------------------------------------------
# 8️⃣ Évaluation du modèle sur le test set
# ------------------------------------------------------------
y_pred = tree.predict(X_test)

# Calcul des KPI
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("\n📊 Évaluation du modèle :")
print(f"MAE  (Mean Absolute Error) : {mae:.3f}")
print(f"RMSE (Root Mean Squared Error) : {rmse:.3f}")
print(f"R²   (Coefficient de détermination) : {r2:.3f}")

# ------------------------------------------------------------
# 9️⃣ Importance des variables
# ------------------------------------------------------------
importances = pd.DataFrame({
    "Feature": features,
    "Importance": tree.feature_importances_
}).sort_values(by="Importance", ascending=False)

print("\n🌳 Importance des variables :")
print(importances)

# ------------------------------------------------------------
# 🔟 Sauvegarder le modèle et ses objets associés
# ------------------------------------------------------------
joblib.dump(tree, "saved_model.pkl")
joblib.dump(imputer, "imputer.pkl")
joblib.dump(features, "features.pkl")

print("\n✅ Modèle de régression et fichiers sauvegardés avec succès")


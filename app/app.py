import streamlit as st
import joblib
import pandas as pd
import numpy as np
import snowflake.connector
import os
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt

# --------------------------------------------------------
# 1️ Charger les variables d'environnement (.env)
# --------------------------------------------------------
load_dotenv()

# --------------------------------------------------------
# 2️ Charger modèle, imputer et features
# --------------------------------------------------------
model = joblib.load("saved_model.pkl")
imputer = joblib.load("imputer.pkl")
features = joblib.load("features.pkl")

st.set_page_config(page_title="Prédiction Note Film", layout="wide")

# --------------------------------------------------------
# 3️ Titre
# --------------------------------------------------------
st.title("🎬 Prédiction de la note d’un film")
st.write("Entrez les caractéristiques du film pour estimer sa note moyenne (modèle de régression).")

# --------------------------------------------------------
# 4️ Inputs utilisateur
# --------------------------------------------------------
user_input = {}
col1, col2, col3, col4 = st.columns(4)

with col1:
    user_input["BUDGET"] = st.number_input("Budget", min_value=0.0, value=1000000.0, step=100000.0)
    user_input["NB_GENRES"] = st.slider("Nombre de genres", 1, 10, 3)
with col2:
    user_input["NB_PROVIDERS"] = st.slider("Nombre de providers", 0, 20, 2)
    user_input["POPULARITY"] = st.number_input("Popularité", min_value=0.0, value=50.0)
with col3:
    user_input["RELEASE_YEAR"] = st.slider("Année de sortie", 1900, 2030, 2020)
    user_input["REVENUE"] = st.number_input("Revenue", min_value=0.0, value=1000000.0, step=100000.0)
with col4:
    user_input["RUNTIME"] = st.slider("Durée (min)", 30, 300, 120)
    user_input["VOTE_COUNT"] = st.number_input("Nombre de votes", min_value=0, value=100)

# --------------------------------------------------------
# 5️ Bouton de prédiction utilisateur
# --------------------------------------------------------
if st.button("Prédire la note"):
    X = pd.DataFrame([user_input], columns=features)
    X_imputed = imputer.transform(X)
    pred = model.predict(X_imputed)[0]

    st.subheader("🎯 Résultat")
    st.success(f"Le modèle estime une note moyenne de **{pred:.2f}/10** pour ce film.")

# --------------------------------------------------------
# 6️ Bloc ÉVALUATION DU MODÈLE
# --------------------------------------------------------
st.markdown("---")
st.header("📊 Évaluation du modèle")

try:
    # Connexion Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA_ML")
    )

    df = pd.read_sql("SELECT * FROM GLD_FACT_MOVIE_ML_NUMERIC", conn)
    conn.close()

    # Données pour l’évaluation
    y = df["LABEL_VOTE_AVERAGE"]
    X = df[features]

    # Imputation + prédiction
    X_imputed = imputer.transform(X)
    y_pred = model.predict(X_imputed)

    # Calcul des KPI
    mae = mean_absolute_error(y, y_pred)
    rmse = np.sqrt(mean_squared_error(y, y_pred))
    r2 = r2_score(y, y_pred)

    # Pourcentage de prédictions correctes (tolérance ±0.5)
    tolerance = 0.5
    accuracy = np.mean(np.abs(y - y_pred) <= tolerance) * 100

    # Affichage clair dans 4 colonnes
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("MAE", f"{mae:.3f}")
    col_b.metric("RMSE", f"{rmse:.3f}")
    col_c.metric("R²", f"{r2:.3f}")
    col_d.metric("Précision (±0.5)", f"{accuracy:.1f} %")

    st.caption("👉 La précision indique le pourcentage de prédictions dont l’erreur est inférieure à ±0.5 point de la vraie note.")

    # --------------------------------------------------------
    # 7️ Importance des variables (graphique compact)
    # --------------------------------------------------------
    st.subheader("🌳 Importance des variables")

    importances = pd.DataFrame({
        "Feature": features,
        "Importance": model.feature_importances_
    }).sort_values(by="Importance", ascending=False)

    fig, ax = plt.subplots(figsize=(6, 3))
    ax.barh(importances["Feature"], importances["Importance"])
    ax.set_xlabel("Importance")
    ax.set_ylabel("Variable")
    ax.tick_params(axis='y', labelsize=9)
    ax.invert_yaxis()
    st.pyplot(fig)

except Exception as e:
    st.error(f"Erreur lors du chargement ou de l’évaluation : {e}")

# app.py
import streamlit as st
import joblib
import numpy as np

# -----------------------------
# 1. Charger modèle et imputer
# -----------------------------
model = joblib.load("saved_model.pkl")
imputer = joblib.load("imputer.pkl")
features = joblib.load("features.pkl")

st.set_page_config(page_title="Prédiction de la note d'un film", layout="wide")

# -----------------------------
# 2. Titre
# -----------------------------
st.title("🎬 Estimation de la note moyenne d'un film")
st.write("Entrez les caractéristiques du film pour obtenir une estimation de sa note (sur 10).")

# -----------------------------
# 3. Inputs interactifs
# -----------------------------
user_input = {}
col1, col2, col3, col4 = st.columns(4)  # organisation en colonnes

with col1:
    user_input["BUDGET"] = st.number_sinput("Budget", min_value=0.0, value=1000000.0, step=100000.0)
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

# -----------------------------
# 4. Bouton de prédiction
# -----------------------------
if st.button("Prédire"):
    X = np.array([list(user_input.values())])
    X = imputer.transform(X)
    pred = model.predict(X)[0]

    # Affichage du résultat
    st.success(f"⭐ La note estimée du film est : **{pred:.2f} / 10**")


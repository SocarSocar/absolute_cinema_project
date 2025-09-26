from fastapi import FastAPI
from pydantic import BaseModel, Field
import joblib
import pandas as pd

# Charger modèle et utilitaires
model = joblib.load("saved_model.pkl")
imputer = joblib.load("imputer.pkl")
feature_names = joblib.load("features.pkl")

app = FastAPI()

# Schéma d’entrée : toutes les features sont obligatoires
class MovieFeatures(BaseModel):
    BUDGET: float = Field(..., description="Budget du film")
    NB_GENRES: int = Field(..., description="Nombre de genres")
    NB_PROVIDERS: int = Field(..., description="Nombre de fournisseurs")
    POPULARITY: float = Field(..., description="Popularité")
    RELEASE_YEAR: int = Field(..., description="Année de sortie")
    REVENUE: float = Field(..., description="Revenu")
    RUNTIME: float = Field(..., description="Durée du film")
    VOTE_COUNT: int = Field(..., description="Nombre de votes")

@app.post("/predict")
def predict(movie: MovieFeatures):
    # Convertir en DataFrame
    df = pd.DataFrame([movie.dict()])
    
    # Réordonner les colonnes selon le modèle
    df = df[feature_names]
    
    # Imputation
    X_imputed = imputer.transform(df)
    
    # Prédiction
    prediction = int(model.predict(X_imputed)[0])
    proba = model.predict_proba(X_imputed)[0].tolist()
    
    return {
        "prediction": prediction,
        "probabilities": {"class_0": proba[0], "class_1": proba[1]}
    }

@app.get("/health")
def health():
    return {"status": "ok"}

import pandas as pd
import numpy as np
import os

# Configuration
SHEET_NAME = 0   # 0 = première feuille, ou "NomDeFeuille"
INPUT_FILE = r"C:\Mes données personnelles\Mes documents persos\CESI\JeuDeDonnees\observatoire-2-edition-11-octobre-2025-.xlsx"
OUTPUT_FILE = r"C:\Mes données personnelles\Mes documents persos\CESI\JeuDeDonnees\observatoire_2025_clean.xlsx"

print(f"📂 Fichier d'entrée : {INPUT_FILE}")
print(f"💾 Fichier de sortie : {OUTPUT_FILE}")


# 1) Lecture de l'Excel
df = pd.read_excel(
    INPUT_FILE,
    sheet_name=SHEET_NAME,
    engine="openpyxl"  # généralement OK pour les .xlsx
)

print(f"Feuille lue : {SHEET_NAME} -> {df.shape[0]} lignes, {df.shape[1]} colonnes")
print("Aperçu brut :")
print(df.head(3))

# 2) Suppression des lignes/colonnes vides
df = df.dropna(how="all")           # lignes complètement vides
df = df.dropna(axis=1, how="all")   # colonnes complètement vides

# 3) Nettoyage des noms de colonnes
new_cols = []
for c in df.columns:
    c_str = str(c).strip()          # supprime espaces début/fin
    c_str = c_str.replace("\n", " ")  # vire les retours à la ligne
    c_str = c_str.replace("  ", " ")
    c_str = c_str.replace(" ", "_")   # espaces -> underscore
    c_str = c_str.replace("%", "pct")
    c_str = c_str.replace("é", "e").replace("è", "e").replace("ê", "e").replace("à", "a").replace("ù", "u")
    new_cols.append(c_str)
df.columns = new_cols

print("\nNoms de colonnes nettoyés :")
print(df.columns.tolist())

# 4) Trim des chaînes + uniformisation virgule/point
for col in df.columns:
    if df[col].dtype == object:
        df[col] = df[col].astype(str).str.strip()
        # remplace virgule par point pour les nombres style "12,5"
        df[col] = df[col].str.replace(",", ".", regex=False)

# 5) Tentative de conversion en numérique quand c'est logique
for col in df.columns:
    # on essaie de convertir, si >50% passent en nombre, on garde le type numérique
    converted = pd.to_numeric(df[col], errors="coerce")
    ratio_num = converted.notna().mean()
    if ratio_num > 0.5:  # plus de la moitié des valeurs sont numériques
        df[col] = converted

# 6) Optionnel: gérer certaines colonnes spécifiques connues (d'après l’aperçu)
# Exemple: les pourcentages (texte style "78 %") -> enlever le % et convertir en float
for col in df.columns:
    if "pct" in col.lower() or "taux" in col.lower() or "pourcentage" in col.lower():
        if df[col].dtype == object:
            df[col] = (
                df[col]
                .str.replace("%", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

# 7) Sauvegarde en Excel propre (et/ou CSV)
df.to_excel(OUTPUT_FILE, index=False)

# Sauvegarde en CSV si besoin
# df.to_csv("observatoire_2025_clean.csv", index=False, encoding="utf-8")

print(f"\n✅ Fichier nettoyé sauvegardé : {os.path.abspath(OUTPUT_FILE)}")
print(f"Dimensions finales : {df.shape[0]} lignes x {df.shape[1]} colonnes")

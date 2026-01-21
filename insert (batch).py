import pandas as pd
from pathlib import Path

INPUT_FILE = Path("C:\\Mes données personnelles\\Mes documents persos\\CESI\\JeuDeDonnees\\observatoire_2025_clean.xlsx")
OUT_SQL   = Path("C:\\Mes données personnelles\\Mes documents persos\\CESI\\JeuDeDonnees\\inserts_observatoire(batch).sql")
BATCH_SIZE = 500  # nb de lignes par INSERT

df = pd.read_excel(INPUT_FILE, engine="openpyxl")

# Renommage simplifié
df = df.rename(columns={
    "Nom_Demarche": "Nom_Demarche",
    "ID_Demarche": "ID_Demarche",
    "Perimetre_ministeriel_(AC/SG)": "Perimetre",
    "Ministere_politique": "Ministere",
    "Ref_Administration": "Ref_Administration",
    "Statut_En_ligne": "Statut_En_ligne",
    "Categories_Usagers": "Categories_Usagers",
    "Volumetrie_Totale": "Volumetrie_Totale",
    "Volumetrie_En_Ligne": "Volumetrie_En_Ligne",
    "pct_Recours_au_Numerique": "pct_Recours_au_Numerique",
    "Note_Satisfaction": "Note_Satisfaction",
    "Prise_en_compte_Handicap": "Prise_en_compte_Handicap",
    "Taux_Audit_RGAA": "Taux_Audit_RGAA",
    "Date_Publication_Declaration_(rouge_:_perimee_;_orange_:_bientôt_perimee)": "Date_Declaration",
    "Note_Clarte_Langage": "Note_Clarte_Langage",
    "Niveau_Autonomie": "Niveau_Autonomie",
    "Aide_Joignable": "Aide_Joignable",
    "Aide_Efficace": "Aide_Efficace",
    "FranceConnect": "FranceConnect",
    "Score_DLNUF": "Score_DLNUF",
    "Taux_Dispo": "Taux_Dispo",
    "Tps_Moy_Chargement": "Tps_Moy_Chargement",
    "URL_Demarche": "URL_Demarche",
})

# Helpers SQL
def sql_str(v):
    if pd.isna(v) or v == "nan":
        return "NULL"
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    return f"'{s}'"

def sql_num(v):
    if pd.isna(v) or v == "nan":
        return "NULL"
    return str(v)

def sql_date(v):
    if pd.isna(v) or v == "nan":
        return "NULL"
    s = str(v).strip()
    if not s:
        return "NULL"

    # Cas du type "08/04/2022 (périmée)" -> on garde que la partie date
    # on prend les 10 premiers caractères qui ressemblent à une date
    s = s[:10]

    # Essayer différents formats jour/mois/année
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            d = pd.to_datetime(s, format=fmt)
            return f"'{d.date()}'"
        except Exception:
            continue

    # Si vraiment illisible -> NULL (évite de péter l'INSERT)
    return "NULL"


lines = []

# ================= ministere (DISTINCT + batch) =================
lines.append("-- INSERTS ministere\n")

ministere_map = {}
next_min_id = 1
ministere_values = []

for _, row in df.iterrows():
    key = (row["Perimetre"], row["Ministere"], row["Ref_Administration"])
    if any(pd.isna(k) for k in key):
        continue
    if key not in ministere_map:
        ministere_map[key] = next_min_id
        per, min_pol, ref = key
        ministere_values.append(
            f"({next_min_id}, {sql_str(per)}, {sql_str(min_pol)}, {sql_str(ref)})"
        )
        next_min_id += 1

for i in range(0, len(ministere_values), BATCH_SIZE):
    batch = ministere_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO ministere (id_ministere, perimetre_ministeriel, ministere_politique, ref_administration)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

# ================= demarche (batch) =================
lines.append("\n-- INSERTS demarche\n")

demarche_values = []

for _, row in df.iterrows():
    if pd.isna(row["ID_Demarche"]):
        continue

    key = (row["Perimetre"], row["Ministere"], row["Ref_Administration"])
    id_min = ministere_map.get(key, "NULL")

    val = (
        f"({int(row['ID_Demarche'])}, "
        f"{sql_str(row['Nom_Demarche'])}, "
        f"{id_min}, "
        f"{sql_str(row['Statut_En_ligne'])}, "
        f"{sql_num(row['Volumetrie_Totale'])}, "
        f"{sql_num(row['Volumetrie_En_Ligne'])}, "
        f"{sql_num(row['pct_Recours_au_Numerique'])}, "
        f"{sql_num(row['Note_Satisfaction'])}, "
        f"{sql_num(row['Note_Clarte_Langage'])}, "
        f"{sql_num(row['Niveau_Autonomie'])}, "
        f"{sql_num(row['Aide_Joignable'])}, "
        f"{sql_num(row['Aide_Efficace'])}, "
        f"{sql_str(row['Prise_en_compte_Handicap'])}, "
        f"{sql_num(row['Taux_Audit_RGAA'])}, "
        f"{sql_date(row['Date_Declaration'])}, "
        f"{sql_str(row['FranceConnect'])}, "
        f"{sql_num(row['Score_DLNUF'])}, "
        f"{sql_num(row['Taux_Dispo'])}, "
        f"{sql_num(row['Tps_Moy_Chargement'])}, "
        f"{sql_str(row['URL_Demarche'])}"
        ")"
    )
    demarche_values.append(val)

for i in range(0, len(demarche_values), BATCH_SIZE):
    batch = demarche_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO demarche (\n"
        "  id_demarche, nom_demarche, id_ministere,\n"
        "  statut_en_ligne, volumetrie_totale, volumetrie_en_ligne, pct_recours_numerique,\n"
        "  note_satisfaction, note_clarte_langage, niveau_autonomie, aide_joignable, aide_efficace,\n"
        "  prise_en_compte_handicap, taux_audit_rgaa, date_declaration, franceconnect, score_dlnuf,\n"
        "  taux_dispo, tps_moy_chargement, url_demarche\n"
        ") VALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

# ================= categorieusager + demarche_categorieusager =================
lines.append("\n-- INSERTS categorieusager & demarche_categorieusager\n")

cat_map = {}
next_cat_id = 1
cat_values = []
link_values = []

for _, row in df.iterrows():
    if pd.isna(row["ID_Demarche"]):
        continue
    dem_id = int(row["ID_Demarche"])
    cats = row["Categories_Usagers"]
    if pd.isna(cats):
        continue
    for raw in str(cats).split(";"):
        lib = raw.strip()
        if not lib:
            continue
        if lib not in cat_map:
            cat_map[lib] = next_cat_id
            cat_values.append(f"({next_cat_id}, {sql_str(lib)})")
            next_cat_id += 1
        id_cat = cat_map[lib]
        link_values.append(f"({dem_id}, {id_cat})")

for i in range(0, len(cat_values), BATCH_SIZE):
    batch = cat_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO categorieusager (id_categorie, libelle)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

for i in range(0, len(link_values), BATCH_SIZE):
    batch = link_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO demarche_categorieusager (id_demarche, id_categorie)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

# ================= indicateursaccessibilite =================
lines.append("\n-- INSERTS indicateursaccessibilite\n")

ind_values = []

for _, row in df.iterrows():
    if pd.isna(row["ID_Demarche"]):
        continue
    dem_id = int(row["ID_Demarche"])
    ind_values.append(
        f"({dem_id}, "
        f"{sql_str(row['Prise_en_compte_Handicap'])}, "
        f"{sql_num(row['Taux_Audit_RGAA'])}, "
        f"{sql_str(row['FranceConnect'])}, "
        f"{sql_num(row['Score_DLNUF'])})"
    )

for i in range(0, len(ind_values), BATCH_SIZE):
    batch = ind_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO indicateursaccessibilite (\n"
        "  id_demarche, prise_en_compte_handicap, taux_audit_rgaa, franceconnect, score_dlnuf\n"
        ") VALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

OUT_SQL.write_text("\n".join(lines), encoding="utf-8")
print("✅ Fichier SQL généré :", OUT_SQL.resolve())

import pandas as pd
from pathlib import Path


# ---------- CONFIG DE BASE ----------

# Chemin vers ton Excel nettoyé
INPUT_FILE = Path("C:\\Mes données personnelles\\Mes documents persos\\CESI\\JeuDeDonnees\\observatoire_2025_clean.xlsx")

# Chemin du fichier SQL qui sera généré
OUT_SQL   = Path("C:\\Mes données personnelles\\Mes documents persos\\CESI\\JeuDeDonnees\\inserts_observatoire(batch).sql")

# Combien de lignes on met par INSERT (multi‑VALUES)
BATCH_SIZE = 500  # nombre de lignes par INSERT


# On charge l'Excel dans un DataFrame pandas
df = pd.read_excel(INPUT_FILE, engine="openpyxl")


# ---------- RENOMMAGE DES COLONNES ----------

# On renomme les colonnes pour avoir des noms plus simples à manipuler
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


# ---------- FONCTIONS UTILITAIRES POUR FAIRE DU SQL ----------

def sql_str(v):
    # pd.isna gère NaN, None, etc. ; on traite aussi explicitement la string "nan"
    if pd.isna(v) or v == "nan":
        return "NULL"
    # On convertit en str et on échappe les antislash et quotes simples
    s = str(v).replace("\\", "\\\\").replace("'", "''")
    # On renvoie la valeur encadrée par des quotes simples
    return f"'{s}'"


def sql_num(v):
    if pd.isna(v) or v == "nan":
        return "NULL"
    # Pas de quotes, MariaDB interprète ça comme un nombre
    return str(v)


def sql_date(v):
    # Cas valeur manquante
    if pd.isna(v) or v == "nan":
        return "NULL"
    # On prend la valeur en string, on supprime les espaces autour
    s = str(v).strip()
    if not s:
        return "NULL"

    # Exemple typique : "08/04/2022 (périmée)" -> on garde juste "08/04/2022"
    s = s[:10]

    # On tente plusieurs formats de date courants
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            # Si la conversion marche avec ce format, on sort la date au format SQL
            d = pd.to_datetime(s, format=fmt)
            return f"'{d.date()}'"   # ex: '2022-04-08'
        except Exception:
            # Si ça plante, on teste le format suivant
            continue

    # Si aucun format ne marche, on abandonne et on met NULL
    return "NULL"



# ---------- GÉNÉRATION DES INSERTS SQL EN BATCHS ----------


# Ici on va stocker toutes les lignes SQL sous forme de texte
lines = []


# =====================================================
# 1) INSERTS POUR LA TABLE ministere (DISTINCT + BATCH)
# =====================================================

lines.append("-- INSERTS ministere\n")

# Dictionnaire pour ne pas insérer 50 fois le même ministère
ministere_map = {}   # (Perimetre, Ministere, Ref_Administration) -> id_ministere
next_min_id = 1      # compteur pour générer id_ministere à la main
ministere_values = []  # liste des tuples SQL "(..., ..., ...)"


# On parcourt toutes les lignes de l'Excel
for _, row in df.iterrows():
    key = (row["Perimetre"], row["Ministere"], row["Ref_Administration"])
    # Si un des trois est NaN -> on skip
    if any(pd.isna(k) for k in key):
        continue
    # Si ce combo de ministère n'a pas encore été vu, on lui donne un id
    if key not in ministere_map:
        ministere_map[key] = next_min_id
        per, min_pol, ref = key
        ministere_values.append(
            f"({next_min_id}, {sql_str(per)}, {sql_str(min_pol)}, {sql_str(ref)})"
        )
        next_min_id += 1

# On découpe la liste en batchs et on fait des gros INSERT multi-VALUES
for i in range(0, len(ministere_values), BATCH_SIZE):
    batch = ministere_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO ministere (id_ministere, perimetre_ministeriel, ministere_politique, ref_administration)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )


# ========================================
# 2) INSERTS POUR LA TABLE demarche (BATCH)
# ========================================

lines.append("\n-- INSERTS demarche\n")

demarche_values = []

for _, row in df.iterrows():
    # Si pas d'ID de démarche, on ignore la ligne
    if pd.isna(row["ID_Demarche"]):
        continue

    # On retrouve l'id_ministere correspondant à cette ligne
    key = (row["Perimetre"], row["Ministere"], row["Ref_Administration"])
    id_min = ministere_map.get(key, "NULL")

    # On fabrique la ligne VALUES (...) complète pour cette démarche
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

# On envoie les demarches en batchs
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


# ===========================================================
# 3) INSERTS categorieusager + demarche_categorieusager (BATCH)
# ===========================================================

lines.append("\n-- INSERTS categorieusager & demarche_categorieusager\n")

cat_map = {}     # libelle -> id_categorie
next_cat_id = 1
cat_values = []   # pour remplir categorieusager
link_values = []  # pour remplir demarche_categorieusager

for _, row in df.iterrows():
    if pd.isna(row["ID_Demarche"]):
        continue
    dem_id = int(row["ID_Demarche"])
    cats = row["Categories_Usagers"]
    if pd.isna(cats):
        continue
    # Plusieurs catégories possibles séparées par ";"
    for raw in str(cats).split(";"):
        lib = raw.strip()
        if not lib:
            continue
        # On crée l'entrée dans categorieusager si besoin
        if lib not in cat_map:
            cat_map[lib] = next_cat_id
            cat_values.append(f"({next_cat_id}, {sql_str(lib)})")
            next_cat_id += 1
        id_cat = cat_map[lib]
        # Et la relation dans demarche_categorieusager
        link_values.append(f"({dem_id}, {id_cat})")

# Batch d'INSERT dans categorieusager
for i in range(0, len(cat_values), BATCH_SIZE):
    batch = cat_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO categorieusager (id_categorie, libelle)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )

# Batch d'INSERT dans demarche_categorieusager
for i in range(0, len(link_values), BATCH_SIZE):
    batch = link_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO demarche_categorieusager (id_demarche, id_categorie)\nVALUES\n  "
        + ",\n  ".join(batch) + ";"
    )


# ======================================
# 4) INSERTS indicateursaccessibilite
# ======================================

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

# Batch d'INSERT
for i in range(0, len(ind_values), BATCH_SIZE):
    batch = ind_values[i:i+BATCH_SIZE]
    lines.append(
        "INSERT INTO indicateursaccessibilite (\n"
        "  id_demarche, prise_en_compte_handicap, taux_audit_rgaa, franceconnect, score_dlnuf\n"
        ") VALUES\n  "
        + ",\n  ".join(batch) + ";"
    )


# ---------- ÉCRITURE DU FICHIER .SQL FINAL ----------

OUT_SQL.write_text("\n".join(lines), encoding="utf-8")
print("✅ Fichier SQL généré :", OUT_SQL.resolve())
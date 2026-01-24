python -m pip install numpy pandas openpyxl
pip install pandas


Lit la première feuille de ton Excel.

Supprime lignes/colonnes vides.

Nettoie les noms de colonnes (accents, espaces, %, etc.) pour être SQL‑friendly.

Nettoie les chaînes (strip, remplace , par .) et convertit en nombres quand c’est pertinent (volumétrie, notes, pourcentages).

Sauvegarde un nouveau fichier observatoire_2025_clean.xlsx prêt pour l’import dans ta BDD.


https://www.data.gouv.fr/datasets/observatoire-de-la-qualite-des-demarches-en-ligne
L'Observatoire "Vos Démarches Essentielles" (VDE) est un dispositif gouvernemental chargé d’évaluer et d’améliorer la qualité des services publics numériques à destination des particuliers, entreprises et associations. 
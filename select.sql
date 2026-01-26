SELECT d.id_demarche,
       d.nom_demarche,
       m.ministere_politique
FROM demarche d
JOIN demarche_categorieusager dc ON d.id_demarche = dc.id_demarche
JOIN categorieusager cu          ON dc.id_categorie = cu.id_categorie
JOIN ministere m                 ON d.id_ministere = m.id_ministere
WHERE cu.libelle = 'Particulier'
ORDER BY m.ministere_politique, d.nom_demarche;


SELECT d.id_demarche,
       d.nom_demarche,
       d.volumetrie_totale,
       d.note_satisfaction
FROM demarche d
WHERE d.volumetrie_totale > 500000
  AND d.note_satisfaction IS NOT NULL
  AND d.note_satisfaction < 6
ORDER BY d.note_satisfaction ASC, d.volumetrie_totale DESC;

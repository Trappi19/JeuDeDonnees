-- Ministere
CREATE TABLE Ministere (
    id_ministere INT AUTO_INCREMENT PRIMARY KEY,
    perimetre_ministeriel VARCHAR(255) NOT NULL,
    ministere_politique   VARCHAR(255) NOT NULL,
    ref_administration    VARCHAR(50)  NOT NULL
) ENGINE=InnoDB;

-- Demarche
CREATE TABLE Demarche (
    id_demarche INT PRIMARY KEY,
    nom_demarche VARCHAR(500) NOT NULL,
    id_ministere INT NOT NULL,

    statut_en_ligne VARCHAR(50),
    volumetrie_totale BIGINT,
    volumetrie_en_ligne BIGINT,
    pct_recours_numerique DECIMAL(10,6),

    note_satisfaction DECIMAL(3,1),
    note_clarte_langage DECIMAL(3,1),
    niveau_autonomie DECIMAL(5,2),
    aide_joignable DECIMAL(5,2),
    aide_efficace DECIMAL(5,2),

    prise_en_compte_handicap VARCHAR(50),
    taux_audit_rgaa DECIMAL(10,4),
    date_declaration DATE,
    franceconnect VARCHAR(50),
    score_dlnuf INT,

    taux_dispo DECIMAL(10,5),
    tps_moy_chargement INT,
    url_demarche TEXT,

    CONSTRAINT fk_demarche_ministere
        FOREIGN KEY (id_ministere) REFERENCES Ministere(id_ministere)
) ENGINE=InnoDB;

-- CategorieUsager + table de liaison
CREATE TABLE CategorieUsager (
    id_categorie INT AUTO_INCREMENT PRIMARY KEY,
    libelle VARCHAR(50) UNIQUE NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Demarche_CategorieUsager (
    id_demarche INT,
    id_categorie INT,
    PRIMARY KEY (id_demarche, id_categorie),
    FOREIGN KEY (id_demarche) REFERENCES Demarche(id_demarche),
    FOREIGN KEY (id_categorie) REFERENCES CategorieUsager(id_categorie)
) ENGINE=InnoDB;

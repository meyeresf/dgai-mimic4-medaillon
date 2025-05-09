-- ############################################################
-- # MIMIC-IV Datenextraktion: Bronze-Ebene
-- #
-- # Dieses Skript erstellt ein Bronze-Schema und eine Tabelle 
-- # zur einheitlichen Speicherung klinischer Parameter aus 
-- # verschiedenen MIMIC-IV-Quelltabellen. Es extrahiert Vitalparameter,
-- # Laborwerte, Medikamentengaben und Ausscheidungen.
-- #
-- # Erstellt von Falk Meyer-Eschenbach
-- # E-Mail: falk.meyer-eschenbach@charite.de
-- #
-- # Primäre Zielgruppe: Anästhesisten und klinische Forscher
-- # Datum: 09.05.2025
-- ############################################################

-- ############################################################
-- # 1. Schema-Erstellung
-- # Wir erstellen zunächst ein eigenes Schema für die Bronze-Ebene,
-- # um die Daten sauber von den Originaldaten zu trennen.
-- ############################################################

CREATE SCHEMA IF NOT EXISTS bronze_schema;

-- ############################################################
-- # 2. Tabellen-Erstellung
-- # Die Bronze-Tabelle verwendet ein einheitliches Format für alle
-- # klinischen Parameter, unabhängig von ihrer Quelltabelle.
-- # Dies ermöglicht eine einheitliche Abfrage verschiedener Parameter.
-- ############################################################

CREATE TABLE IF NOT EXISTS bronze_schema.clinical_parameters (
    id SERIAL PRIMARY KEY,                   -- Eindeutige ID für jeden Eintrag
    subject_id INTEGER NOT NULL,             -- Patienten-ID
    hadm_id INTEGER,                         -- Krankenhaus-Aufnahme-ID
    stay_id INTEGER,                         -- ICU-Aufenthalt-ID
    charttime TIMESTAMP NOT NULL,            -- Zeitpunkt der Messung/Verabreichung
    storetime TIMESTAMP,                     -- Zeitpunkt der Dateneingabe (falls verfügbar)
    itemid INTEGER NOT NULL,                 -- ID des Parameters (aus d_items/d_labitems)
    parameter_name VARCHAR(255) NOT NULL,    -- Name des Parameters (z.B. "Herzfrequenz")
    value TEXT,                              -- Wert als Text (für Werte mit Einheiten oder Anmerkungen)
    valuenum NUMERIC,                        -- Numerischer Wert (für Berechnungen)
    valueuom VARCHAR(50),                    -- Einheit (z.B. "mmHg", "mg/dl")
    warning BOOLEAN DEFAULT FALSE,           -- Flag für Warnungen
    error BOOLEAN DEFAULT FALSE,             -- Flag für Fehler/abnormale Werte
    source_table VARCHAR(50) NOT NULL        -- Quelltabelle (für Rückverfolgbarkeit)
);

-- ############################################################
-- # 3. Index-Erstellung
-- # Indizes beschleunigen Abfragen erheblich, insbesondere bei
-- # großen Datensätzen wie MIMIC-IV. Wir erstellen Indizes für
-- # häufig abgefragte Spalten.
-- ############################################################

CREATE INDEX IF NOT EXISTS idx_clinical_parameters_subject_id 
    ON bronze_schema.clinical_parameters(subject_id);
CREATE INDEX IF NOT EXISTS idx_clinical_parameters_itemid 
    ON bronze_schema.clinical_parameters(itemid);
CREATE INDEX IF NOT EXISTS idx_clinical_parameters_charttime 
    ON bronze_schema.clinical_parameters(charttime);

-- ############################################################
-- # 4. Datenextraktion und -insertion
-- # Im Folgenden werden Daten aus verschiedenen Quelltabellen
-- # extrahiert und in die Bronze-Tabelle eingefügt.
-- # Für jede Quelltabelle gibt es einen eigenen INSERT-Befehl.
-- ############################################################

-- ############################################################
-- # 4.1 Extraktion von Vitalparametern (chartevents)
-- # Erweitert um GCS und Beatmungsparameter für den SOFA-Score
-- ############################################################

INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.charttime,
    c.storetime,
    c.itemid,
    d.label AS parameter_name,
    c.value,
    c.valuenum,
    c.valueuom,
    FALSE AS warning,
    FALSE AS error,
    'chartevents' AS source_table
FROM mimiciv_icu.chartevents c
JOIN mimiciv_icu.d_items d ON c.itemid = d.itemid
WHERE c.itemid IN (
    -- Originale Vitalparameter
    220045, -- Herzfrequenz
    220050, -- Arterieller Blutdruck systolisch
    220179, -- Nicht-invasiver Blutdruck systolisch
    220180, -- Nicht-invasiver Blutdruck diastolisch
    223761, -- Temperatur (°C)
    223762, -- Temperatur (°F)
    220210, -- Atemfrequenz
    220277, -- O2 Sättigung
    220339, -- Zentraler Venendruck (CVP)
    227428, -- SpO2-FiO2 Verhältnis
    220074, -- Mittlerer arterieller Druck (MAP)
    
    -- Neu hinzugefügte GCS-Parameter für SOFA-Score
    220739, -- GCS Total
    223900, -- GCS Verbal
    223901, -- GCS Motor
    220210, -- GCS Eye
    
    -- Neu hinzugefügte Beatmungsparameter für SOFA-Score
    224688, -- Ventilator Mode
    220339, -- Ventilation Status
    224690, -- Respiratory Rate (Set)
    224687, -- Respiratory Rate (Total)
    224684, -- Tidal Volume (Set)
    224685, -- Tidal Volume (Observed)
    224696, -- PEEP (Positive End Expiratory Pressure)
    223835, -- FiO2
    224695, -- Peak Inspiratory Pressure
    224738, -- Plateau Pressure
    224700  -- Driving Pressure
)
-- Vermeidung von Duplikaten durch Überprüfung vorhandener Einträge
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = c.subject_id
    AND cp.charttime = c.charttime
    AND cp.itemid = c.itemid
    AND cp.source_table = 'chartevents'
);

-- ############################################################
-- # 4.2 Extraktion von Laborwerten (labevents)
-- # Erweitert um zusätzliche Laborparameter für den SOFA-Score
-- ############################################################

INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    l.subject_id,
    l.hadm_id,
    NULL AS stay_id,  -- labevents hat keine stay_id
    l.charttime,
    l.storetime,
    l.itemid,
    d.label AS parameter_name,
    CAST(l.value AS TEXT) AS value,
    l.valuenum,
    l.valueuom,
    FALSE AS warning,
    (l.flag = 'abnormal') AS error,  -- Flag für abnormale Werte
    'labevents' AS source_table
FROM mimiciv_hosp.labevents l
JOIN mimiciv_hosp.d_labitems d ON l.itemid = d.itemid
WHERE l.itemid IN (
    -- Originale Laborwerte
    50912, -- Kreatinin
    50971, -- Kalium
    50983, -- Natrium
    50885, -- Bilirubin total
    50883, -- Bilirubin direkt (konjugiert)
    51006, -- Urea Nitrogen (BUN)
    50802, -- Base Excess
    50821, -- pCO2 arterial
    50818, -- pO2 arterial
    50820, -- pH arterial
    50809, -- Glucose
    50811, -- Hämoglobin
    51222, -- Laktat
    51300, -- WBC (Leukozyten)
    51301, -- Thrombozyten
    51265, -- INR
    
    -- Neu hinzugefügte Laborparameter für SOFA-Score und Sepsis-3
    50878, -- AST (SGOT) - für Leberfunktion
    50861, -- ALT (SGPT) - für Leberfunktion
    51250, -- CRP - Entzündungsmarker für Sepsis
    51139, -- Laktatdehydrogenase (LDH)
    51288, -- Procalcitonin - wichtiger Sepsismarker
    51116, -- Ammoniak - für Leberfunktion
    51196, -- D-Dimer - für Gerinnungsstörungen
    51275  -- Troponin T - für Myokardschädigung
)
-- Vermeidung von Duplikaten durch Überprüfung vorhandener Einträge
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = l.subject_id
    AND cp.charttime = l.charttime
    AND cp.itemid = l.itemid
    AND cp.source_table = 'labevents'
);

-- ############################################################
-- # 4.3 Extraktion von Medikamentengaben (inputevents)
-- # Erweitert um zusätzliche Vasopressoren für den SOFA-Score
-- ############################################################

INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    i.starttime AS charttime,  -- Wir verwenden starttime als charttime
    NULL AS storetime,
    i.itemid,
    d.label AS parameter_name,
    CAST(i.amount AS TEXT) AS value,
    i.amount AS valuenum,
    i.amountuom AS valueuom,
    FALSE AS warning,
    (i.statusdescription = 'Rewritten') AS error,  -- Flag für umgeschriebene Einträge
    'inputevents' AS source_table
FROM mimiciv_icu.inputevents i
JOIN mimiciv_icu.d_items d ON i.itemid = d.itemid
WHERE i.itemid IN (
    -- Originale Medikamente
    221906, -- Norepinephrin
    221289, -- Propofol
    221662, -- Dopamin
    221668, -- Dobutamin
    221828, -- Fentanyl
    221744, -- Midazolam
    222315, -- Ketamin
    221749, -- Morphin
    222168, -- Rocuronium
    221712, -- Isofluran (gas)
    221372, -- Sevofluran (gas)
    221261, -- Phenylephrin
    
    -- Neu hinzugefügte Vasopressoren für SOFA-Score
    221289, -- Epinephrin (Adrenalin)
    222315  -- Vasopressin
)
-- Vermeidung von Duplikaten durch Überprüfung vorhandener Einträge
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = i.subject_id
    AND cp.charttime = i.starttime
    AND cp.itemid = i.itemid
    AND cp.source_table = 'inputevents'
);
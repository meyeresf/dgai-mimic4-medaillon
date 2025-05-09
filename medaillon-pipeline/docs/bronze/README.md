# Bronze-Ebene: Datenextraktion

Diese Anleitung führt Sie durch den Prozess der Erstellung einer Bronze-Tabelle, die Rohdaten aus dem MIMIC-IV-Datensatz in einem einheitlichen Format enthält.

## Inhaltsverzeichnis

- [Bronze-Ebene: Datenextraktion](#bronze-ebene-datenextraktion)
  - [Inhaltsverzeichnis](#inhaltsverzeichnis)
  - [1. Überblick](#1-überblick)
  - [2. Vorbereitung](#2-vorbereitung)
  - [3. Erstellung des Bronze-Schemas](#3-erstellung-des-bronze-schemas)
  - [4. Erstellung der Bronze-Tabelle](#4-erstellung-der-bronze-tabelle)
  - [5. Extraktion von Daten aus Quelltabellen](#5-extraktion-von-daten-aus-quelltabellen)
    - [5.1 Extraktion aus chartevents](#51-extraktion-aus-chartevents)
    - [5.2 Extraktion aus labevents](#52-extraktion-aus-labevents)
    - [5.3 Extraktion aus inputevents](#53-extraktion-aus-inputevents)
    - [5.4 Extraktion aus outputevents](#54-extraktion-aus-outputevents)
  - [6. Kombination von Abfragen mit UNION ALL](#6-kombination-von-abfragen-mit-union-all)
  - [8. Erstellung eines vollständigen SQL-Skripts](#8-erstellung-eines-vollständigen-sql-skripts)
  - [9. Nächste Schritte](#9-nächste-schritte)

## 1. Überblick

Die Bronze-Ebene ist der erste Schritt in unserer Datenaufbereitungspipeline. Hier extrahieren wir die Rohdaten aus verschiedenen Tabellen des MIMIC-IV-Datensatzes und strukturieren sie in einem einheitlichen Format. Dabei führen wir nur minimale Transformationen durch, um die Vollständigkeit und Nachvollziehbarkeit der Daten zu gewährleisten.

## 2. Vorbereitung

Stellen Sie sicher, dass Sie Zugriff auf die MIMIC-IV-Datenbank haben und dass pgAdmin korrekt eingerichtet ist. Überprüfen Sie die Konfiguration in der Datei `config/bronze/database.yaml`.

## 3. Erstellung des Bronze-Schemas

Zunächst erstellen wir ein separates Schema für unsere Bronze-Tabellen:

```sql
-- Erstellen des Bronze-Schemas
CREATE SCHEMA IF NOT EXISTS bronze_schema;
```

## 4. Erstellung der Bronze-Tabelle

Nun erstellen wir die Bronze-Tabelle mit einem einheitlichen Format für alle klinischen Parameter:

```sql
-- Erstellen der Bronze-Tabelle für klinische Parameter
CREATE TABLE IF NOT EXISTS bronze_schema.clinical_parameters (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER,
    stay_id INTEGER,
    charttime TIMESTAMP NOT NULL,
    storetime TIMESTAMP,
    itemid INTEGER NOT NULL,
    parameter_name VARCHAR(255) NOT NULL,
    value TEXT,
    valuenum NUMERIC,
    valueuom VARCHAR(50),
    warning BOOLEAN DEFAULT FALSE,
    error BOOLEAN DEFAULT FALSE,
    source_table VARCHAR(50) NOT NULL
);

-- Erstellen eines Indexes für schnellere Abfragen
CREATE INDEX IF NOT EXISTS idx_clinical_parameters_subject_id ON bronze_schema.clinical_parameters(subject_id);
CREATE INDEX IF NOT EXISTS idx_clinical_parameters_itemid ON bronze_schema.clinical_parameters(itemid);
CREATE INDEX IF NOT EXISTS idx_clinical_parameters_charttime ON bronze_schema.clinical_parameters(charttime);
```

## 5. Extraktion von Daten aus Quelltabellen

Für verschiedene Quelltabellen benötigen wir unterschiedliche SQL-Abfragen. Hier sind Beispiele für einige der wichtigsten Tabellen:

### 5.1 Extraktion aus chartevents

```sql
-- Extraktion von Vitalparametern aus chartevents
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
    220045, -- Herzfrequenz
    220050, -- Arterial Blood Pressure systolic
    220179, -- Nicht-invasiver Blutdruck systolisch
    220180  -- Nicht-invasiver Blutdruck diastolisch
)
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = c.subject_id
    AND cp.charttime = c.charttime
    AND cp.itemid = c.itemid
    AND cp.source_table = 'chartevents'
);

-- Überprüfen der eingefügten Daten
WITH ranked_data AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY itemid ORDER BY charttime) as rn
  FROM bronze_schema.clinical_parameters 
  WHERE itemid IN (220045, 220050, 220179, 220180) 
  AND source_table = 'chartevents'
)
SELECT * FROM ranked_data WHERE rn <= 10;
```

### 5.2 Extraktion aus labevents

```sql
-- Extraktion von Laborwerten aus labevents
INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    l.subject_id,
    l.hadm_id,
    NULL AS stay_id,
    l.charttime,
    l.storetime,
    l.itemid,
    d.label AS parameter_name,
    CAST(l.value AS TEXT) AS value,
    l.valuenum,
    l.valueuom,
    FALSE AS warning,
    (l.flag = 'abnormal') AS error,
    'labevents' AS source_table
FROM mimiciv_hosp.labevents l
JOIN mimiciv_hosp.d_labitems d ON l.itemid = d.itemid
WHERE l.itemid IN (
    50912, -- Kreatinin
    50971, -- Kalium
    50983  -- Natrium
)
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = l.subject_id
    AND cp.charttime = l.charttime
    AND cp.itemid = l.itemid
    AND cp.source_table = 'labevents'
);

-- Überprüfen der eingefügten Daten
WITH ranked_data AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY itemid ORDER BY charttime) as rn
  FROM bronze_schema.clinical_parameters 
  WHERE itemid IN (50912, 50971, 50983) 
  AND source_table = 'labevents'
)
SELECT * FROM ranked_data WHERE rn <= 10;
```

### 5.3 Extraktion aus inputevents

```sql
-- Extraktion von Medikamentengaben aus inputevents
INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    i.starttime AS charttime,
    NULL AS storetime,
    i.itemid,
    d.label AS parameter_name,
    CAST(i.amount AS TEXT) AS value,
    i.amount AS valuenum,
    i.amountuom AS valueuom,
    FALSE AS warning,
    (i.statusdescription = 'Rewritten') AS error,
    'inputevents' AS source_table
FROM mimiciv_icu.inputevents i
JOIN mimiciv_icu.d_items d ON i.itemid = d.itemid
WHERE i.itemid IN (
    221906, -- Norepinephrin
    221289, -- Propofol
    221662  -- Dopamin
)
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = i.subject_id
    AND cp.charttime = i.starttime
    AND cp.itemid = i.itemid
    AND cp.source_table = 'inputevents'
);

-- Überprüfen der eingefügten Daten
WITH ranked_data AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY itemid ORDER BY charttime) as rn
  FROM bronze_schema.clinical_parameters 
  WHERE itemid IN (221906, 221289, 221662) 
  AND source_table = 'inputevents'
)
SELECT * FROM ranked_data WHERE rn <= 10;
```

### 5.4 Extraktion aus outputevents

```sql
-- Extraktion von Ausscheidungen aus outputevents
INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT 
    o.subject_id,
    o.hadm_id,
    o.stay_id,
    o.charttime,
    o.storetime,
    o.itemid,
    d.label AS parameter_name,
    CAST(o.value AS TEXT) AS value,
    o.value AS valuenum,
    o.valueuom,
    FALSE AS warning,
    FALSE AS error,
    'outputevents' AS source_table
FROM mimiciv_icu.outputevents o
JOIN mimiciv_icu.d_items d ON o.itemid = d.itemid
WHERE o.itemid IN (
    226559, -- Urinausscheidung
    226560, -- Urinausscheidung (Foley)
    226561  -- Urinausscheidung (Void)
)
AND NOT EXISTS (
    SELECT 1 FROM bronze_schema.clinical_parameters cp
    WHERE cp.subject_id = o.subject_id
    AND cp.charttime = o.charttime
    AND cp.itemid = o.itemid
    AND cp.source_table = 'outputevents'
);

-- Überprüfen der eingefügten Daten
WITH ranked_data AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY itemid ORDER BY charttime) as rn
  FROM bronze_schema.clinical_parameters 
  WHERE itemid IN (226559, 226560, 226561) 
  AND source_table = 'outputevents'
)
SELECT * FROM ranked_data WHERE rn <= 10;
```

## 6. Kombination von Abfragen mit UNION ALL

Um Daten aus mehreren Tabellen in einem Durchgang zu extrahieren, können wir die obigen Abfragen mit UNION ALL kombinieren:

```sql
-- Kombinierte Extraktion aus mehreren Tabellen
INSERT INTO bronze_schema.clinical_parameters (
    subject_id, hadm_id, stay_id, charttime, storetime, itemid, parameter_name,
    value, valuenum, valueuom, warning, error, source_table
)
SELECT * FROM (
    -- Extraktion aus chartevents
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
    WHERE c.itemid IN (220045, 220050, 220179, 220180)
    AND NOT EXISTS (
        SELECT 1 FROM bronze_schema.clinical_parameters cp
        WHERE cp.subject_id = c.subject_id
            AND cp.charttime = c.charttime
            AND cp.itemid = c.itemid
            AND cp.source_table = 'chartevents')
    UNION ALL
    
    -- Extraktion aus labevents
    SELECT 
        l.subject_id,
        l.hadm_id,
        NULL AS stay_id,
        l.charttime,
        l.storetime,
        l.itemid,
        d.label AS parameter_name,
        CAST(l.value AS TEXT) AS value,
        l.valuenum,
        l.valueuom,
        FALSE AS warning,
        (l.flag = 'abnormal') AS error,
        'labevents' AS source_table
    FROM mimiciv_hosp.labevents l
    JOIN mimiciv_hosp.d_labitems d ON l.itemid = d.itemid
    WHERE l.itemid IN (50912, 50971, 50983)
    AND NOT EXISTS (
        SELECT 1 FROM bronze_schema.clinical_parameters cp
        WHERE cp.subject_id = l.subject_id
            AND cp.charttime = l.charttime
            AND cp.itemid = l.itemid
            AND cp.source_table = 'labevents')
) AS combined_data;
```


## 8. Erstellung eines vollständigen SQL-Skripts

Basierend auf den obigen Beispielen können Sie ein vollständiges SQL-Skript erstellen, das:

1. Das Bronze-Schema erstellt
2. Die Bronze-Tabelle erstellt
3. Daten aus verschiedenen Quelltabellen extrahiert und in die Bronze-Tabelle einfügt

Speichern Sie dieses Skript in der Datei `src/bronze/create_bronze_tables.sql`.


## 9. Nächste Schritte

Nachdem Sie die Bronze-Tabelle erstellt und mit Daten gefüllt haben, können Sie zur Silver-Ebene übergehen, wo wir die Daten standardisieren und die Qualität sichern werden. Weitere Informationen finden Sie in der Datei [../docs/silver/README.md](../docs/silver/README.md).



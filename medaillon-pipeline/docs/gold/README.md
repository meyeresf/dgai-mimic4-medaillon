# Gold-Ebene: Feature-Engineering

Diese Anleitung führt Sie durch den Prozess des Feature-Engineerings auf der Gold-Ebene unserer Datenaufbereitungspipeline.

## Inhaltsverzeichnis

1. [Überblick](#1-überblick)
2. [Vorbereitung](#2-vorbereitung)
3. [Erstellung des Gold-Schemas](#3-erstellung-des-gold-schemas)
4. [Berechnung abgeleiteter Parameter](#4-berechnung-abgeleiteter-parameter)
5. [Zeitliche Aggregation](#5-zeitliche-aggregation)
6. [Berechnung klinischer Scores](#6-berechnung-klinischer-scores)
7. [Erstellung eines vollständigen SQL-Skripts](#7-erstellung-eines-vollständigen-sql-skripts)
8. [Nächste Schritte](#8-nächste-schritte)

## 1. Überblick

Die Gold-Ebene ist der letzte Schritt in unserer Datenaufbereitungspipeline. Hier bereiten wir die standardisierten und qualitätsgesicherten Daten aus der Silver-Ebene für spezifische Analysen auf. Dies umfasst die Berechnung abgeleiteter Parameter, zeitliche Aggregation und die Berechnung klinischer Scores.

## 2. Vorbereitung

Stellen Sie sicher, dass Sie die Silver-Ebene erfolgreich eingerichtet haben und dass die Silver-Tabellen mit Daten gefüllt sind. Überprüfen Sie die Konfiguration in der Datei `config/gold/database.yaml`.

## 3. Erstellung des Gold-Schemas

Zunächst erstellen wir ein separates Schema für unsere Gold-Tabellen:

```sql
-- Erstellen des Gold-Schemas
CREATE SCHEMA IF NOT EXISTS gold_schema;
```

## 4. Berechnung abgeleiteter Parameter

In der Gold-Ebene berechnen wir abgeleitete Parameter, die für spezifische Analysen relevant sind.

### 4.1 Erstellung der Tabelle für abgeleitete Parameter

```sql
-- Erstellen der Tabelle für abgeleitete Parameter
CREATE TABLE IF NOT EXISTS gold_schema.derived_parameters (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    concept_id INTEGER NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    value NUMERIC,
    unit VARCHAR(50),
    is_derived BOOLEAN DEFAULT TRUE,
    derivation_method VARCHAR(255),
    UNIQUE(subject_id, window_start, concept_id)
);

-- Erstellen von Indizes für schnellere Abfragen
CREATE INDEX IF NOT EXISTS idx_derived_parameters_subject_id ON gold_schema.derived_parameters(subject_id);
CREATE INDEX IF NOT EXISTS idx_derived_parameters_concept_id ON gold_schema.derived_parameters(concept_id);
CREATE INDEX IF NOT EXISTS idx_derived_parameters_window_start ON gold_schema.derived_parameters(window_start);
```

### 4.2 Berechnung des Mittleren Arteriellen Drucks (MAP)

```sql
-- Berechnung des Mittleren Arteriellen Drucks (MAP)
INSERT INTO gold_schema.derived_parameters (
    subject_id, window_start, concept_id, concept_name, value, unit, is_derived, derivation_method
)
WITH systolic_bp AS (
    SELECT 
        subject_id,
        window_start,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3004249  -- Systolic blood pressure
),
diastolic_bp AS (
    SELECT 
        subject_id,
        window_start,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3012888  -- Diastolic blood pressure
)
SELECT 
    sbp.subject_id,
    sbp.window_start,
    37168599 AS concept_id,  -- Mean arterial pressure
    'Mean arterial pressure' AS concept_name,
    (sbp.value + 2 * dbp.value) / 3 AS value,
    'mmHg' AS unit,
    TRUE AS is_derived,
    'MAP = (SBP + 2*DBP) / 3' AS derivation_method
FROM systolic_bp sbp
JOIN diastolic_bp dbp 
    ON sbp.subject_id = dbp.subject_id 
    AND sbp.window_start = dbp.window_start
ON CONFLICT (subject_id, window_start, concept_id) DO UPDATE
SET 
    value = EXCLUDED.value,
    unit = EXCLUDED.unit,
    is_derived = EXCLUDED.is_derived,
    derivation_method = EXCLUDED.derivation_method;
```

### 4.3 Berechnung des Schockindex

```sql
-- Berechnung des Schockindex (Herzfrequenz / Systolischer Blutdruck)
INSERT INTO gold_schema.derived_parameters (
    subject_id, window_start, concept_id, concept_name, value, unit, is_derived, derivation_method
)
WITH heart_rate AS (
    SELECT 
        subject_id,
        window_start,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3027598  -- Heart rate
),
systolic_bp AS (
    SELECT 
        subject_id,
        window_start,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3004249  -- Systolic blood pressure
)
SELECT 
    hr.subject_id,
    hr.window_start,
    4141365 AS concept_id,  -- Shock index
    'Shock index' AS concept_name,
    hr.value / sbp.value AS value,
    NULL AS unit,
    TRUE AS is_derived,
    'Shock index = Heart rate / Systolic blood pressure' AS derivation_method
FROM heart_rate hr
JOIN systolic_bp sbp 
    ON hr.subject_id = sbp.subject_id 
    AND hr.window_start = sbp.window_start
WHERE sbp.value > 0  -- Vermeidung von Division durch Null
ON CONFLICT (subject_id, window_start, concept_id) DO UPDATE
SET 
    value = EXCLUDED.value,
    unit = EXCLUDED.unit,
    is_derived = EXCLUDED.is_derived,
    derivation_method = EXCLUDED.derivation_method;
```

## 5. Zeitliche Aggregation

In der Gold-Ebene führen wir auch zeitliche Aggregationen durch, um Trends und Muster in den Daten zu erkennen.

### 5.1 Erstellung der Tabelle für zeitliche Aggregationen

```sql
-- Erstellen der Tabelle für zeitliche Aggregationen
CREATE TABLE IF NOT EXISTS gold_schema.temporal_aggregations (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    concept_id INTEGER NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    min_value NUMERIC,
    max_value NUMERIC,
    mean_value NUMERIC,
    median_value NUMERIC,
    std_dev NUMERIC,
    count INTEGER,
    aggregation_window VARCHAR(50),
    UNIQUE(subject_id, window_start, concept_id, aggregation_window)
);

-- Erstellen von Indizes für schnellere Abfragen
CREATE INDEX IF NOT EXISTS idx_temporal_aggregations_subject_id ON gold_schema.temporal_aggregations(subject_id);
CREATE INDEX IF NOT EXISTS idx_temporal_aggregations_concept_id ON gold_schema.temporal_aggregations(concept_id);
CREATE INDEX IF NOT EXISTS idx_temporal_aggregations_window_start ON gold_schema.temporal_aggregations(window_start);
```

### 5.2 Tägliche Aggregation von Vitalparametern

```sql
-- Tägliche Aggregation von Vitalparametern
INSERT INTO gold_schema.temporal_aggregations (
    subject_id, window_start, window_end, concept_id, concept_name,
    min_value, max_value, mean_value, median_value, std_dev, count, aggregation_window
)
WITH daily_windows AS (
    SELECT 
        subject_id,
        date_trunc('day', window_start) AS day_start,
        date_trunc('day', window_start) + interval '1 day' AS day_end,
        concept_id,
        concept_name
    FROM silver_schema.imputed_parameters
    GROUP BY subject_id, date_trunc('day', window_start), concept_id, concept_name
),
daily_stats AS (
    SELECT 
        dw.subject_id,
        dw.day_start,
        dw.day_end,
        dw.concept_id,
        dw.concept_name,
        MIN(ip.value) AS min_value,
        MAX(ip.value) AS max_value,
        AVG(ip.value) AS mean_value,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY ip.value) AS median_value,
        STDDEV(ip.value) AS std_dev,
        COUNT(ip.value) AS count
    FROM daily_windows dw
    JOIN silver_schema.imputed_parameters ip 
        ON dw.subject_id = ip.subject_id 
        AND dw.concept_id = ip.concept_id
        AND ip.window_start >= dw.day_start 
        AND ip.window_start < dw.day_end
    GROUP BY dw.subject_id, dw.day_start, dw.day_end, dw.concept_id, dw.concept_name
)
SELECT 
    subject_id,
    day_start AS window_start,
    day_end AS window_end,
    concept_id,
    concept_name,
    min_value,
    max_value,
    mean_value,
    median_value,
    std_dev,
    count,
    '1 day' AS aggregation_window
FROM daily_stats
ON CONFLICT (subject_id, window_start, concept_id, aggregation_window) DO UPDATE
SET 
    window_end = EXCLUDED.window_end,
    min_value = EXCLUDED.min_value,
    max_value = EXCLUDED.max_value,
    mean_value = EXCLUDED.mean_value,
    median_value = EXCLUDED.median_value,
    std_dev = EXCLUDED.std_dev,
    count = EXCLUDED.count;
```

## 6. Berechnung klinischer Scores

In der Gold-Ebene berechnen wir auch klinische Scores, die für die Bewertung des Patientenzustands relevant sind.

### 6.1 Erstellung der Tabelle für klinische Scores

```sql
-- Erstellen der Tabelle für klinische Scores
CREATE TABLE IF NOT EXISTS gold_schema.clinical_scores (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    window_start TIMESTAMP NOT NULL,
    score_id INTEGER NOT NULL,
    score_name VARCHAR(255) NOT NULL,
    score_value NUMERIC,
    score_category VARCHAR(50),
    score_components JSONB,
    UNIQUE(subject_id, window_start, score_id)
);

-- Erstellen von Indizes für schnellere Abfragen
CREATE INDEX IF NOT EXISTS idx_clinical_scores_subject_id ON gold_schema.clinical_scores(subject_id);
CREATE INDEX IF NOT EXISTS idx_clinical_scores_score_id ON gold_schema.clinical_scores(score_id);
CREATE INDEX IF NOT EXISTS idx_clinical_scores_window_start ON gold_schema.clinical_scores(window_start);
```

### 6.2 Berechnung des SOFA-Scores (Sequential Organ Failure Assessment)

```sql
-- Berechnung des SOFA-Scores
INSERT INTO gold_schema.clinical_scores (
    subject_id, window_start, score_id, score_name, score_value, score_category, score_components
)
WITH respiratory_component AS (
    -- PaO2/FiO2 ratio
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value >= 400 THEN 0
            WHEN value >= 300 THEN 1
            WHEN value >= 200 THEN 2
            WHEN value >= 100 THEN 3
            ELSE 4
        END AS score,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3050453  -- PaO2/FiO2 ratio
),
cardiovascular_component AS (
    -- Mean arterial pressure or vasopressors
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value >= 70 THEN 0
            WHEN value >= 65 THEN 1
            ELSE 2  -- Vereinfachung: Wir berücksichtigen hier keine Vasopressoren
        END AS score,
        value
    FROM gold_schema.derived_parameters
    WHERE concept_id = 37168599  -- Mean arterial pressure
),
liver_component AS (
    -- Bilirubin
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value < 1.2 THEN 0
            WHEN value < 2.0 THEN 1
            WHEN value < 6.0 THEN 2
            WHEN value < 12.0 THEN 3
            ELSE 4
        END AS score,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3024128  -- Bilirubin
),
coagulation_component AS (
    -- Platelets
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value >= 150 THEN 0
            WHEN value >= 100 THEN 1
            WHEN value >= 50 THEN 2
            WHEN value >= 20 THEN 3
            ELSE 4
        END AS score,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3007461  -- Platelets
),
renal_component AS (
    -- Creatinine or urine output
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value < 1.2 THEN 0
            WHEN value < 2.0 THEN 1
            WHEN value < 3.5 THEN 2
            WHEN value < 5.0 THEN 3
            ELSE 4
        END AS score,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3016723  -- Creatinine
),
neurological_component AS (
    -- Glasgow Coma Scale
    SELECT 
        subject_id,
        window_start,
        CASE
            WHEN value = 15 THEN 0
            WHEN value >= 13 THEN 1
            WHEN value >= 10 THEN 2
            WHEN value >= 6 THEN 3
            ELSE 4
        END AS score,
        value
    FROM silver_schema.imputed_parameters
    WHERE concept_id = 3007194  -- Glasgow coma score total
),
sofa_score AS (
    SELECT 
        rc.subject_id,
        rc.window_start,
        rc.score AS respiratory_score,
        cc.score AS cardiovascular_score,
        lc.score AS liver_score,
        coc.score AS coagulation_score,
        rec.score AS renal_score,
        nc.score AS neurological_score,
        rc.score + cc.score + lc.score + coc.score + rec.score + nc.score AS total_score,
        jsonb_build_object(
            'respiratory', jsonb_build_object('score', rc.score, 'value', rc.value),
            'cardiovascular', jsonb_build_object('score', cc.score, 'value', cc.value),
            'liver', jsonb_build_object('score', lc.score, 'value', lc.value),
            'coagulation', jsonb_build_object('score', coc.score, 'value', coc.value),
            'renal', jsonb_build_object('score', rec.score, 'value', rec.value),
            'neurological', jsonb_build_object('score', nc.score, 'value', nc.value)
        ) AS components
    FROM respiratory_component rc
    JOIN cardiovascular_component cc ON rc.subject_id = cc.subject_id AND rc.window_start = cc.window_start
    JOIN liver_component lc ON rc.subject_id = lc.subject_id AND rc.window_start = lc.window_start
    JOIN coagulation_component coc ON rc.subject_id = coc.subject_id AND rc.window_start = coc.window_start
    JOIN renal_component rec ON rc.subject_id = rec.subject_id AND rc.window_start = rec.window_start
    JOIN neurological_component nc ON rc.subject_id = nc.subject_id AND rc.window_start = nc.window_start
)
SELECT 
    subject_id,
    window_start,
    4296653 AS score_id,  -- SOFA score
    'Sequential Organ Failure Assessment Score' AS score_name,
    total_score AS score_value,
    CASE
        WHEN total_score >= 10 THEN 'High risk'
        WHEN total_score >= 6 THEN 'Medium risk'
        ELSE 'Low risk'
    END AS score_category,
    components AS score_components
FROM sofa_score
ON CONFLICT (subject_id, window_start, score_id) DO UPDATE
SET 
    score_value = EXCLUDED.score_value,
    score_category = EXCLUDED.score_category,
    score_components = EXCLUDED.score_components;
```

## 7. Erstellung eines vollständigen SQL-Skripts

Basierend auf den obigen Beispielen können Sie ein vollständiges SQL-Skript erstellen, das:

1. Das Gold-Schema erstellt
2. Die Tabelle für abgeleitete Parameter erstellt und befüllt
3. Die Tabelle für zeitliche Aggregationen erstellt und befüllt
4. Die Tabelle für klinische Scores erstellt und befüllt

Speichern Sie dieses Skript in der Datei `src/gold/create_gold_tables.sql`.

## 8. Nächste Schritte

Nachdem Sie die Gold-Ebene eingerichtet haben, können Sie die aufbereiteten Daten für spezifische Analysen verwenden. Hier sind einige mögliche nächste Schritte:

1. Visualisierung der Daten mit Tools wie Grafana, Tableau oder Power BI
2. Entwicklung von Machine-Learning-Modellen zur Vorhersage klinischer Outcomes
3. Integration der Pipeline in ein klinisches Entscheidungsunterstützungssystem
4. Erweiterung der Pipeline um weitere Parameter und Scores

Weitere Beispiele und Anwendungsfälle finden Sie im Ordner `nbs/examples/`.

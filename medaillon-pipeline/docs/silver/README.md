# Silver-Ebene: Standardisierung und Qualitätssicherung

Diese Anleitung führt Sie durch den Prozess der Standardisierung und Qualitätssicherung von klinischen Daten auf der Silver-Ebene unserer Datenaufbereitungspipeline. Besonders für anästhesiologische Daten ist dieser Schritt entscheidend, da hier heterogene Messparameter vereinheitlicht und Ausreißer identifiziert werden.

## Inhaltsverzeichnis

- [Silver-Ebene: Standardisierung und Qualitätssicherung](#silver-ebene-standardisierung-und-qualitätssicherung)
  - [Inhaltsverzeichnis](#inhaltsverzeichnis)
  - [1. Überblick](#1-überblick)
  - [2. Erstellung des Silver-Schemas](#2-erstellung-des-silver-schemas)
  - [3. Standardisierung mit OMOP-Konzepten](#3-standardisierung-mit-omop-konzepten)
    - [3.1 Erstellung der Mapping-Tabelle](#31-erstellung-der-mapping-tabelle)
    - [3.2 Erstellung der standardisierten Silver-Tabelle](#32-erstellung-der-standardisierten-silver-tabelle)
  - [4. Ausreißererkennung und -entfernung](#4-ausreißererkennung-und--entfernung)
    - [4.1 Erstellung der Tabelle für physiologische Grenzen](#41-erstellung-der-tabelle-für-physiologische-grenzen)
    - [4.2 Befüllen der standardisierten Silver-Tabelle mit Ausreißererkennung](#42-befüllen-der-standardisierten-silver-tabelle-mit-ausreißererkennung)
  - [5. Erstellung eines vollständigen SQL-Skripts](#5-erstellung-eines-vollständigen-sql-skripts)
  - [6. Nächste Schritte](#6-nächste-schritte)
    - [6.1 Anwendungsbeispiele für die Anästhesie und Intensivmedizin](#61-anwendungsbeispiele-für-die-anästhesie-und-intensivmedizin)

## 1. Überblick

Die Silver-Ebene ist der zweite Schritt in unserer Datenaufbereitungspipeline. Hier standardisieren wir die Daten aus der Bronze-Ebene durch Mapping zu OMOP-Konzepten und entfernen Ausreißer basierend auf physiologischen Grenzen. Das Ziel ist es, einen konsistenten und qualitätsgesicherten Datensatz zu erstellen, der für weitere Analysen geeignet ist, während das Long-Format beibehalten wird.

In der Anästhesie und Intensivmedizin ist dieser Schritt besonders wichtig, da Vitalparameter und Laborwerte von verschiedenen Geräten und Systemen mit unterschiedlichen Bezeichnungen und Einheiten erfasst werden. Beispielsweise kann der mittlere arterielle Druck als "MAP", "Mean Arterial Pressure", "Art. Mitteldruck" oder "ABP mean" bezeichnet werden. Durch die Standardisierung werden diese unterschiedlichen Bezeichnungen einem einheitlichen Konzept zugeordnet.


## 2. Erstellung des Silver-Schemas

Zunächst erstellen wir ein separates Schema für unsere Silver-Tabellen:

```sql
-- Erstellen des Silver-Schemas
CREATE SCHEMA IF NOT EXISTS silver_schema;
```

## 3. Standardisierung mit OMOP-Konzepten

In der Silver-Ebene standardisieren wir die Parameternamen durch Mapping zu OMOP-Konzepten, um eine einheitliche Terminologie zu gewährleisten.

### 3.1 Erstellung der Mapping-Tabelle

```sql
-- Erstellen der Mapping-Tabelle für OMOP-Konzepte
CREATE TABLE IF NOT EXISTS silver_schema.parameter_mapping (
    id SERIAL PRIMARY KEY,
    source_itemid INTEGER NOT NULL,
    source_parameter_name VARCHAR(255) NOT NULL,
    target_concept_id INTEGER NOT NULL,
    target_concept_name VARCHAR(255) NOT NULL,
    target_domain_id VARCHAR(50) NOT NULL,
    target_vocabulary_id VARCHAR(50) NOT NULL,
    UNIQUE(source_itemid)
);

-- Einfügen von Mapping-Daten
INSERT INTO silver_schema.parameter_mapping (
 source_itemid, source_parameter_name, target_concept_id,
 target_concept_name, target_domain_id, target_vocabulary_id
)
VALUES
    -- Vitalparameter
    (220050, 'Arterieller Blutdruck systolisch', 3004249, 'Systolic blood pressure', 'Measurement', 'LOINC'),
    (220179, 'Nicht-invasiver Blutdruck systolisch', 3004249, 'Systolic blood pressure', 'Measurement', 'LOINC'),
    (220180, 'Nicht-invasiver Blutdruck diastolisch', 3012888, 'Diastolic blood pressure', 'Measurement', 'LOINC'),
    (220074, 'Mittlerer arterieller Druck (MAP)', 37168599, 'Mean arterial pressure', 'Measurement', 'SNOMED'),
    (220277, 'O2 Sättigung', 4020553, 'Oxygen saturation measurement', 'Measurement', 'SNOMED'),
    (223761, 'Temperatur (°C)', 45771331, 'Temperature', 'Measurement', 'SNOMED'),
    (223762, 'Temperatur (°F)', 45771331, 'Temperature', 'Measurement', 'SNOMED'),
    (220210, 'Atemfrequenz', 4313591, 'Respiratory rate', 'Measurement', 'SNOMED'),

    -- SOFA: GCS Parameter
    (220739, 'GCS Total', 3007194, 'Glasgow Coma Scale total', 'Measurement', 'LOINC'),
    (223900, 'GCS Verbal', 3009094, 'Glasgow Coma Scale verbal response', 'Measurement', 'LOINC'),
    (223901, 'GCS Motor', 3008223, 'Glasgow Coma Scale motor response', 'Measurement', 'LOINC'),
    (224640, 'GCS Eye', 3016335, 'Glasgow Coma Scale eye opening', 'Measurement', 'LOINC'),
    -- SOFA: Beatmungsparameter
    (224688, 'Ventilator Mode', 3004921, 'Ventilator mode', 'Measurement', 'LOINC'),
    (220339, 'Ventilation Status', 4224130, 'Ventilation status', 'Measurement', 'SNOMED'),
    -- Laborwerte
    (50912, 'Kreatinin', 3016723, 'Creatinine', 'Measurement', 'LOINC'),
    (50885, 'Bilirubin total', 3024128, 'Bilirubin.total', 'Measurement', 'LOINC'),
    (50818, 'pO2 arterial', 3027801, 'Oxygen [Partial pressure] in Arterial blood', 'Measurement', 'LOINC'),
    (51301, 'Thrombozyten', 3007461, 'Platelets', 'Measurement', 'LOINC'),
    (50983, 'Natrium', 3019550, 'Sodium', 'Measurement', 'LOINC'),
    -- Beatmungsparameter
    (223835, 'Inspired O2 Fraction', 42869590, 'Oxygen/Gas total [Pure volume fraction] Inhaled gas', 'Measurement', 'LOINC'),
    -- Ausscheidungen
    (226559, 'Urinausscheidung', 3014315, 'Urine output', 'Measurement', 'LOINC'),
    (226560, 'Urinausscheidung (Foley)', 3014315, 'Urine output', 'Measurement', 'LOINC'),
    (226561, 'Urinausscheidung (Void)', 3014315, 'Urine output', 'Measurement', 'LOINC'),
    -- Medikamente
    (221906, 'Norepinephrin', 1321341, 'Norepinephrine', 'Drug', 'RxNorm'),
    (221662, 'Dopamin', 1337860, 'Dopamine', 'Drug', 'RxNorm'),
    (221668, 'Dobutamin', 1337720, 'Dobutamine', 'Drug', 'RxNorm'),
    -- SOFA: Weitere Vasopressoren
    (221289, 'Epinephrin (Adrenalin)', 1343916, 'Epinephrine', 'Drug', 'RxNorm')
    ON CONFLICT (source_itemid) DO UPDATE
    SET
        source_parameter_name = EXCLUDED.source_parameter_name,
        target_concept_id = EXCLUDED.target_concept_id,
        target_concept_name = EXCLUDED.target_concept_name,
        target_domain_id = EXCLUDED.target_domain_id,
        target_vocabulary_id = EXCLUDED.target_vocabulary_id;
```

### 3.2 Erstellung der standardisierten Silver-Tabelle

```sql
-- Erstellen der standardisierten Silver-Tabelle
CREATE TABLE IF NOT EXISTS silver_schema.standardized_parameters (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER,
    stay_id INTEGER,
    charttime TIMESTAMP NOT NULL,
    concept_id INTEGER NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    value NUMERIC,
    unit VARCHAR(50),
    is_error BOOLEAN DEFAULT FALSE,
    is_outlier BOOLEAN DEFAULT FALSE,
    source_itemid INTEGER NOT NULL,
    source_table VARCHAR(50) NOT NULL
);

-- Erstellen von Indizes für schnellere Abfragen
CREATE INDEX IF NOT EXISTS idx_standardized_parameters_subject_id ON silver_schema.standardized_parameters(subject_id);
CREATE INDEX IF NOT EXISTS idx_standardized_parameters_concept_id ON silver_schema.standardized_parameters(concept_id);
CREATE INDEX IF NOT EXISTS idx_standardized_parameters_charttime ON silver_schema.standardized_parameters(charttime);
```

## 4. Ausreißererkennung und -entfernung

In der Silver-Ebene erkennen und entfernen wir Ausreißer, um die Datenqualität zu verbessern. Dies ist besonders wichtig in der Anästhesie und Intensivmedizin, wo Messfehler oder fehlerhafte Dokumentation zu klinisch unplausiblen Werten führen können.

### 4.1 Erstellung der Tabelle für physiologische Grenzen

```sql
-- Erstellen der Tabelle für physiologische Grenzen
CREATE TABLE IF NOT EXISTS silver_schema.physiological_limits (
 id SERIAL PRIMARY KEY,
 concept_id INTEGER NOT NULL,
 concept_name VARCHAR(255) NOT NULL,
 min_value NUMERIC NOT NULL,
 max_value NUMERIC NOT NULL,
UNIQUE(concept_id)
);

-- Einfügen von physiologischen Grenzen
INSERT INTO silver_schema.physiological_limits (
 concept_id, concept_name, min_value, max_value
)
VALUES
-- Vitalparameter
 (3004249, 'Systolic blood pressure', 50, 250),           -- systolischer Blutdruck (mmHg)
 (3012888, 'Diastolic blood pressure', 20, 150),          -- diastolischer Blutdruck (mmHg)
 (37168599, 'Mean arterial pressure', 30, 180),           -- mittlerer arterieller Druck (mmHg)
 (4020553, 'Oxygen saturation measurement', 50, 100),     -- O2 Sättigung (%)
 (45771331, 'Temperature', 30, 45),                       -- Temperatur (°C)
 (4313591, 'Respiratory rate', 4, 60),                    -- Atemfrequenz (Atemzüge/min)

-- SOFA: GCS Parameter
 (3007194, 'Glasgow Coma Scale total', 3, 15),            -- GCS Gesamtpunktzahl
 (3009094, 'Glasgow Coma Scale verbal response', 1, 5),   -- GCS verbal
 (3008223, 'Glasgow Coma Scale motor response', 1, 6),    -- GCS motorisch
 (3016335, 'Glasgow Coma Scale eye opening', 1, 4),       -- GCS Augenöffnung

-- Laborwerte
 (3016723, 'Creatinine', 0.2, 15),                        -- Kreatinin (mg/dL)
 (3024128, 'Bilirubin.total', 0.1, 50),                   -- Bilirubin gesamt (mg/dL)
 (3027801, 'Oxygen [Partial pressure] in Arterial blood', 20, 500), -- pO2 arteriell (mmHg)
 (3007461, 'Platelets', 5, 1000),                         -- Thrombozyten (×10³/µL)
 (3019550, 'Sodium', 110, 170),                           -- Natrium (mmol/L)

-- Beatmungsparameter
 (42869590, 'Oxygen/Gas total [Pure volume fraction] Inhaled gas', 0.21, 1.0), -- FiO2 (21%-100%)

-- Ausscheidungen (ml pro Stunde)
 (3014315, 'Urine output', 0, 1000)                       -- Urinausscheidung (ml/h)

ON CONFLICT (concept_id) DO UPDATE
SET
 concept_name = EXCLUDED.concept_name,
 min_value = EXCLUDED.min_value,
 max_value = EXCLUDED.max_value;
```

Die physiologischen Grenzen sollten an die klinische Situation angepasst werden. Die hier angegebenen Werte sind Richtwerte und können je nach Patientenpopulation und klinischem Kontext variieren.

### 4.2 Befüllen der standardisierten Silver-Tabelle mit Ausreißererkennung

```sql
-- Befüllen der standardisierten Silver-Tabelle mit Ausreißererkennung
INSERT INTO silver_schema.standardized_parameters (
 subject_id, hadm_id, stay_id, charttime, concept_id, concept_name,
 value, unit, is_error, is_outlier, source_itemid, source_table
)
SELECT
 bp.subject_id,
 bp.hadm_id,
 bp.stay_id,
 bp.charttime,
 pm.target_concept_id AS concept_id,
 pm.target_concept_name AS concept_name,
 -- Umwandlung nur für tatsächliche Fahrenheit-Werte
 CASE
   WHEN bp.itemid = 223762 AND bp.valueuom = '°F' THEN ((bp.valuenum - 32) * 5/9)  -- Nur Fahrenheit zu Celsius umrechnen
   ELSE bp.valuenum  -- Alle anderen Werte bleiben unverändert
 END AS value,
 -- Einheitsanpassung
 CASE
   WHEN bp.itemid = 223762 AND bp.valueuom = '°F' THEN '°C'  -- Nur bei tatsächlichen Fahrenheit-Werten
   ELSE bp.valueuom
 END AS unit,
 bp.error AS is_error,
 CASE
   WHEN pl.min_value IS NOT NULL AND pl.max_value IS NOT NULL THEN
     CASE
       WHEN bp.itemid = 223762 AND bp.valueuom = '°F' THEN 
         (((bp.valuenum - 32) * 5/9) < pl.min_value OR ((bp.valuenum - 32) * 5/9) > pl.max_value)
       ELSE
         (bp.valuenum < pl.min_value OR bp.valuenum > pl.max_value)
     END
   ELSE FALSE
 END AS is_outlier,
 bp.itemid AS source_itemid,
 bp.source_table
FROM bronze_schema.clinical_parameters bp
JOIN silver_schema.parameter_mapping pm ON bp.itemid = pm.source_itemid
LEFT JOIN silver_schema.physiological_limits pl ON pm.target_concept_id = pl.concept_id
WHERE bp.valuenum IS NOT NULL;
```

## 5. Erstellung eines vollständigen SQL-Skripts

Basierend auf den obigen Beispielen können Sie ein vollständiges SQL-Skript erstellen, das:

1. Das Silver-Schema erstellt
2. Die Mapping-Tabelle für OMOP-Konzepte erstellt und befüllt
3. Die Tabelle für physiologische Grenzen erstellt und befüllt
4. Die standardisierte Silver-Tabelle erstellt und befüllt, inklusive Ausreißererkennung

Speichern Sie dieses Skript in der Datei `src/silver/create_silver_tables.sql`.

## 6. Nächste Schritte

Nachdem Sie die Silver-Ebene eingerichtet haben, können Sie zur Gold-Ebene übergehen, wo wir die standardisierten und qualitätsgesicherten Daten für spezifische Analysen aufbereiten werden. Weitere Informationen finden Sie in der Datei [../gold/README.md](../gold/README.md).

### 6.1 Anwendungsbeispiele für die Anästhesie und Intensivmedizin

Mit den standardisierten Daten aus der Silver-Ebene können Sie verschiedene anästhesiologisch relevante Analysen durchführen:

1. **Hämodynamisches Monitoring**: Analyse von Blutdruckverläufen und Herzfrequenz während operativer Eingriffe
2. **Beatmungsmanagement**: Untersuchung der Beatmungsparameter und deren Auswirkung auf den Oxygenierungsindex (PaO2/FiO2)
3. **Flüssigkeitsmanagement**: Analyse der Flüssigkeitsbilanz und deren Zusammenhang mit hämodynamischer Stabilität
4. **Medikamenteneffekte**: Untersuchung der Auswirkungen von Anästhetika und Analgetika auf Vitalparameter
5. **Risikostratifizierung**: Entwicklung von Prädiktionsmodellen für postoperative Komplikationen

Diese Analysen werden in der Gold-Ebene durch spezifische Aggregationen und Feature-Engineering weiter aufbereitet.

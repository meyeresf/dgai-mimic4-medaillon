# Medaillon-Prinzip: Flexible und qualitätsgesicherte Aufbereitung klinischer Daten

Diese Anleitung führt Sie durch den Prozess der Einrichtung und Nutzung einer Pipeline zur qualitätsgesicherten Aufbereitung klinischer Daten nach dem Medaillon-Prinzip (Bronze, Silver, Gold).

## Inhaltsverzeichnis

1. [Einrichtung der Datenbankumgebung](#1-einrichtung-der-datenbankumgebung)
2. [Überblick über das Medaillon-Prinzip](#2-überblick-über-das-medaillon-prinzip)
3. [Bronze-Ebene: Datenextraktion](#3-bronze-ebene-datenextraktion)
4. [Silver-Ebene: Standardisierung und Qualitätssicherung](#4-silver-ebene-standardisierung-und-qualitätssicherung)
5. [Gold-Ebene: Feature-Engineering](#5-gold-ebene-feature-engineering)
6. [Weiterführende Ressourcen](#6-weiterführende-ressourcen)

## 1. Einrichtung der Datenbankumgebung

### Was ist Docker?

Docker ist eine Plattform, die es ermöglicht, Anwendungen in isolierten Umgebungen (Containern) auszuführen. Diese Container enthalten alle notwendigen Abhängigkeiten und Konfigurationen, sodass die Anwendung unabhängig vom Host-System konsistent läuft. Für unsere Übung verwenden wir Docker, um eine PostgreSQL-Datenbank und pgAdmin (ein grafisches Verwaltungstool für PostgreSQL) bereitzustellen.

### Was ist MIMIC-IV?

MIMIC-IV (Medical Information Mart for Intensive Care) ist ein frei verfügbarer Datensatz, der anonymisierte Gesundheitsdaten von Patienten enthält, die zwischen 2008 und 2019 auf den Intensivstationen des Beth Israel Deaconess Medical Center in Boston aufgenommen wurden. Der Datensatz umfasst demografische Informationen, Vitalparameter, Laborwerte, Medikationen und mehr.

Für diese Übung verwenden wir den MIMIC-IV-Demo-Datensatz, der eine Teilmenge des vollständigen Datensatzes mit Daten von etwa 100 Patienten enthält.

### Einrichtungsschritte

Die detaillierte Anleitung zur Einrichtung der Datenbankumgebung finden Sie in der Datei `setup/setup_anleitung.md` mit allen notwendigen Schritten.

Nach erfolgreicher Einrichtung sollten Sie Zugriff auf:
- Eine PostgreSQL-Datenbank mit dem MIMIC-IV-Demo-Datensatz
- pgAdmin zur grafischen Verwaltung der Datenbank

## 2. Überblick über das Medaillon-Prinzip

Das Medaillon-Prinzip ist ein Ansatz zur stufenweisen Veredelung von Daten, der drei Hauptebenen umfasst:

### Bronze-Ebene
- **Ziel**: Rohdatenextraktion und -strukturierung mit minimaler Transformation
- **Fokus**: Vollständigkeit und Nachvollziehbarkeit
- **Typische Operationen**: Extraktion aus Quelldatenbanken, Vereinheitlichung des Formats

### Silver-Ebene
- **Ziel**: Standardisierung, Qualitätssicherung und Imputation fehlender Werte
- **Fokus**: Konsistenz und Qualität
- **Typische Operationen**: Standardisierung von Parameternamen, Ausreißererkennung, Imputation

### Gold-Ebene
- **Ziel**: Feature-Engineering, Berechnung klinischer Scores und Optimierung für Analysen
- **Fokus**: Analysebereitschaft
- **Typische Operationen**: Berechnung abgeleiteter Parameter, Aggregation, Vorbereitung für ML-Modelle

## 3. Bronze-Ebene: Datenextraktion

Die Bronze-Ebene ist der erste Schritt in unserer Pipeline. Hier extrahieren wir die Rohdaten aus der MIMIC-IV-Datenbank und strukturieren sie in einem einheitlichen Format.

### Konfiguration der Datenbankverbindung

Die Konfiguration für die Datenbankverbindung befindet sich in der Datei `config/bronze/database.yaml`:

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: mimic4
  schema_output: bronze_schema
```

### Erste SQL-Abfragen

Lassen Sie uns zunächst einige einfache SQL-Abfragen ausführen, um uns mit der Datenbankstruktur vertraut zu machen:

```sql
-- Anzahl der Patienten in der Datenbank
SELECT COUNT(*) FROM mimiciv_hosp.patients;

-- Die ersten 10 Patienten mit demografischen Informationen
SELECT subject_id, gender, anchor_age, anchor_year
FROM mimiciv_hosp.patients
LIMIT 10;
```

### Format der Bronze-Tabelle

Unsere Bronze-Tabelle hat folgendes Format:

```sql
CREATE TABLE bronze_schema.clinical_parameters (
    id SERIAL PRIMARY KEY,
    subject_id INTEGER NOT NULL,
    hadm_id INTEGER,
    stay_id INTEGER,
    charttime TIMESTAMP NOT NULL,
    storetime TIMESTAMP,
    itemid INTEGER NOT NULL,
    parameter_name VARCHAR(255) NOT NULL,
    value NUMERIC,
    valuenum NUMERIC,
    valueuom VARCHAR(50),
    warning BOOLEAN DEFAULT FALSE,
    error BOOLEAN DEFAULT FALSE,
    source_table VARCHAR(50) NOT NULL
);
```

Dieses Format ermöglicht es uns, Daten aus verschiedenen Quelltabellen in einer einheitlichen Struktur zu speichern.

### Extraktion aus verschiedenen Quelltabellen

Für verschiedene Quelltabellen benötigen wir unterschiedliche SQL-Abfragen:

#### Beispiel: Extraktion aus chartevents

```sql
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
    c.error,
    'chartevents' AS source_table,
    c.charttime AS source_id
FROM mimiciv_icu.chartevents c
JOIN mimiciv_icu.d_items d ON c.itemid = d.itemid
WHERE c.itemid IN (220045, 220050, 220179, 220180)
LIMIT 100;
```

#### Beispiel: Extraktion aus labevents

```sql
SELECT 
    l.subject_id,
    l.hadm_id,
    NULL AS stay_id,
    l.charttime,
    l.storetime,
    l.itemid,
    d.label AS parameter_name,
    NULL AS value,
    l.valuenum,
    l.valueuom,
    FALSE AS warning,
    (l.flag = 'abnormal') AS error,
    'labevents' AS source_table,
    l.labevent_id AS source_id
FROM mimiciv_hosp.labevents l
JOIN mimiciv_hosp.d_labitems d ON l.itemid = d.itemid
WHERE l.itemid IN (50912, 50971, 50983)
LIMIT 100;
```

Wie Sie sehen, müssen wir für jede Quelltabelle eine spezifische Abfrage erstellen, da die Spaltenstruktur und die Bedeutung der Spalten zwischen den Tabellen variieren können.

### Potenzial für einen QueryBuilder

Die manuelle Erstellung dieser Abfragen kann zeitaufwändig und fehleranfällig sein, insbesondere wenn wir viele verschiedene Parameter aus verschiedenen Tabellen extrahieren möchten. Ein QueryBuilder könnte diesen Prozess automatisieren, indem er anhand der Metadaten in den Tabellen `d_items` und `d_labitems` automatisch die passenden Abfragen generiert.

Für diese Übung werden wir jedoch zunächst manuell vorgehen, um ein besseres Verständnis für die Datenstruktur zu entwickeln.

### Erstellung der Bronze-Tabelle

Eine detaillierte Anleitung zur Erstellung der Bronze-Tabelle finden Sie in der Datei [docs/bronze/README.md](docs/bronze/README.md). Dort erfahren Sie, wie Sie:

1. Das Bronze-Schema erstellen
2. Die Bronze-Tabelle erstellen
3. Daten aus verschiedenen Quelltabellen extrahieren und in die Bronze-Tabelle einfügen
4. Die Abfragen mit UNION ALL kombinieren, um Daten aus mehreren Tabellen zu extrahieren

## 4. Silver-Ebene: Standardisierung und Qualitätssicherung

Nach der Extraktion der Rohdaten in die Bronze-Ebene folgt die Standardisierung und Qualitätssicherung in der Silver-Ebene. Weitere Informationen finden Sie in der Datei [docs/silver/README.md](docs/silver/README.md).

## 5. Gold-Ebene: Feature-Engineering

Die Gold-Ebene ist die letzte Stufe unserer Pipeline, in der wir die standardisierten Daten für spezifische Analysen aufbereiten. Weitere Informationen finden Sie in der Datei [docs/gold/README.md](docs/gold/README.md).

## 6. Weiterführende Ressourcen

- [MIMIC-IV Dokumentation](https://mimic.mit.edu/docs/iv/)
- [PostgreSQL Dokumentation](https://www.postgresql.org/docs/)
- [OMOP Common Data Model](https://ohdsi.github.io/CommonDataModel/)
- [The Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/)

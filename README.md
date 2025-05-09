# Medaillon-Pipeline für SOFA-Score Berechnung

Dieses Repository enthält eine Beispiel-Pipeline zur Demonstration des Medaillon-Prinzips in der klinischen Datenverarbeitung für ein Seminar für Anästhesisten.

## Überblick

Die Pipeline verarbeitet den MIMIC-IV Demo-Datensatz und durchläuft folgende Stufen:

1. **Bronze**: Extraktion relevanter Daten für den SOFA-Score aus dem MIMIC-IV Datensatz
2. **Silber**: Standardisierung der Daten und Mapping auf OMOP Concept IDs (Long-Format)
3. **Gold**: Berechnung des SOFA-Scores mit verschiedenen Aggregations- und Imputationsmethoden
4. **Analyse**: Vergleich der unterschiedlichen Berechnungsmethoden

## Projektstruktur

```
medaillon-pipeline/
├── config/             # Konfigurationsdateien
│   ├── bronze/         # Konfiguration für Bronze-Layer
│   ├── silver/         # Konfiguration für Silber-Layer
│   └── gold/           # Konfiguration für Gold-Layer
├── data/               # Datenspeicher (nicht im Repository)
│   ├── bronze/         # Bronze-Tabellen
│   ├── silver/         # Silber-Tabellen
│   └── gold/           # Gold-Tabellen
├── docs/               # Dokumentation
│   ├── bronze/         # Dokumentation für Bronze-Layer
│   ├── silver/         # Dokumentation für Silber-Layer
│   └── gold/           # Dokumentation für Gold-Layer
├── nbs/                # Jupyter Notebooks für Analysen
├── src/                # Quellcode
│   ├── bronze/         # Code für Bronze-Layer
│   ├── silver/         # Code für Silber-Layer
│   └── gold/           # Code für Gold-Layer
└── README.md           # Projektbeschreibung
```

## SOFA-Score

Der SOFA-Score (Sequential Organ Failure Assessment) wird verwendet, um das Ausmaß der Organdysfunktion bei kritisch kranken Patienten zu bewerten. Er umfasst sechs verschiedene Organsysteme:

1. **Atmung**: PaO2/FiO2-Verhältnis
2. **Koagulation**: Thrombozytenzahl
3. **Leber**: Bilirubin
4. **Kardiovaskulär**: Mittlerer arterieller Druck (MAP) und Vasopressoren
5. **ZNS**: Glasgow Coma Scale (GCS)
6. **Niere**: Kreatinin und Urinausscheidung

Jedes Organsystem wird mit 0 (normal) bis 4 (schwere Dysfunktion) Punkten bewertet. Der Gesamtscore reicht von 0 bis 24 Punkten.

## Medaillon-Prinzip

Das Medaillon-Prinzip ist ein Ansatz zur strukturierten Datenverarbeitung in drei Ebenen:

1. **Bronze**: Rohdaten werden extrahiert und in einem einheitlichen Format gespeichert
2. **Silber**: Daten werden standardisiert, bereinigt und validiert
3. **Gold**: Daten werden für spezifische Anwendungsfälle aufbereitet

Dieses Projekt demonstriert, wie das Medaillon-Prinzip auf klinische Daten angewendet werden kann, um den SOFA-Score zu berechnen.

## Installation und Verwendung

```bash
# Repository klonen
git clone https://github.com/username/medaillon-pipeline.git
cd medaillon-pipeline

# Virtuelle Umgebung erstellen und aktivieren
python -m venv venv
source venv/bin/activate  # Linux/Mac
# oder
# venv\Scripts\activate  # Windows

# Abhängigkeiten installieren
pip install -r requirements.txt

# Jupyter Notebook starten
jupyter notebook
```

Öffnen Sie die Notebooks im Verzeichnis `nbs/` für Beispiele zur Verwendung der Pipeline.

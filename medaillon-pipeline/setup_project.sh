#!/bin/bash

# Erstellen der Verzeichnisstruktur für das Medaillon-Pipeline-Projekt

# Hauptverzeichnisse
mkdir -p config/{bronze,silver,gold}
mkdir -p data/{bronze,silver,gold}
mkdir -p docs/{bronze,silver,gold}
mkdir -p nbs
mkdir -p src/{bronze,silver,gold,utils}

# Leere .gitkeep-Dateien in leeren Verzeichnissen erstellen
touch data/{bronze,silver,gold}/.gitkeep

echo "Verzeichnisstruktur für das Medaillon-Pipeline-Projekt wurde erstellt."
echo "Führen Sie 'python -m venv venv' aus, um eine virtuelle Umgebung zu erstellen."

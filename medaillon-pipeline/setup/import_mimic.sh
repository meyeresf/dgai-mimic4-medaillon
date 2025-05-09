#!/bin/bash
# Skript zum Importieren des MIMIC-IV-Datensatzes in PostgreSQL

# Konfiguration
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"
POSTGRES_DB="mimic4"
MIMIC_DATA_DIR="./mimic-iv"
MIMIC_CODE_DIR="./mimic-code"

# Umgebungsvariablen setzen
export PGPASSWORD=$POSTGRES_PASSWORD

# Überprüfen, ob das MIMIC-Code-Repository vorhanden ist
if [ ! -d "$MIMIC_CODE_DIR" ]; then
    echo "MIMIC-Code-Repository wird geklont..."
    git clone https://github.com/MIT-LCP/mimic-code.git $MIMIC_CODE_DIR
fi

# Überprüfen, ob das MIMIC-IV-Datenverzeichnis existiert
if [ ! -d "$MIMIC_DATA_DIR" ]; then
    echo "Erstelle MIMIC-IV-Datenverzeichnis..."
    mkdir -p $MIMIC_DATA_DIR
    echo "Bitte laden Sie die MIMIC-IV-CSV-Dateien in das Verzeichnis $MIMIC_DATA_DIR herunter."
    echo "Anleitung: https://physionet.org/content/mimiciv/2.0/"
    exit 1
fi

# Überprüfen, ob die Datenbank existiert, sonst erstellen
echo "Überprüfe Datenbankverbindung..."
if ! psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -lqt | cut -d \| -f 1 | grep -qw $POSTGRES_DB; then
    echo "Erstelle Datenbank $POSTGRES_DB..."
    psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -c "CREATE DATABASE $POSTGRES_DB;"
fi

# Wechseln zum MIMIC-Code-Verzeichnis
cd $MIMIC_CODE_DIR/mimic-iv/buildmimic/postgres

# Erstellen der Tabellen
echo "Erstelle MIMIC-IV-Tabellen..."
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f create_mimic_iv_tables.sql

# Importieren der CSV-Dateien
echo "Importiere MIMIC-IV-Daten..."
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -v mimic_data_dir="$MIMIC_DATA_DIR" -f load_csv.sql

# Erstellen der Indizes
echo "Erstelle Indizes für bessere Leistung..."
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f index.sql

echo "MIMIC-IV-Import abgeschlossen!"

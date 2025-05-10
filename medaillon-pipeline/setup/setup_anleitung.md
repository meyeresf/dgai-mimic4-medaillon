# Setup-Anleitung für die Medaillon-Pipeline

Diese Anleitung führt Sie durch die Einrichtung der Umgebung für die Medaillon-Pipeline mit dem MIMIC-IV-Datensatz.

## Voraussetzungen

- Docker und Docker Compose
- Zugang zum MIMIC-IV-Datensatz (PhysioNet-Zugang erforderlich)

## Schnellstart

Folgen Sie diesen Schritten, um die Umgebung einzurichten:

```bash
# 1. Docker-Container starten
cd setup
docker-compose up -d

# 2. Datenbank erstellen
createdb -h localhost -U postgres mimic4

# 3. Datenbankschema erstellen
cd ../mimic-code/mimic-iv/buildmimic/postgres
psql -h localhost -U postgres -d mimic4 -f create.sql

# 4. Daten importieren (mit komprimierten Dateien)
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -v mimic_data_dir=../../../medaillon-pipeline/data/mimic-iv-clinical-database-demo-2.2 -f load_gz.sql

# 5. Constraints hinzufügen
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -f constraint.sql

# 6. Indizes erstellen
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -f index.sql

# 7. Überprüfen der Datenbankstruktur
psql -h localhost -U postgres -d mimic4 -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'mimic%';"
psql -h localhost -U postgres -d mimic4 -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
```

## Detaillierte Anleitung

### 1. Docker-Umgebung einrichten

Im Verzeichnis `setup` befindet sich eine `docker-compose.yml`-Datei, die Container für PostgreSQL und pgAdmin konfiguriert:

```bash
cd setup
docker-compose up -d

# Sie sollten in dem Terminal folgendes sehen:
[+] Running 2/2
 ✔ Container mimic4-pgadmin Started
 ✔ Container mimic4-postgres  Started  

# Mit dem Befehl können Sie überprüfen, ob die container laufen:
docker ps -a

# Die beiden Container mimic4-pgadmin und mimic4-postgres sollten in der Liste erscheinen und der Status zeigen, dass die Container nicht Exited sind, sonder laufen
```

Nachdem die Anwendungen laufen, können Sie über pgAdmin (im Container mimic4-pgadmin) auf die Datenbank (im Container mimic4-postgres) zugreifen.

- PostgreSQL-Server auf Port 5432 (mimic4-postgres)
- pgAdmin auf Port 5050 (Zugriff über http://localhost:5050)

Besuchen Sie http://localhost:5050 (Anwendung pgAdmin in dem Container auf ihrem Gerät).
Melden Sie sich an:
      PGADMIN_DEFAULT_EMAIL: admin@example.com
      PGADMIN_DEFAULT_PASSWORD: admin

Hat alles funktioniert?

### 2. MIMIC-IV-Daten herunterladen

1. Besuchen Sie die [PhysioNet-Website](https://physionet.org/content/mimic-iv-demo/2.2/) und melden Sie sich an.
2. Laden Sie den MIMIC-IV-Demo-Datensatz herunter.
3. Entpacken Sie die Dateien in das Verzeichnis `data/mimic-iv-clinical-database-demo-2.2/`.

### 3. Datenbank erstellen und Daten importieren

Wir verwenden die Skripte aus dem mimic-code Repository:

```bash
# Datenbank erstellen (in dem container mimic4-postgres wird in Postgres eine Datenbank angelegt)
createdb -h localhost -U postgres mimic4

# Datenbankschema erstellen (Abschnitt in der Datenbank)
cd mimic-code/mimic-iv/buildmimic/postgres
psql -h localhost -U postgres -d mimic4 -f create.sql

# Daten importieren (für komprimierte .gz Dateien)
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -v mimic_data_dir=/Users/me/Desktop/Code/Charité/IMI/hu-medical-data-science/DGAI/UE/medaillon-pipeline/data/mimic-iv-clinical-database-demo-2.2 -f load_gz.sql

# Alternativ für unkomprimierte Dateien
# psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -v mimic_data_dir=../../../medaillon-pipeline/data/mimic-iv-clinical-database-demo-2.2 -f load.sql

# Constraints hinzufügen (Bedingungen, die sicherstellen, dass keine Duplikate etc. vorhanden sind)
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -f constraint.sql

# Indizes erstellen (steigert die Performance der Datenbank)
psql -h localhost -U postgres -d mimic4 -v ON_ERROR_STOP=1 -f index.sql
```

### 4. Zugriff auf die Datenbank über pgAdmin

1. Öffnen Sie einen Webbrowser und navigieren Sie zu `http://localhost:5050`
2. Melden Sie sich mit den folgenden Anmeldedaten an:
   - E-Mail: `admin@example.com`
   - Passwort: `admin`

3. Fügen Sie einen neuen Server hinzu:
   - Rechtsklick auf "Servers" → "Create" → "Server..."
   - Auf der Registerkarte "General":
     - Name: `MIMIC4`
   - Auf der Registerkarte "Connection":
     - Host: `postgres` (der Name des Services in der Docker-Compose-Datei)
     - Port: `5432`
     - Maintenance database: `mimic4`
     - Username: `postgres`
     - Password: `postgres`
   - Klicken Sie auf "Save"

4. Überprüfen Sie zunächst die verfügbaren Schemas und Tabellen:
   ```sql
   -- Verfügbare Schemas anzeigen
   SELECT schema_name 
   FROM information_schema.schemata 
   WHERE schema_name LIKE 'mimic%';
   ```
   
   Bei der MIMIC-IV-Demo-Version werden die Tabellen in den Schemas `mimiciv_hosp`, `mimiciv_icu` und `mimiciv_derived` erstellt. Sie können die Daten wie folgt abfragen:
   ```sql
   -- Beispiel für Patiententabelle
   SELECT COUNT(*) FROM mimiciv_hosp.patients;
   
   -- Die ersten 10 Patienten
   SELECT * FROM mimiciv_hosp.patients LIMIT 10;
   
   -- Beispiel für Aufnahmen
   SELECT COUNT(*) FROM mimiciv_hosp.admissions;
   
   -- Beispiel für ICU-Aufenthalte
   SELECT COUNT(*) FROM mimiciv_icu.icustays;
   ```

### Verwendung von pgAdmin für SQL-Abfragen

pgAdmin bietet eine benutzerfreundliche Oberfläche zum Ausführen von SQL-Abfragen:

1. **Navigieren zur Abfrage-Oberfläche**:
   - Erweitern Sie im linken Navigationsbaum: Servers > MIMIC4 > Databases > mimic4 > Schemas > public > Tables
   - Sie sehen nun alle Tabellen der MIMIC-IV-Datenbank

2. **Erstellen einer neuen Abfrage**:
   - Klicken Sie auf das Symbol "Query Tool" in der oberen Symbolleiste (oder drücken Sie Alt+Shift+Q)
   - Alternativ: Rechtsklick auf eine Tabelle > "Query Tool"

3. **Schreiben und Ausführen einer Abfrage**:
   - Geben Sie Ihre SQL-Abfrage im Abfrage-Editor ein
   - Klicken Sie auf den "Execute/Refresh"-Button (grüner Pfeil) oder drücken Sie F5
   - Das Ergebnis wird im unteren Bereich angezeigt

4. **Beispielabfragen zum Testen**:
   ```sql
   -- Überblick über die verfügbaren Schemas
   SELECT schema_name 
   FROM information_schema.schemata
   WHERE schema_name LIKE 'mimic%';
   
   -- Überblick über alle Tabellen in einem Schema
   SELECT table_name 
   FROM information_schema.tables 
   WHERE table_schema = 'mimiciv_hosp'
   ORDER BY table_name;
   
   -- Anzahl der Patienten
   SELECT COUNT(*) FROM mimiciv_hosp.patients;
   
   -- Die ersten 10 Patienten
   SELECT * FROM mimiciv_hosp.patients LIMIT 10;
   
   -- Anzahl der Aufnahmen pro Patient
   SELECT subject_id, COUNT(*) as num_admissions 
   FROM mimiciv_hosp.admissions 
   GROUP BY subject_id 
   ORDER BY num_admissions DESC;
   
   -- Verbindung von Patienten und Aufnahmen
   SELECT p.subject_id, p.gender, p.anchor_age, a.hadm_id, a.admittime, a.dischtime
   FROM mimiciv_hosp.patients p
   JOIN mimiciv_hosp.admissions a ON p.subject_id = a.subject_id
   LIMIT 10;
   ```

5. **Speichern von Abfragen**:
   - Klicken Sie auf das Speichern-Symbol in der Symbolleiste
   - Geben Sie einen Namen für die Abfrage ein

## 5. Fehlerbehebung

### Häufige Probleme

1. **Datenbankverbindungsprobleme**:
   ```bash
   # Testen der Verbindung
   psql -h localhost -U postgres -c "SELECT 1;"
   ```

2. **Probleme beim Datenimport**:
   - Stellen Sie sicher, dass der Pfad zu den Daten korrekt ist
   - Überprüfen Sie, ob die Dateien im richtigen Format vorliegen (.csv oder .csv.gz)
   - Verwenden Sie `load.sql` für unkomprimierte oder `load_gz.sql` für komprimierte Dateien

3. **Docker-Probleme**:
   ```bash
   # Container-Status prüfen
   docker ps
   
   # Container-Logs anzeigen
   docker logs mimic4-postgres
   ```

## 6. Weitere Ressourcen

- [MIMIC-IV Dokumentation](https://mimic.mit.edu/docs/iv/)
- [MIMIC-Code Repository](https://github.com/MIT-LCP/mimic-code)
- [PostgreSQL Dokumentation](https://www.postgresql.org/docs/)



## Python-Umgebung für die Gold-Ebene (optional)

Dieser Teil ist nicht erforderlich. Stoppen Sie hier.

Für die Gold-Ebene wird Python verwendet. Diesen Schritt können Sie auch zu einem späteren Zeitpunkt durchführen, wenn Sie mit der Gold-Ebene arbeiten möchten.

```bash
# Virtuelle Umgebung erstellen
python -m venv venv

# Unter Windows aktivieren
venv\Scripts\activate

# Unter macOS/Linux aktivieren
source venv/bin/activate

# Abhängigkeiten installieren
pip install -r requirements.txt

# Konfiguration anpassen
nano config/gold/database.yaml
```

## Abschluss des Setups

Herzlichen Glückwunsch! Sie haben das Setup der Datenbankumgebung für die Medaillon-Pipeline erfolgreich abgeschlossen. Die MIMIC-IV-Daten sind nun in Ihrer PostgreSQL-Datenbank importiert und können über pgAdmin abgefragt werden.

Die weiteren Schritte zur Arbeit mit der Medaillon-Pipeline (Bronze-, Silver- und Gold-Ebene) werden in separaten Anleitungen behandelt.

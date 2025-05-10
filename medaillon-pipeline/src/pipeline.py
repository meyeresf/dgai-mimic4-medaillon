import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yaml
import os
from sqlalchemy import text
from .database import DatabaseConnection


class DataPipeline:
    """
    Klasse zur Implementierung der Datenaufbereitungspipeline für die Gold-Ebene.
    """
    
    def __init__(self, config_path=None, db_connection=None):
        """
        Initialisiert die Pipeline mit den Konfigurationsparametern.
        
        Args:
            config_path (str, optional): Pfad zur Konfigurationsdatei.
                                         Wenn None, wird die Standardkonfiguration verwendet.
            db_connection (DatabaseConnection, optional): Datenbankverbindungsobjekt.
                                                         Wenn None, wird eine neue Verbindung erstellt.
        """
        if config_path is None:
            # Standardpfad zur Konfigurationsdatei
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(base_dir, 'config', 'gold', 'pipeline.yaml')
        
        # Konfiguration laden
        self.config = self._load_config(config_path)
        
        # Datenbankverbindung
        self.db = db_connection if db_connection else DatabaseConnection()
    
    def _load_config(self, config_path):
        """
        Lädt die Konfiguration aus einer YAML-Datei.
        
        Args:
            config_path (str): Pfad zur Konfigurationsdatei.
            
        Returns:
            dict: Konfigurationsparameter.
        """
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def load_data(self, table=None, schema=None, query=None):
        """
        Lädt Daten aus der Datenbank.
        
        Args:
            table (str, optional): Name der Tabelle. Wenn None, wird die Tabelle aus der Konfiguration verwendet.
            schema (str, optional): Name des Schemas. Wenn None, wird das Eingabeschema aus der Konfiguration verwendet.
            query (str, optional): Benutzerdefinierte SQL-Abfrage. Wenn angegeben, werden table und schema ignoriert.
            
        Returns:
            pandas.DataFrame: Geladene Daten.
        """
        if query:
            return self.db.execute_query(query)
        
        if table is None:
            table = self.config.get('input_table', 'standardized_parameters')
        
        if schema is None:
            schema = self.db.get_input_schema()
        
        query = f"SELECT * FROM {schema}.{table}"
        return self.db.execute_query(query)
    
    def pivot_data(self, data, index_cols=None, value_col=None, pivot_col=None):
        """
        Wandelt Daten vom Long-Format ins Wide-Format um.
        
        Args:
            data (pandas.DataFrame): Daten im Long-Format.
            index_cols (list, optional): Spalten für den Index. Wenn None, werden die Spalten aus der Konfiguration verwendet.
            value_col (str, optional): Spalte mit den Werten. Wenn None, wird die Spalte aus der Konfiguration verwendet.
            pivot_col (str, optional): Spalte für die Pivot-Operation. Wenn None, wird die Spalte aus der Konfiguration verwendet.
            
        Returns:
            pandas.DataFrame: Daten im Wide-Format.
        """
        if index_cols is None:
            index_cols = self.config.get('pivot', {}).get('index_cols', ['subject_id', 'charttime'])
        
        if value_col is None:
            value_col = self.config.get('pivot', {}).get('value_col', 'value')
        
        if pivot_col is None:
            pivot_col = self.config.get('pivot', {}).get('pivot_col', 'concept_name')
        
        # Pivot-Operation durchführen
        pivot_data = data.pivot_table(
            index=index_cols,
            columns=pivot_col,
            values=value_col,
            aggfunc='mean'  # Standardaggregation: Mittelwert
        ).reset_index()
        
        return pivot_data
    
    def aggregate_data(self, data, time_window=None, agg_method=None):
        """
        Aggregiert Daten in Zeitfenstern.
        
        Args:
            data (pandas.DataFrame): Daten, die aggregiert werden sollen.
            time_window (str, optional): Größe des Zeitfensters (z.B. '1H', '30min'). 
                                         Wenn None, wird das Zeitfenster aus der Konfiguration verwendet.
            agg_method (str, optional): Aggregationsmethode (z.B. 'mean', 'median', 'max'). 
                                        Wenn None, wird die Methode aus der Konfiguration verwendet.
            
        Returns:
            pandas.DataFrame: Aggregierte Daten.
        """
        if time_window is None:
            time_window = self.config.get('aggregation', {}).get('time_window', '1H')
        
        if agg_method is None:
            agg_method = self.config.get('aggregation', {}).get('method', 'mean')
        
        # Kopie der Daten erstellen
        result = data.copy()
        
        # Zeitstempelspalte identifizieren
        time_col = None
        for col in result.columns:
            if pd.api.types.is_datetime64_any_dtype(result[col]):
                time_col = col
                break
        
        if time_col is None:
            for col in ['charttime', 'time_window', 'timestamp']:
                if col in result.columns:
                    result[col] = pd.to_datetime(result[col])
                    time_col = col
                    break
        
        if time_col is None:
            raise ValueError("Keine Zeitstempelspalte gefunden.")
        
        # Zeitfenster erstellen
        result['time_window'] = result[time_col].dt.floor(time_window)
        
        # Spalten für die Gruppierung identifizieren
        id_cols = [col for col in result.columns if 'id' in col.lower() and col != 'concept_id']
        if not id_cols:
            id_cols = ['subject_id'] if 'subject_id' in result.columns else []
        
        # Gruppieren und aggregieren
        group_cols = id_cols + ['time_window']
        
        # Numerische Spalten identifizieren
        numeric_cols = result.select_dtypes(include=['number']).columns.tolist()
        numeric_cols = [col for col in numeric_cols if col not in group_cols]
        
        # Aggregationsfunktion auswählen
        if agg_method == 'mean':
            agg_func = np.mean
        elif agg_method == 'median':
            agg_func = np.median
        elif agg_method == 'max':
            agg_func = np.max
        elif agg_method == 'min':
            agg_func = np.min
        else:
            agg_func = np.mean  # Standardmäßig Mittelwert verwenden
        
        # Aggregation durchführen
        aggregated = result.groupby(group_cols)[numeric_cols].agg(agg_func).reset_index()
        
        return aggregated
    
    def impute_missing_values(self, data, method=None, group_by=None):
        """
        Imputiert fehlende Werte in den Daten.
        
        Args:
            data (pandas.DataFrame): Daten mit fehlenden Werten.
            method (str, optional): Imputationsmethode ('locf', 'nocb', 'mean', 'median', 'zero', 'constant').
                                    Wenn None, wird die Methode aus der Konfiguration verwendet.
            group_by (list, optional): Spalten für die Gruppierung bei der Imputation.
                                      Wenn None, werden die Spalten aus der Konfiguration verwendet.
            
        Returns:
            pandas.DataFrame: Daten mit imputierten Werten.
        """
        if method is None:
            method = self.config.get('imputation', {}).get('method', 'locf')
        
        if group_by is None:
            group_by = self.config.get('imputation', {}).get('group_by', ['subject_id'])
            group_by = [col for col in group_by if col in data.columns]
        
        # Kopie der Daten erstellen
        result = data.copy()
        
        # Zeitstempelspalte identifizieren
        time_cols = [col for col in result.columns if pd.api.types.is_datetime64_any_dtype(result[col])]
        if time_cols:
            time_col = time_cols[0]
            # Nach Zeit sortieren
            result = result.sort_values(by=group_by + [time_col])
        
        # Numerische Spalten identifizieren
        numeric_cols = result.select_dtypes(include=['number']).columns.tolist()
        numeric_cols = [col for col in numeric_cols if col not in group_by and col not in time_cols]
        
        # Imputation durchführen
        if method == 'locf':  # Last Observation Carried Forward
            if group_by:
                result[numeric_cols] = result.groupby(group_by)[numeric_cols].ffill()
            else:
                result[numeric_cols] = result[numeric_cols].ffill()
        
        elif method == 'nocb':  # Next Observation Carried Backward
            if group_by:
                result[numeric_cols] = result.groupby(group_by)[numeric_cols].bfill()
            else:
                result[numeric_cols] = result[numeric_cols].bfill()
        
        elif method == 'mean':  # Mittelwert
            if group_by:
                for col in numeric_cols:
                    means = result.groupby(group_by)[col].transform('mean')
                    result[col] = result[col].fillna(means)
            else:
                for col in numeric_cols:
                    result[col] = result[col].fillna(result[col].mean())
        
        elif method == 'median':  # Median
            if group_by:
                for col in numeric_cols:
                    medians = result.groupby(group_by)[col].transform('median')
                    result[col] = result[col].fillna(medians)
            else:
                for col in numeric_cols:
                    result[col] = result[col].fillna(result[col].median())
        
        elif method == 'zero':  # Nullen
            result[numeric_cols] = result[numeric_cols].fillna(0)
        
        elif method == 'constant':  # Konstanter Wert
            constant_value = self.config.get('imputation', {}).get('constant_value', 0)
            result[numeric_cols] = result[numeric_cols].fillna(constant_value)
        
        elif method == 'last':  # Letzter verfügbarer Wert
            # Für jeden Patienten den letzten verfügbaren Wert für jede Spalte finden
            if group_by:
                # Sortieren nach Zeit (absteigend) innerhalb jeder Gruppe
                time_cols = [col for col in result.columns if pd.api.types.is_datetime64_any_dtype(result[col])]
                if time_cols:
                    time_col = time_cols[0]
                    # Für jede Gruppe separat verarbeiten
                    for _, group_df in result.groupby(group_by):
                        # Letzte nicht-NaN Werte für jede Spalte finden
                        last_values = {}
                        for col in numeric_cols:
                            # Nicht-NaN Werte in zeitlich absteigender Reihenfolge
                            valid_values = group_df.sort_values(by=time_col, ascending=False)[[time_col, col]].dropna()
                            if not valid_values.empty:
                                # Für jede eindeutige Zeit den letzten Wert nehmen
                                last_values[col] = valid_values.drop_duplicates(subset=[time_col]).set_index(time_col)[col]
                        
                        # Für jede Zeile in der Gruppe
                        for idx, row in group_df.iterrows():
                            for col in numeric_cols:
                                if pd.isna(result.at[idx, col]) and col in last_values:
                                    # Finde den letzten Wert vor diesem Zeitpunkt
                                    last_times = last_values[col].index
                                    valid_times = last_times[last_times <= row[time_col]]
                                    if not valid_times.empty:
                                        result.at[idx, col] = last_values[col][valid_times[0]]
            else:
                # Ohne Gruppierung einfach den letzten nicht-NaN Wert verwenden
                for col in numeric_cols:
                    last_valid = None
                    for idx in result.index:
                        if not pd.isna(result.at[idx, col]):
                            last_valid = result.at[idx, col]
                        elif last_valid is not None:
                            result.at[idx, col] = last_valid
        
        elif method == 'last':  # Letzter verfügbarer Wert
            # Für jeden Patienten den letzten verfügbaren Wert für jede Spalte finden
            if group_by:
                # Sortieren nach Zeit (absteigend) innerhalb jeder Gruppe
                time_cols = [col for col in result.columns if pd.api.types.is_datetime64_any_dtype(result[col])]
                if time_cols:
                    time_col = time_cols[0]
                    # Für jede Gruppe separat verarbeiten
                    for _, group_df in result.groupby(group_by):
                        # Letzte nicht-NaN Werte für jede Spalte finden
                        last_values = {}
                        for col in numeric_cols:
                            # Nicht-NaN Werte in zeitlich absteigender Reihenfolge
                            valid_values = group_df.sort_values(by=time_col, ascending=False)[[time_col, col]].dropna()
                            if not valid_values.empty:
                                # Für jede eindeutige Zeit den letzten Wert nehmen
                                last_values[col] = valid_values.drop_duplicates(subset=[time_col]).set_index(time_col)[col]
                        
                        # Für jede Zeile in der Gruppe
                        for idx, row in group_df.iterrows():
                            for col in numeric_cols:
                                if pd.isna(result.at[idx, col]) and col in last_values:
                                    # Finde den letzten Wert vor diesem Zeitpunkt
                                    last_times = last_values[col].index
                                    valid_times = last_times[last_times <= row[time_col]]
                                    if not valid_times.empty:
                                        result.at[idx, col] = last_values[col][valid_times[0]]
            else:
                # Ohne Gruppierung einfach den letzten nicht-NaN Wert verwenden
                for col in numeric_cols:
                    last_valid = None
                    for idx in result.index:
                        if not pd.isna(result.at[idx, col]):
                            last_valid = result.at[idx, col]
                        elif last_valid is not None:
                            result.at[idx, col] = last_valid
        
        return result
    
    def calculate_derived_parameters(self, data):
        """
        Berechnet abgeleitete Parameter basierend auf den vorhandenen Daten.
        
        Args:
            data (pandas.DataFrame): Eingabedaten.
            
        Returns:
            pandas.DataFrame: Daten mit abgeleiteten Parametern.
        """
        result = data.copy()
        
        # Abgeleitete Parameter aus der Konfiguration laden
        derived_params = self.config.get('derived_parameters', [])
        
        for param in derived_params:
            name = param.get('name')
            formula = param.get('formula')
            required_columns = param.get('required_columns', [])
            
            # Prüfen, ob alle erforderlichen Spalten vorhanden sind
            required_columns_present = True
            for col in required_columns:
                if isinstance(col, str) and not col.isdigit():
                    # Wenn es ein Spaltenname ist
                    if col not in result.columns:
                        print(f"Warnung: Erforderliche Spalte {col} für {name} nicht gefunden")
                        required_columns_present = False
                else:
                    # Wenn es eine concept_id ist
                    col_str = str(col)
                    if col_str not in result.columns:
                        print(f"Warnung: Erforderliche concept_id {col} für {name} nicht gefunden")
                        required_columns_present = False
            
            if required_columns_present:
                try:
                    # Formel auswerten
                    # Für Spalten mit Namen statt concept_ids
                    if '$["' in formula:
                        formula_with_df = formula.replace('$["', 'result["').replace('"]', '"]')
                    elif '$[' in formula:
                        # Für concept_ids
                        formula_with_df = formula.replace('$[', 'result["').replace(']', '"]')
                    else:
                        # Für einfache Variablen oder direkte Werte
                        try:
                            # Versuchen, die Formel direkt auszuwerten (für konstante Werte)
                            formula_value = eval(formula)
                            formula_with_df = str(formula_value)
                        except:
                            # Ansonsten als Spaltennamen behandeln
                            formula_with_df = formula
                    
                    print(f"Berechne {name} mit Formel: {formula_with_df}")
                    
                    # Überprüfen, ob die Spalten numerische Werte enthalten
                    for col in required_columns:
                        col_str = str(col)
                        if col_str in result.columns and result[col_str].dtype == 'object':
                            print(f"Konvertiere Spalte {col_str} zu numerischen Werten")
                            result[col_str] = pd.to_numeric(result[col_str], errors='coerce')
                    
                    # Berechnung durchführen
                    result[name] = eval(formula_with_df)
                    
                    # Ergebnisse anzeigen
                    if not result[name].empty:
                        print(f"Ergebnis für {name}: Min={result[name].min()}, Max={result[name].max()}, Mittelwert={result[name].mean()}")
                    else:
                        print(f"Keine Ergebnisse für {name} berechnet")
                except Exception as e:
                    print(f"Fehler bei der Berechnung von {name}: {e}")
            else:
                print(f"Überspringe Berechnung von {name} wegen fehlender Spalten")
        
        return result
    
    def calculate_clinical_scores(self, data):
        """
        Berechnet klinische Scores basierend auf den vorhandenen Daten.
        
        Args:
            data (pandas.DataFrame): Eingabedaten.
            
        Returns:
            pandas.DataFrame: Daten mit klinischen Scores.
        """
        result = data.copy()
        
        # Klinische Scores aus der Konfiguration laden
        clinical_scores = self.config.get('clinical_scores', [])
        
        for score in clinical_scores:
            name = score.get('name')
            components = score.get('components', [])
            
            # Score-Spalte initialisieren
            result[name] = 0
            
            # Für jeden SOFA-Teilscore
            for component in components:
                component_name = component.get('name')
                parameter = component.get('parameter')
                thresholds = component.get('thresholds', [])
                scores = component.get('scores', [])
                
                # Parameter kann entweder ein Spaltenname oder eine concept_id sein
                param_col = parameter
                if isinstance(parameter, int) or (isinstance(parameter, str) and parameter.isdigit()):
                    # Wenn parameter eine concept_id ist, suchen wir die entsprechende Spalte
                    concept_id = str(parameter)
                    print(f"Suche nach Spalte für concept_id {concept_id}")
                    print(f"Verfügbare Spalten: {result.columns.tolist()}")
                    
                    # Exakte Übereinstimmung
                    if concept_id in result.columns:
                        param_col = concept_id
                        print(f"Exakte Übereinstimmung gefunden: {param_col}")
                    else:
                        # Suche nach Spalten mit dem Namen statt der concept_id
                        if isinstance(parameter, str) and parameter in result.columns:
                            param_col = parameter
                            print(f"Parameter als Name gefunden: {param_col}")
                        else:
                            print(f"Warnung: Parameter {parameter} nicht in Daten gefunden oder Thresholds/Scores ungültig")
                            continue
                else:
                    # Wenn parameter ein Name ist, prüfen wir, ob er in den Spalten existiert
                    if parameter not in result.columns:
                        # Versuchen, ähnliche Spalten zu finden
                        if parameter == "MAP" and "Mean arterial pressure" in result.columns:
                            param_col = "Mean arterial pressure"
                            print(f"Parameter {parameter} als 'Mean arterial pressure' gefunden")
                        # Erweiterte Suche nach Parametern mit ähnlichen Namen
                        else:
                            # Suche nach Spalten, die den Parameternamen enthalten
                            matching_cols = [col for col in result.columns if parameter in col]
                            if matching_cols:
                                param_col = matching_cols[0]  # Erste Übereinstimmung verwenden
                                print(f"Parameter {parameter} als '{param_col}' gefunden (Teilübereinstimmung)")
                            else:
                                # Spezielle Zuordnungen für bekannte Parameter
                                parameter_mappings = {
                                    "Platelets": ["Platelets [#/volume] in Blood", "Thrombocytes", "Platelet count"],
                                    "Bilirubin.total": ["Bilirubin.total [Mass/volume] in Serum or Plasma", "Total bilirubin", "Bilirubin"],
                                    "Creatinine": ["Creatinine [Mass/volume] in Serum or Plasma", "Serum creatinine", "Creatinine level"],
                                    "PaO2_FiO2_ratio": ["PaO2/FiO2", "P/F ratio", "Oxygen [Partial pressure] in Arterial blood", "PaO2"]
                                }
                                
                                found = False
                                if parameter in parameter_mappings:
                                    for alt_name in parameter_mappings[parameter]:
                                        if alt_name in result.columns:
                                            param_col = alt_name
                                            print(f"Parameter {parameter} als '{param_col}' gefunden (Mapping)")
                                            found = True
                                            break
                                
                                if not found:
                                    print(f"Warnung: Parameter {parameter} nicht in Daten gefunden oder Thresholds/Scores ungültig")
                                    continue
                
                if param_col in result.columns and len(thresholds) + 1 == len(scores):
                    # Überprüfen, ob die Spalte Werte enthält
                    if result[param_col].notna().sum() == 0:
                        print(f"Warnung: Spalte {param_col} enthält keine Werte")
                        continue
                
                    # Überprüfen, ob die Werte im erwarteten Bereich liegen
                    if component_name == 'respiratory' and result[param_col].max() > 1000:
                        print(f"Warnung: PaO2/FiO2-Werte ungewöhnlich hoch (max={result[param_col].max()})")
                    elif component_name == 'coagulation' and result[param_col].max() > 1000:
                        print(f"Warnung: Thrombozytenwerte ungewöhnlich hoch (max={result[param_col].max()})")
                    elif component_name == 'liver' and result[param_col].max() > 50:
                        print(f"Warnung: Bilirubinwerte ungewöhnlich hoch (max={result[param_col].max()})")
                    elif component_name == 'cardiovascular' and result[param_col].max() > 200:
                        print(f"Warnung: MAP-Werte ungewöhnlich hoch (max={result[param_col].max()})")
                    elif component_name == 'renal' and result[param_col].max() > 20:
                        print(f"Warnung: Kreatininwerte ungewöhnlich hoch (max={result[param_col].max()})")
                    
                    print(f"Berechne SOFA-Komponente {component_name} mit Parameter {param_col}")
                    print(f"Werte in {param_col}: Min={result[param_col].min()}, Max={result[param_col].max()}, Median={result[param_col].median()}")
                    
                    # Score-Komponente berechnen
                    # Standardmäßig mit dem niedrigsten Score (0) initialisieren
                    component_score = pd.Series(scores[0], index=result.index)
                    
                    # Richtung der Schwellenwerte aus der Konfiguration lesen
                    direction = component.get('direction', 'descending')
                    
                    # Standardrichtungen für bekannte SOFA-Komponenten festlegen, falls nicht angegeben
                    if 'direction' not in component:
                        if component_name == 'respiratory' or component_name == 'coagulation' or component_name == 'cardiovascular' or component_name == 'cns':
                            direction = 'descending'  # Niedrigere Werte sind schlechter
                        elif component_name == 'liver' or component_name == 'renal':
                            direction = 'ascending'   # Höhere Werte sind schlechter
                    
                    print(f"Komponente {component_name} verwendet Richtung: {direction}")
                    
                    # SOFA-spezifische Logik basierend auf der Richtung der Schwellenwerte
                    if direction == 'ascending':  # Höhere Werte sind schlechter (z.B. Bilirubin, Kreatinin)
                        # Initialisiere mit dem niedrigsten Score (0)
                        component_score = pd.Series(scores[0], index=result.index)
                        
                        # Für jeden Schwellenwert und Score
                        for i in range(len(thresholds)):
                            # Wert > Schwellenwert bedeutet höherer SOFA-Score
                            mask = result[param_col] > thresholds[i]
                            component_score[mask] = scores[i+1]
                    else:  # direction == 'descending', Niedrigere Werte sind schlechter (z.B. PaO2/FiO2, Thrombozyten, MAP, GCS)
                        # Initialisiere mit dem niedrigsten Score (0)
                        component_score = pd.Series(scores[0], index=result.index)
                        
                        # Für jeden Schwellenwert und Score
                        for i in range(len(thresholds)):
                            # Wert < Schwellenwert bedeutet höherer SOFA-Score
                            mask = result[param_col] < thresholds[i]
                            component_score[mask] = scores[i+1]
                    
                    # Spezielle Behandlung für GCS
                    if component_name == 'cns':
                        # Überprüfen, ob der GCS-Wert plausibel ist
                        # GCS sollte zwischen 3 und 15 liegen
                        invalid_gcs = (result[param_col] < 3) | (result[param_col] > 15)
                        if invalid_gcs.any():
                            print(f"Warnung: {invalid_gcs.sum()} GCS-Werte außerhalb des gültigen Bereichs (3-15)")
                            # Setze ungültige Werte auf NaN, um sie später zu imputieren
                            component_score[invalid_gcs] = np.nan
                    
                    # Komponente zum Gesamtscore hinzufügen
                    if name not in result.columns:
                        result[name] = 0
                    
                    # Überprüfen, ob die Komponente gültige Werte hat
                    if not component_score.isna().all():
                        result[name] += component_score
                        print(f"Komponente {component_name} zum Gesamtscore hinzugefügt")
                    else:
                        print(f"Warnung: Komponente {component_name} hat keine gültigen Werte und wird nicht zum Gesamtscore hinzugefügt")
                    
                    # Komponente als separate Spalte speichern
                    result[f"{name}_{component_name}"] = component_score
                    print(f"Komponente {component_name} berechnet: Min={component_score.min()}, Max={component_score.max()}, Mittelwert={component_score.mean()}")
                else:
                    print(f"Warnung: Parameter {param_col} nicht in Daten gefunden oder Thresholds/Scores ungültig")
            
            # Überprüfen des Gesamtscores
            if name in result.columns:
                # Begrenze den SOFA-Score auf maximal 24 Punkte
                result[name] = result[name].clip(upper=24)
                
                # Überprüfen auf ungewöhnlich hohe Werte
                high_scores = result[result[name] > 15]
                if not high_scores.empty:
                    print(f"Warnung: {len(high_scores)} Einträge haben einen SOFA-Score > 15")
                    print(f"Beispiel für hohe Scores: {high_scores[name].head()}")
                
                print(f"SOFA-Gesamtscore berechnet: Min={result[name].min()}, Max={result[name].max()}, Mittelwert={result[name].mean()}")
            else:
                print(f"Warnung: {name} wurde nicht berechnet, da keine Komponenten gefunden wurden")
        
        return result
    
    def run_pipeline(self, data=None, save_to_db=False):
        """
        Führt die gesamte Pipeline aus.
        
        Args:
            data (pandas.DataFrame, optional): Eingabedaten. Wenn None, werden die Daten aus der Datenbank geladen.
            save_to_db (bool, optional): Ob die Ergebnisse in der Datenbank gespeichert werden sollen.
            
        Returns:
            pandas.DataFrame: Ergebnis der Pipeline.
        """
        # Daten laden, falls nicht bereitgestellt
        if data is None:
            data = self.load_data()
        
        # Pipeline-Schritte ausführen
        if self.config.get('pivot_data', True):
            data = self.pivot_data(data)
        
        if self.config.get('aggregate_data', True):
            data = self.aggregate_data(data)
        
        if self.config.get('impute_missing_values', True):
            data = self.impute_missing_values(data)
        
        if self.config.get('calculate_derived_parameters', True):
            data = self.calculate_derived_parameters(data)
        
        if self.config.get('calculate_clinical_scores', True):
            data = self.calculate_clinical_scores(data)
        
        # Ergebnisse in der Datenbank speichern
        if save_to_db:
            self._save_to_database(data)
        
        return data
    
    def _save_to_database(self, data, table=None, schema=None, if_exists='replace'):
        """
        Speichert die Daten in der Datenbank.
        
        Args:
            data (pandas.DataFrame): Zu speichernde Daten.
            table (str, optional): Name der Zieltabelle. Wenn None, wird die Tabelle aus der Konfiguration verwendet.
            schema (str, optional): Name des Zielschemas. Wenn None, wird das Ausgabeschema aus der Konfiguration verwendet.
            if_exists (str, optional): Verhalten, wenn die Tabelle bereits existiert ('fail', 'replace', 'append').
        """
        if table is None:
            table = self.config.get('output_table', 'gold_parameters')
        
        if schema is None:
            schema = self.db.get_output_schema()
        
        # Verbindung zur Datenbank herstellen
        engine = self.db.connect()
        
        # Daten in der Datenbank speichern
        data.to_sql(
            name=table,
            schema=schema,
            con=engine,
            if_exists=if_exists,
            index=False
        )

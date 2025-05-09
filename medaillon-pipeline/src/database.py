import pandas as pd
from sqlalchemy import create_engine
import yaml
import os


class DatabaseConnection:
    """
    Klasse zur Verwaltung der Datenbankverbindung für die Medaillon-Pipeline.
    """
    
    def __init__(self, config_path=None):
        """
        Initialisiert die Datenbankverbindung mit den Konfigurationsparametern.
        
        Args:
            config_path (str, optional): Pfad zur Konfigurationsdatei. 
                                         Wenn None, wird die Standardkonfiguration verwendet.
        """
        if config_path is None:
            # Standardpfad zur Konfigurationsdatei
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(base_dir, 'config', 'gold', 'database.yaml')
        
        # Konfiguration laden
        self.config = self._load_config(config_path)
        
        # Verbindungsstring erstellen
        self.connection_string = self._create_connection_string()
        
        # Engine erstellen
        self.engine = None
    
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
    
    def _create_connection_string(self):
        """
        Erstellt den Verbindungsstring für SQLAlchemy.
        
        Returns:
            str: SQLAlchemy-Verbindungsstring.
        """
        db_config = self.config['database']
        return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    def connect(self):
        """
        Stellt eine Verbindung zur Datenbank her.
        
        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy-Engine-Objekt.
        """
        if self.engine is None:
            self.engine = create_engine(self.connection_string)
        return self.engine
    
    def execute_query(self, query):
        """
        Führt eine SQL-Abfrage aus und gibt das Ergebnis als DataFrame zurück.
        
        Args:
            query (str): SQL-Abfrage.
            
        Returns:
            pandas.DataFrame: Ergebnis der Abfrage.
        """
        from sqlalchemy import text
        engine = self.connect()
        return pd.read_sql(text(query), engine)
    
    def get_schema_names(self):
        """
        Gibt die Namen der Schemas in der Datenbank zurück.
        
        Returns:
            list: Liste der Schema-Namen.
        """
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT LIKE 'pg_%' 
        AND schema_name != 'information_schema'
        """
        result = self.execute_query(query)
        return result['schema_name'].tolist()
    
    def get_tables(self, schema=None):
        """
        Gibt die Tabellen in einem Schema zurück.
        
        Args:
            schema (str, optional): Name des Schemas. Wenn None, wird das Eingabeschema aus der Konfiguration verwendet.
            
        Returns:
            list: Liste der Tabellennamen.
        """
        if schema is None:
            schema = self.config['database']['schema_input']
        
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema}'
        """
        result = self.execute_query(query)
        return result['table_name'].tolist()
    
    def get_columns(self, table, schema=None):
        """
        Gibt die Spalten einer Tabelle zurück.
        
        Args:
            table (str): Name der Tabelle.
            schema (str, optional): Name des Schemas. Wenn None, wird das Eingabeschema aus der Konfiguration verwendet.
            
        Returns:
            list: Liste der Spaltennamen.
        """
        if schema is None:
            schema = self.config['database']['schema_input']
        
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        """
        result = self.execute_query(query)
        return result['column_name'].tolist()
    
    def get_input_schema(self):
        """
        Gibt das Eingabeschema aus der Konfiguration zurück.
        
        Returns:
            str: Name des Eingabeschemas.
        """
        return self.config['database']['schema_input']
    
    def get_output_schema(self):
        """
        Gibt das Ausgabeschema aus der Konfiguration zurück.
        
        Returns:
            str: Name des Ausgabeschemas.
        """
        return self.config['database']['schema_output']

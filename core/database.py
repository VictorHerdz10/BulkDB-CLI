import psycopg2
from psycopg2 import sql
from typing import Tuple, Optional, List, Dict, Any
import sys

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.cursor = None
    
    def validate_connection(self, connection_string: str) -> Tuple[bool, str]:
        """Valida si el string de conexión es correcto"""
        try:
            # Cerrar conexión existente si hay una
            self.close_connection()
            
            self.connection = psycopg2.connect(connection_string)
            self.cursor = self.connection.cursor()
            
            # Test simple de conexión
            self.cursor.execute("SELECT version();")
            postgres_version = self.cursor.fetchone()
            
            return True, f"✅ Conexión exitosa - PostgreSQL {postgres_version[0].split(',')[0]}"
            
        except psycopg2.Error as e:
            return False, f"❌ Error de conexión: {e}"
    
    def table_exists(self, table_name: str) -> bool:
        """Verifica si una tabla existe en la base de datos"""
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """
            self.cursor.execute(query, (table_name,))
            return self.cursor.fetchone()[0]
        except psycopg2.Error:
            return False
    
    def get_table_columns(self, table_name: str) -> List[Tuple]:
        """Obtiene información de las columnas de una tabla"""
        query = """
            SELECT 
                column_name, 
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """
        self.cursor.execute(query, (table_name,))
        return self.cursor.fetchall()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Any:
        """Ejecuta una consulta SQL"""
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return True
        except psycopg2.Error as e:
            self.connection.rollback()
            raise e
    
    def get_primary_key(self, table_name: str) -> Optional[str]:
        """Obtiene la clave primaria de una tabla"""
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s 
                AND tc.constraint_type = 'PRIMARY KEY'
            LIMIT 1;
        """
        self.cursor.execute(query, (table_name,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """Obtiene información de claves foráneas"""
        query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
        """
        
        try:
            self.cursor.execute(query, (table_name,))
            results = self.cursor.fetchall()
            
            return [
                {
                    'column': row[0],
                    'foreign_table': row[1],
                    'foreign_column': row[2]
                }
                for row in results
            ]
        except psycopg2.Error:
            return []
    
    def get_column_sample_data(self, table_name: str, column_name: str, limit: int = 10) -> List[Any]:
        """Obtiene datos de ejemplo de una columna para análisis"""
        try:
            query = sql.SQL("SELECT DISTINCT {} FROM {} LIMIT {}").format(
                sql.Identifier(column_name),
                sql.Identifier(table_name),
                sql.Literal(limit)
            )
            self.cursor.execute(query)
            return [row[0] for row in self.cursor.fetchall()]
        except psycopg2.Error:
            return []
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
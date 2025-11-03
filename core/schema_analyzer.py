from typing import Dict, List, Any, Tuple
import psycopg2
from psycopg2 import sql

class SchemaAnalyzer:
    def __init__(self, database_manager):
        self.db = database_manager
    
    def analyze_relationships(self, table_name: str) -> List[Dict[str, Any]]:
        """Analiza las relaciones de una tabla (1:1, 1:M, M:M)"""
        relationships = []
        
        try:
            # Buscar claves foráneas
            query = """
                SELECT
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                    JOIN information_schema.referential_constraints AS rc
                      ON rc.constraint_name = tc.constraint_name
                WHERE 
                    tc.constraint_type = 'FOREIGN KEY' 
                    AND tc.table_name = %s;
            """
            
            self.db.cursor.execute(query, (table_name,))
            foreign_keys = self.db.cursor.fetchall()
            
            for fk in foreign_keys:
                relationship_type = self._determine_relationship_type(
                    table_name, fk[2]  # foreign_table_name
                )
                
                relationships.append({
                    'column': fk[1],  # column_name
                    'foreign_table': fk[2],  # foreign_table_name
                    'foreign_column': fk[3],  # foreign_column_name
                    'relationship_type': relationship_type,
                    'on_update': fk[4],
                    'on_delete': fk[5]
                })
                
        except psycopg2.Error as e:
            print(f"Error analizando relaciones: {e}")
        
        return relationships
    
    def _determine_relationship_type(self, table1: str, table2: str) -> str:
        """Determina el tipo de relación entre dos tablas"""
        try:
            # Análisis simplificado - en una implementación real haríamos más verificaciones
            # Por ahora asumimos uno_a_muchos como predeterminado
            return "uno_a_muchos"
                
        except psycopg2.Error:
            return "uno_a_muchos"  # Por defecto
    
    def check_foreign_table_data(self, foreign_table: str) -> Tuple[bool, int]:
        """Verifica si la tabla relacionada tiene datos"""
        try:
            query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(foreign_table)
            )
            self.db.cursor.execute(query)
            count = self.db.cursor.fetchone()[0]
            return count > 0, count
        except psycopg2.Error:
            return False, 0
    
    def get_table_row_count(self, table_name: str) -> int:
        """Obtiene el número de filas en una tabla"""
        try:
            query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            )
            self.db.cursor.execute(query)
            return self.db.cursor.fetchone()[0]
        except psycopg2.Error:
            return 0
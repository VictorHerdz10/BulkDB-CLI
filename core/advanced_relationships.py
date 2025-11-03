from typing import Dict, List, Any, Tuple, Optional
import psycopg2
from psycopg2 import sql
import random

class RelationshipManager:
    def __init__(self, database_manager, schema_analyzer):
        self.db = database_manager
        self.analyzer = schema_analyzer
    
    def analyze_advanced_relationships(self, table_name: str) -> Dict[str, Any]:
        """Análisis avanzado de relaciones incluyendo cardinalidad"""
        basic_relationships = self.analyzer.analyze_relationships(table_name)
        advanced_info = {}
        
        for rel in basic_relationships:
            advanced_info[rel['column']] = {
                **rel,
                'cardinality': self._determine_cardinality(rel),
                'data_availability': self._check_data_availability(rel),
                'recommendation': self._generate_recommendation(rel)
            }
        
        return advanced_info
    
    def _determine_cardinality(self, relationship: Dict) -> str:
        """Determina la cardinalidad exacta de la relación"""
        try:
            # Contar registros únicos en la tabla foránea
            foreign_query = sql.SQL("SELECT COUNT(DISTINCT {}) FROM {}").format(
                sql.Identifier(relationship['foreign_column']),
                sql.Identifier(relationship['foreign_table'])
            )
            self.db.cursor.execute(foreign_query)
            foreign_unique = self.db.cursor.fetchone()[0]
            
            # Para relaciones uno-a-muchos, verificar si es realmente uno-a-uno
            if relationship['relationship_type'] == 'uno_a_muchos':
                # Si hay una restricción única, podría ser uno-a-uno
                unique_check = """
                    SELECT COUNT(*) 
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_name = %s 
                        AND kcu.column_name = %s
                        AND tc.constraint_type = 'UNIQUE'
                """
                self.db.cursor.execute(unique_check, 
                    (relationship['foreign_table'], relationship['foreign_column']))
                has_unique = self.db.cursor.fetchone()[0] > 0
                
                if has_unique and foreign_unique == 1:
                    return "uno_a_uno_exacto"
                elif has_unique:
                    return "uno_a_uno_potencial"
            
            return relationship['relationship_type']
            
        except psycopg2.Error:
            return relationship['relationship_type']
    
    def _check_data_availability(self, relationship: Dict) -> Dict[str, Any]:
        """Verifica disponibilidad y distribución de datos"""
        try:
            # Contar total de registros
            count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(relationship['foreign_table'])
            )
            self.db.cursor.execute(count_query)
            total_records = self.db.cursor.fetchone()[0]
            
            # Obtener valores únicos
            distinct_query = sql.SQL("SELECT DISTINCT {} FROM {}").format(
                sql.Identifier(relationship['foreign_column']),
                sql.Identifier(relationship['foreign_table'])
            )
            self.db.cursor.execute(distinct_query)
            distinct_values = [row[0] for row in self.db.cursor.fetchall()]
            
            return {
                'has_data': total_records > 0,
                'total_records': total_records,
                'distinct_values': len(distinct_values),
                'values': distinct_values
            }
            
        except psycopg2.Error:
            return {'has_data': False, 'total_records': 0, 'distinct_values': 0, 'values': []}
    
    def _generate_recommendation(self, relationship: Dict) -> str:
        """Genera recomendaciones basadas en el análisis de relaciones"""
        availability = self._check_data_availability(relationship)
        
        if not availability['has_data']:
            return f"Poblar tabla {relationship['foreign_table']} primero"
        
        if availability['distinct_values'] < 10:
            return f"Solo {availability['distinct_values']} valores únicos disponibles"
        
        if relationship['relationship_type'] == 'muchos_a_muchos':
            return "Considerar tabla intermedia para relación muchos-a-muchos"
        
        return "Relación lista para usar"
    
    def generate_relationship_data(self, relationship: Dict, count: int) -> List[Any]:
        """Genera datos para campos de relación considerando cardinalidad"""
        availability = self._check_data_availability(relationship)
        
        if not availability['has_data']:
            # Si no hay datos, generar valores aleatorios del tipo correcto
            return self._generate_fallback_values(relationship, count)
        
        values = availability['values']
        
        # Estrategias basadas en cardinalidad
        if relationship['relationship_type'] == 'uno_a_uno':
            # Para uno-a-uno, usar valores únicos
            if len(values) >= count:
                return random.sample(values, count)
            else:
                # Completar con valores aleatorios si no hay suficientes únicos
                result = values.copy()
                while len(result) < count:
                    result.append(random.choice(values))
                return result[:count]
        
        else:
            # Para uno-a-muchos, permitir repeticiones
            return [random.choice(values) for _ in range(count)]
    
    def _generate_fallback_values(self, relationship: Dict, count: int) -> List[Any]:
        """Genera valores de fallback cuando no hay datos de relación"""
        # Obtener tipo de dato de la columna foránea
        try:
            columns = self.db.get_table_columns(relationship['foreign_table'])
            foreign_col_info = next(
                (col for col in columns if col[0] == relationship['foreign_column']), 
                None
            )
            
            if foreign_col_info:
                data_type = foreign_col_info[1]
                # Generar valores básicos del tipo correcto
                if 'int' in data_type:
                    return list(range(1, count + 1))
                elif 'char' in data_type or 'text' in data_type:
                    return [f"temp_{i}" for i in range(1, count + 1)]
                elif 'bool' in data_type:
                    return [random.choice([True, False]) for _ in range(count)]
            
        except psycopg2.Error:
            pass
        
        # Fallback genérico
        return list(range(1, count + 1))
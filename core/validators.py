from typing import Dict, List, Any, Tuple, Optional
import psycopg2
from psycopg2 import sql
from utils.helpers import ValidationHelpers

class DataValidator:
    def __init__(self, database_manager):
        self.db = database_manager
        self.helpers = ValidationHelpers()
    
    def validate_table_structure(self, table_name: str) -> Tuple[bool, List[str]]:
        """Valida la estructura de una tabla"""
        warnings = []
        
        try:
            # Verificar si la tabla existe
            if not self.db.table_exists(table_name):
                return False, ["La tabla no existe"]
            
            # Obtener columnas
            columns = self.db.get_table_columns(table_name)
            if not columns:
                return False, ["La tabla no tiene columnas"]
            
            # Verificar si tiene clave primaria
            primary_key = self.db.get_primary_key(table_name)
            if not primary_key:
                warnings.append("La tabla no tiene clave primaria definida")
            
            # Analizar cada columna
            for col in columns:
                col_name, data_type, is_nullable, default, max_length, numeric_precision, numeric_scale = col
                
                # Verificar nombres de columnas
                if not self._is_valid_column_name(col_name):
                    warnings.append(f"Nombre de columna '{col_name}' puede ser problemático")
                
                # Verificar tipos de datos soportados
                if not self._is_supported_data_type(data_type):
                    warnings.append(f"Tipo de dato '{data_type}' en '{col_name}' puede necesitar configuración especial")
            
            return True, warnings
            
        except psycopg2.Error as e:
            return False, [f"Error validando tabla: {e}"]
    
    def _is_valid_column_name(self, name: str) -> bool:
        """Verifica si el nombre de columna es válido"""
        if not name:
            return False
            
        # Evitar palabras reservadas de SQL
        reserved_words = {
            'select', 'insert', 'update', 'delete', 'where', 'from', 'table', 'column',
            'join', 'inner', 'outer', 'left', 'right', 'group', 'order', 'by', 'having',
            'distinct', 'limit', 'offset', 'as', 'on', 'and', 'or', 'not', 'in', 'like',
            'between', 'is', 'null', 'true', 'false'
        }
        return name.lower() not in reserved_words and name.replace('_', '').isalnum()
    
    def _is_supported_data_type(self, data_type: str) -> bool:
        """Verifica si el tipo de dato es soportado"""
        if not data_type:
            return False
            
        supported_types = {
            'integer', 'bigint', 'smallint', 'serial', 'bigserial',
            'numeric', 'decimal', 'real', 'double precision',
            'character varying', 'varchar', 'char', 'text',
            'boolean', 'bool',
            'date', 'time', 'timestamp', 'timestamptz',
            'json', 'jsonb', 'uuid'
        }
        return any(supported in data_type for supported in supported_types)
    
    def validate_foreign_key_constraints(self, table_name: str) -> Tuple[bool, List[str]]:
        """Valida restricciones de claves foráneas"""
        warnings = []
        
        try:
            relationships = self._get_foreign_keys(table_name)
            
            for rel in relationships:
                foreign_table = rel['foreign_table']
                foreign_column = rel['foreign_column']
                
                # Verificar si la tabla foránea existe
                if not self.db.table_exists(foreign_table):
                    warnings.append(f"Tabla foránea '{foreign_table}' no existe")
                    continue
                
                # Verificar si la tabla foránea tiene datos
                count_query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(foreign_table)
                )
                self.db.cursor.execute(count_query)
                foreign_count = self.db.cursor.fetchone()[0]
                
                if foreign_count == 0:
                    warnings.append(f"Tabla foránea '{foreign_table}' está vacía")
                
                # Verificar si la columna foránea existe
                foreign_columns = [col[0] for col in self.db.get_table_columns(foreign_table)]
                if foreign_column not in foreign_columns:
                    warnings.append(f"Columna foránea '{foreign_column}' no existe en '{foreign_table}'")
            
            return len(warnings) == 0, warnings
            
        except psycopg2.Error as e:
            return False, [f"Error validando claves foráneas: {e}"]
    
    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
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
            self.db.cursor.execute(query, (table_name,))
            results = self.db.cursor.fetchall()
            
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
    
    def validate_data_compatibility(self, table_name: str, sample_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Valida la compatibilidad de datos con la estructura de la tabla"""
        warnings = []
        
        try:
            columns_info = self.db.get_table_columns(table_name)
            columns_dict = {col[0]: col for col in columns_info}
            
            for column_name, value in sample_data.items():
                if column_name not in columns_dict:
                    warnings.append(f"Columna '{column_name}' no existe en la tabla")
                    continue
                
                col_info = columns_dict[column_name]
                data_type = col_info[1]
                is_nullable = col_info[2]
                max_length = col_info[4]
                
                # Validar nulabilidad
                if value is None and is_nullable == 'NO':
                    warnings.append(f"Columna '{column_name}' no permite NULL")
                    continue
                
                if value is not None:
                    # Validar longitud para strings
                    if max_length and len(str(value)) > max_length:
                        warnings.append(f"Valor demasiado largo para '{column_name}': {len(str(value))} > {max_length}")
                    
                    # Validar tipo de dato
                    type_warning = self._validate_data_type(value, data_type, column_name)
                    if type_warning:
                        warnings.append(type_warning)
            
            return len(warnings) == 0, warnings
            
        except Exception as e:
            return False, [f"Error validando compatibilidad: {e}"]
    
    def _validate_data_type(self, value: Any, data_type: str, column_name: str) -> Optional[str]:
        """Valida que el valor sea compatible con el tipo de dato"""
        try:
            if 'int' in data_type:
                int(value)
            elif 'numeric' in data_type or 'decimal' in data_type:
                float(value)
            elif 'bool' in data_type:
                if str(value).lower() not in ('true', 'false', '1', '0', 't', 'f', 'yes', 'no'):
                    return f"Valor '{value}' no es booleano válido para '{column_name}'"
            elif 'date' in data_type or 'time' in data_type:
                # Validación básica de fecha
                if not self.helpers.is_valid_date(str(value)):
                    return f"Valor '{value}' no es fecha válida para '{column_name}'"
            
            return None
        except (ValueError, TypeError):
            return f"Valor '{value}' no compatible con tipo '{data_type}' para '{column_name}'"
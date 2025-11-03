import re
import string
from typing import Any, List, Dict, Optional
from datetime import datetime, date
from decimal import Decimal

class ValidationHelpers:
    @staticmethod
    def is_valid_email(email: str) -> bool:
        """Valida formato de email"""
        if not email or not isinstance(email, str):
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def is_valid_phone(phone: str) -> bool:
        """Valida formato de teléfono"""
        if not phone or not isinstance(phone, str):
            return False
        # Permite formatos internacionales
        pattern = r'^[\+]?[0-9\s\-\(\)]{7,20}$'
        return bool(re.match(pattern, phone))
    
    @staticmethod
    def is_valid_date(date_str: str) -> bool:
        """Valida formato de fecha"""
        if not date_str:
            return False
            
        try:
            # Intentar parsear como fecha ISO
            datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return True
        except ValueError:
            # Intentar otros formatos comunes
            formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y.%m.%d']
            for fmt in formats:
                try:
                    datetime.strptime(date_str, fmt)
                    return True
                except ValueError:
                    continue
            return False
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Valida formato de URL"""
        if not url:
            return False
        pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, url, re.IGNORECASE))

class DataHelpers:
    @staticmethod
    def sanitize_identifier(name: str) -> str:
        """Limpia identificadores para SQL"""
        if not name:
            return ""
        return re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    @staticmethod
    def format_value_for_sql(value: Any) -> str:
        """Formatea valores para consultas SQL"""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (datetime, date)):
            return f"'{value.isoformat()}'"
        elif isinstance(value, (str, Decimal)):
            # Escapar comillas simples para SQL
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
        else:
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    @staticmethod
    def detect_data_pattern(column_name: str, data_type: str) -> str:
        """Detecta patrones de datos basados en nombre y tipo"""
        if not column_name:
            return 'generic'
            
        column_name = column_name.lower()
        
        patterns = {
            'email': ['email', 'correo', 'mail'],
            'name': ['name', 'nombre', 'firstname', 'lastname', 'fullname'],
            'phone': ['phone', 'telefono', 'movil', 'celular', 'tel'],
            'address': ['address', 'direccion', 'calle', 'ciudad', 'domicilio'],
            'date': ['date', 'fecha', 'created', 'updated', 'timestamp'],
            'boolean': ['active', 'activo', 'enabled', 'habilitado', 'is_', 'flag'],
            'currency': ['price', 'precio', 'amount', 'monto', 'salary', 'salario', 'cost'],
            'url': ['url', 'link', 'website', 'web']
        }
        
        for pattern, keywords in patterns.items():
            if any(keyword in column_name for keyword in keywords):
                return pattern
        
        return 'generic'
    
    @staticmethod
    def generate_insert_statements(table_name: str, records: List[Dict]) -> List[str]:
        """Genera sentencias INSERT a partir de registros"""
        if not records:
            return []
        
        columns = list(records[0].keys())
        statements = []
        
        for record in records:
            values = [DataHelpers.format_value_for_sql(record[col]) for col in columns]
            column_list = ', '.join(columns)
            value_list = ', '.join(values)
            
            statements.append(
                f"INSERT INTO {table_name} ({column_list}) VALUES ({value_list});"
            )
        
        return statements

class TypeConversionHelpers:
    @staticmethod
    def convert_to_python_type(value: Any, postgres_type: str) -> Any:
        """Convierte valores de PostgreSQL a tipos de Python"""
        if value is None:
            return None
            
        postgres_type = postgres_type.lower()
        
        try:
            if 'int' in postgres_type:
                return int(value)
            elif 'numeric' in postgres_type or 'decimal' in postgres_type or 'real' in postgres_type or 'double' in postgres_type:
                return float(value)
            elif 'bool' in postgres_type:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ('true', 't', 'yes', 'y', '1', '1.0')
            elif 'date' in postgres_type:
                if isinstance(value, (datetime, date)):
                    return value
                return datetime.fromisoformat(str(value))
            elif 'time' in postgres_type:
                if isinstance(value, datetime):
                    return value.time()
                return datetime.fromisoformat(str(value)).time()
            else:
                return str(value)
        except (ValueError, TypeError):
            return value
    
    @staticmethod
    def convert_to_postgres_type(value: Any, postgres_type: str) -> Any:
        """Convierte valores de Python a tipos de PostgreSQL"""
        if value is None:
            return None
            
        postgres_type = postgres_type.lower()
        
        try:
            if 'int' in postgres_type:
                return int(value)
            elif 'numeric' in postgres_type or 'decimal' in postgres_type:
                return Decimal(str(value))
            elif 'bool' in postgres_type:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ('true', 't', 'yes', 'y', '1', '1.0')
            elif 'date' in postgres_type:
                if isinstance(value, (datetime, date)):
                    return value
                return datetime.fromisoformat(str(value)).date()
            elif 'timestamp' in postgres_type:
                if isinstance(value, datetime):
                    return value
                return datetime.fromisoformat(str(value))
            else:
                return str(value)
        except (ValueError, TypeError):
            return value

class StringHelpers:
    @staticmethod
    def generate_random_string(length: int = 10, include_digits: bool = True, include_special: bool = False) -> str:
        """Genera una cadena aleatoria"""
        chars = string.ascii_letters
        if include_digits:
            chars += string.digits
        if include_special:
            chars += "!@#$%^&*()_+-=[]{}|;:,.<>?"
        
        return ''.join(random.choice(chars) for _ in range(length))
    
    @staticmethod
    def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
        """Trunca una cadena si excede la longitud máxima"""
        if not text or len(text) <= max_length:
            return text
        
        if max_length <= len(suffix):
            return suffix[:max_length]
        
        return text[:max_length - len(suffix)] + suffix

# Importar random para las funciones que lo necesitan
import random
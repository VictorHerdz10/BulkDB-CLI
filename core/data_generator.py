import random
import string
from datetime import datetime, timedelta
from typing import Any, List, Dict, Set
import re

class DataGenerator:
    def __init__(self):
        self.faker_data = {
            'names': ['Juan', 'María', 'Carlos', 'Ana', 'Luis', 'Laura', 'Pedro', 'Sofia', 'Miguel', 'Elena'],
            'lastnames': ['Gómez', 'Rodríguez', 'López', 'Martínez', 'García', 'Pérez', 'Sánchez', 'Romero'],
            'emails': ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'empresa.com'],
            'cities': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao', 'Málaga', 'Zaragoza', 'Murcia'],
            'companies': ['TechCorp', 'InnovaSoft', 'DataSystems', 'WebSolutions', 'CloudTech', 'DigitalMinds'],
            'products': ['Laptop', 'Mouse', 'Teclado', 'Monitor', 'Tablet', 'Smartphone', 'Impresora', 'Router'],
            'streets': ['Mayor', 'Real', 'Gran Vía', 'Sol', 'Castellana', 'Diagonal', 'Rambla', 'Independencia']
        }
        
        self.generated_values_cache: Dict[str, Set[Any]] = {}
        self.column_patterns = self._initialize_patterns()
    
    def _initialize_patterns(self) -> Dict[str, tuple]:
        """Inicializa patrones para detección inteligente de columnas"""
        return {
            'email': (r'.*email.*', self._generate_email),
            'name': (r'.*name.*|.*nombre.*', self._generate_name),
            'phone': (r'.*phone.*|.*telefono.*|.*teléfono.*', self._generate_phone),
            'address': (r'.*address.*|.*direccion.*|.*dirección.*', self._generate_address),
            'city': (r'.*city.*|.*ciudad.*', self._generate_city),
            'company': (r'.*company.*|.*empresa.*', self._generate_company),
            'product': (r'.*product.*|.*producto.*', self._generate_product),
            'description': (r'.*desc.*', self._generate_description),
            'code': (r'.*code.*|.*codigo.*|.*código.*', self._generate_code),
            'price': (r'.*price.*|.*precio.*|.*cost.*|.*costo.*', self._generate_price),
            'quantity': (r'.*quantity.*|.*cantidad.*|.*stock.*', self._generate_quantity),
            'status': (r'.*status.*|.*estado.*', self._generate_status),
            'active': (r'.*active.*|.*activo.*', self._generate_boolean),
            'flag': (r'.*flag.*', self._generate_boolean),
        }
    
    def generate_value(self, data_type: str, column_name: str, max_length: int = None, sample_data: List[Any] = None) -> Any:
        """Genera valores realistas basados en el tipo de dato, nombre de columna y datos existentes"""
        
        # Primero intentar usar datos existentes si están disponibles
        if sample_data and len(sample_data) > 0:
            return random.choice(sample_data)
        
        # Detección inteligente por nombre de columna
        column_name_lower = column_name.lower()
        for pattern_name, (pattern, generator_func) in self.column_patterns.items():
            if re.match(pattern, column_name_lower, re.IGNORECASE):
                value = generator_func()
                if max_length and isinstance(value, str):
                    return value[:max_length]
                return value
        
        # Generación por tipo de dato PostgreSQL
        data_type_lower = data_type.lower()
        
        if 'integer' in data_type_lower or 'int' in data_type_lower:
            return random.randint(1, 1000)
        
        elif 'boolean' in data_type_lower or 'bool' in data_type_lower:
            return random.choice([True, False])
        
        elif 'timestamp' in data_type_lower:
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2023, 12, 31)
            random_date = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            return random_date
        
        elif 'date' in data_type_lower:
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2023, 12, 31)
            random_date = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            return random_date.date()
        
        elif 'time' in data_type_lower:
            return timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        
        elif 'character varying' in data_type_lower or 'varchar' in data_type_lower or 'text' in data_type_lower:
            if max_length:
                base_text = f"Texto ejemplo para {column_name}"
                return base_text[:max_length]
            return f"Texto de ejemplo para la columna {column_name}"
        
        elif 'numeric' in data_type_lower or 'decimal' in data_type_lower:
            return round(random.uniform(10.0, 1000.0), 2)
        
        elif 'uuid' in data_type_lower:
            import uuid
            return str(uuid.uuid4())
        
        else:
            # Fallback para tipos desconocidos
            return f"dato_{random.randint(1000, 9999)}"
    
    # Métodos específicos de generación
    def _generate_email(self) -> str:
        name = random.choice(self.faker_data['names']).lower()
        lastname = random.choice(self.faker_data['lastnames']).lower()
        domain = random.choice(self.faker_data['emails'])
        return f"{name}.{lastname}@{domain}"
    
    def _generate_name(self) -> str:
        return f"{random.choice(self.faker_data['names'])} {random.choice(self.faker_data['lastnames'])}"
    
    def _generate_phone(self) -> str:
        return f"+34 {random.randint(600, 699)} {random.randint(100, 999)} {random.randint(100, 999)}"
    
    def _generate_address(self) -> str:
        street = random.choice(self.faker_data['streets'])
        number = random.randint(1, 200)
        return f"Calle {street} {number}"
    
    def _generate_city(self) -> str:
        return random.choice(self.faker_data['cities'])
    
    def _generate_company(self) -> str:
        return random.choice(self.faker_data['companies'])
    
    def _generate_product(self) -> str:
        return random.choice(self.faker_data['products'])
    
    def _generate_description(self) -> str:
        descriptions = [
            "Producto de alta calidad con garantía",
            "Servicio profesional y confiable",
            "Solución innovadora para tus necesidades",
            "Artículo premium con excelentes características"
        ]
        return random.choice(descriptions)
    
    def _generate_code(self) -> str:
        return f"COD{random.randint(1000, 9999)}"
    
    def _generate_price(self) -> float:
        return round(random.uniform(5.0, 500.0), 2)
    
    def _generate_quantity(self) -> int:
        return random.randint(0, 1000)
    
    def _generate_status(self) -> str:
        statuses = ['active', 'inactive', 'pending', 'completed', 'cancelled']
        return random.choice(statuses)
    
    def _generate_boolean(self) -> bool:
        return random.choice([True, False])
    
    def generate_unique_values(self, data_type: str, column_name: str, count: int, max_length: int = None, sample_data: List[Any] = None) -> List[Any]:
        """Genera valores únicos para evitar duplicados"""
        cache_key = f"{data_type}_{column_name}"
        
        if cache_key not in self.generated_values_cache:
            self.generated_values_cache[cache_key] = set()
        
        values = set()
        attempts = 0
        max_attempts = count * 10  # Límite para evitar loops infinitos
        
        while len(values) < count and attempts < max_attempts:
            new_value = self.generate_value(data_type, column_name, max_length, sample_data)
            if new_value not in self.generated_values_cache[cache_key]:
                values.add(new_value)
                self.generated_values_cache[cache_key].add(new_value)
            attempts += 1
        
        # Si no podemos generar suficientes valores únicos, completamos con sufijos
        result = list(values)
        while len(result) < count:
            base_value = self.generate_value(data_type, column_name, max_length, sample_data)
            if isinstance(base_value, (int, float)):
                unique_value = base_value + len(result) + 1
            else:
                unique_value = f"{base_value}_{len(result) + 1000}"
            result.append(unique_value)
            self.generated_values_cache[cache_key].add(unique_value)
        
        return result
    
    def clear_cache(self):
        """Limpia la caché de valores generados"""
        self.generated_values_cache.clear()
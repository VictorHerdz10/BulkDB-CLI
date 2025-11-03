import random
import string
from datetime import datetime, timedelta
from typing import Any, List, Dict, Set
import re
from .field_predictor import FieldPredictor

class DataGenerator:
    def __init__(self):
        self.faker_data = {
            'names': ['Juan', 'María', 'Carlos', 'Ana', 'Luis', 'Laura', 'Pedro', 'Sofia', 'Miguel', 'Elena',
                     'John', 'Mary', 'David', 'Sarah', 'Michael', 'Jennifer', 'Robert', 'Linda', 'William', 'Elizabeth'],
            'lastnames': ['Gómez', 'Rodríguez', 'López', 'Martínez', 'García', 'Pérez', 'Sánchez', 'Romero',
                         'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'],
            'emails': ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'enterprise.com'],
            'cities': ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao', 'Málaga', 'Zaragoza', 'Murcia',
                      'New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto', 'Mumbai'],
            'companies': ['TechCorp', 'InnovaSoft', 'DataSystems', 'WebSolutions', 'CloudTech', 'DigitalMinds',
                         'GlobalInc', 'FutureTech', 'SmartSolutions', 'EliteCorp'],
            'products': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Tablet', 'Smartphone', 'Printer', 'Router',
                        'Headphones', 'Camera', 'Speaker', 'Watch', 'Charger', 'Cable', 'Adapter'],
            'streets': ['Main', 'First', 'Second', 'Third', 'Park', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm'],
            'categories': ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty', 'Toys', 'Food', 'Health', 'Automotive']
        }
        
        self.generated_values_cache: Dict[str, Set[Any]] = {}
        self.column_patterns = self._initialize_patterns()
        self.field_predictor = FieldPredictor()
        self.unique_value_tracker: Dict[str, Set[Any]] = {}
        self.sequence_counters: Dict[str, int] = {}
    
    def _initialize_patterns(self) -> Dict[str, tuple]:
        """Inicializa patrones para detección inteligente de columnas"""
        return {
            'id': (r'^id$|.*_id$', self._generate_id),
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
            'url': (r'.*url.*|.*link.*|.*website.*', self._generate_url),
            'category': (r'.*category.*|.*categoria.*|.*type.*', self._generate_category),
            'date': (r'.*date.*|.*fecha.*', self._generate_date),
            'age': (r'.*age.*|.*edad.*', self._generate_age),  # NUEVO: Patrón para edad
        }
    
    def generate_value(self, data_type: str, column_name: str, max_length: int = None, 
                      sample_data: List[Any] = None, table_name: str = None, 
                      is_unique: bool = False, existing_data: List[Any] = None,
                      is_primary_key: bool = False, is_nullable: bool = True) -> Any:
        """Genera valores basados en el tipo de dato REAL de PostgreSQL"""
        
        # MODIFICACIÓN PRINCIPAL: ELIMINAR GENERACIÓN DE NULL
        # Aunque la columna permita NULL, siempre generaremos un valor real
        # para mantener la legibilidad y consistencia de los datos
        
        # PRIMERO: Generar según el tipo de dato PostgreSQL específico
        value = self._generate_by_postgresql_type(data_type, column_name, max_length, is_nullable)
        
        # SEGUNDO: Si es único, hacerlo único
        if is_unique and value is not None:
            value = self._make_unique(value, column_name, data_type)
        
        return value
    
    def _generate_by_postgresql_type(self, data_type: str, column_name: str, max_length: int = None, is_nullable: bool = True) -> Any:
        """Genera valores basados EXCLUSIVAMENTE en el tipo de dato PostgreSQL"""
        data_type_lower = data_type.lower()
        
        # MODIFICACIÓN CRÍTICA: ELIMINAR LA PROBABILIDAD DE NULL
        # Aunque is_nullable sea True, NO generaremos valores NULL
        # Esto asegura que todos los datos sean legibles y consistentes
        
        # GENERACIÓN POR TIPO DE DATO POSTGRESQL REAL
        if 'integer' in data_type_lower or 'int' in data_type_lower:
            # Para enteros, verificar si es edad
            if 'edad' in column_name.lower() or 'age' in column_name.lower():
                return self._generate_age()
            return random.randint(1, 100)
        
        elif 'boolean' in data_type_lower or 'bool' in data_type_lower:
            # PARA BOOLEANOS: SOLO True o False, NUNCA strings
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
        
        elif 'character varying' in data_type_lower or 'varchar' in data_type_lower:
            # Para textos, usar generadores inteligentes según el nombre de columna
            return self._generate_smart_text(column_name, max_length)
        
        elif 'text' in data_type_lower:
            return self._generate_smart_text(column_name, max_length or 100)
        
        elif 'numeric' in data_type_lower or 'decimal' in data_type_lower:
            return round(random.uniform(1.0, 1000.0), 2)
        
        elif 'real' in data_type_lower or 'double precision' in data_type_lower:
            return random.uniform(1.0, 100.0)
        
        elif 'uuid' in data_type_lower:
            import uuid
            return str(uuid.uuid4())
        
        elif 'json' in data_type_lower or 'jsonb' in data_type_lower:
            return {'id': random.randint(1, 100), 'name': f'item_{random.randint(1, 100)}'}
        
        else:
            # Fallback para tipos desconocidos - NUNCA retornar None
            return f"data_{random.randint(1000, 9999)}"
    
    def _generate_smart_text(self, column_name: str, max_length: int = None) -> str:
        """Genera texto inteligente basado en el nombre de la columna"""
        column_name_lower = column_name.lower()
        
        # Detección por patrones de nombres de columna
        if 'email' in column_name_lower:
            value = self._generate_email()
        elif 'name' in column_name_lower or 'nombre' in column_name_lower:
            value = self._generate_name()
        elif 'phone' in column_name_lower or 'telefono' in column_name_lower:
            value = self._generate_phone()
        elif 'address' in column_name_lower or 'direccion' in column_name_lower:
            value = self._generate_address()
        elif 'city' in column_name_lower or 'ciudad' in column_name_lower:
            value = self._generate_city()
        elif 'company' in column_name_lower or 'empresa' in column_name_lower:
            value = self._generate_company()
        elif 'product' in column_name_lower or 'producto' in column_name_lower:
            value = self._generate_product()
        elif 'description' in column_name_lower or 'descripcion' in column_name_lower:
            value = self._generate_description()
        elif 'code' in column_name_lower or 'codigo' in column_name_lower:
            value = self._generate_code()
        elif 'url' in column_name_lower or 'link' in column_name_lower:
            value = self._generate_url()
        elif 'category' in column_name_lower or 'categoria' in column_name_lower:
            value = self._generate_category()
        elif 'status' in column_name_lower or 'estado' in column_name_lower:
            # PARA STATUS: generar texto apropiado, NO booleanos
            value = self._generate_status()
        else:
            # Texto genérico - NUNCA None
            value = f"Valor para {column_name} {random.randint(1, 1000)}"
        
        # Aplicar longitud máxima si existe
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value
    
    def _generate_age(self) -> int:
        """Genera una edad realista"""
        return random.randint(18, 80)
    
    def _make_unique(self, value: Any, column_name: str, data_type: str) -> Any:
        """Asegura que el valor sea único"""
        cache_key = f"{data_type}_{column_name}"
        
        if cache_key not in self.unique_value_tracker:
            self.unique_value_tracker[cache_key] = set()
        
        original_value = value
        attempts = 0
        max_attempts = 100
        
        while value in self.unique_value_tracker[cache_key] and attempts < max_attempts:
            if isinstance(value, (int, float)):
                value = value + random.randint(1, 1000)
            elif isinstance(value, str):
                value = f"{original_value}_{random.randint(1000, 9999)}"
            else:
                value = f"{original_value}_{random.randint(1000, 9999)}"
            attempts += 1
        
        self.unique_value_tracker[cache_key].add(value)
        return value

    # MÉTODOS ESPECÍFICOS DE GENERACIÓN (SOLO PARA TEXTO)
    
    def _generate_email(self) -> str:
        name = random.choice(self.faker_data['names']).lower().replace(' ', '.')
        lastname = random.choice(self.faker_data['lastnames']).lower()
        domain = random.choice(self.faker_data['emails'])
        return f"{name}.{lastname}@{domain}"
    
    def _generate_name(self) -> str:
        return f"{random.choice(self.faker_data['names'])} {random.choice(self.faker_data['lastnames'])}"
    
    def _generate_phone(self) -> str:
        return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
    
    def _generate_address(self) -> str:
        street = random.choice(self.faker_data['streets'])
        number = random.randint(1, 9999)
        return f"{number} {street} Street, {random.choice(self.faker_data['cities'])}"
    
    def _generate_city(self) -> str:
        return random.choice(self.faker_data['cities'])
    
    def _generate_company(self) -> str:
        return random.choice(self.faker_data['companies'])
    
    def _generate_product(self) -> str:
        return random.choice(self.faker_data['products'])
    
    def _generate_description(self) -> str:
        descriptions = [
            "High quality product with warranty",
            "Professional and reliable service",
            "Innovative solution for your needs",
            "Premium item with excellent features",
            "Durable and efficient design",
            "User-friendly interface and functionality"
        ]
        return random.choice(descriptions)
    
    def _generate_code(self) -> str:
        return f"CODE{random.randint(10000, 99999)}"
    
    def _generate_price(self) -> float:
        return round(random.uniform(5.0, 500.0), 2)
    
    def _generate_quantity(self) -> int:
        return random.randint(0, 1000)
    
    def _generate_status(self) -> str:
        """Genera estados de texto para columnas de status (NO booleanos)"""
        statuses = ['active', 'inactive', 'pending', 'completed', 'cancelled', 'processing', 'shipped']
        return random.choice(statuses)
    
    def _generate_boolean(self) -> bool:
        """Genera booleanos REALES (True/False)"""
        return random.choice([True, False])
    
    def _generate_url(self) -> str:
        domains = ['example.com', 'test.org', 'demo.net', 'sample.dev']
        paths = ['', '/page', '/product', '/article', '/blog']
        return f"https://{random.choice(domains)}{random.choice(paths)}"
    
    def _generate_category(self) -> str:
        return random.choice(self.faker_data['categories'])
    
    def _generate_date(self) -> datetime:
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        return start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
    
    def _generate_id(self) -> int:
        """Genera un ID numérico"""
        return random.randint(1, 10000000)
    
    def generate_unique_values(self, data_type: str, column_name: str, count: int, 
                              max_length: int = None, sample_data: List[Any] = None,
                              table_name: str = None) -> List[Any]:
        """Genera valores únicos para evitar duplicados"""
        values = set()
        
        while len(values) < count:
            value = self.generate_value(
                data_type, column_name, max_length, sample_data, table_name, is_unique=True
            )
            if value is not None:  # Solo agregar valores no nulos
                values.add(value)
        
        return list(values)
    
    def clear_cache(self):
        """Limpia la caché de valores generados"""
        self.generated_values_cache.clear()
        self.unique_value_tracker.clear()
        self.sequence_counters.clear()
    
    def get_field_analysis(self, column_name: str, table_name: str, data_type: str, 
                          sample_data: List = None) -> Dict[str, Any]:
        """Obtiene análisis detallado del campo"""
        return self.field_predictor.predict_field_type(column_name, table_name, data_type, sample_data)
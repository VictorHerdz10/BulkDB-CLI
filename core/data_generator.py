import random
import string
from datetime import datetime, timedelta
from typing import Any, List, Dict, Set, Optional
import re
import json
import os
import sys

def resource_path(relative_path: str) -> str:
        """Obtiene la ruta real del recurso, compatible con PyInstaller --onefile"""
        try:
            # PyInstaller almacena recursos en una carpeta temporal (_MEIPASS)
            base_path = sys._MEIPASS
        except AttributeError:
        # Estamos en modo desarrollo
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)
        

class DataGenerator:
    
    def __init__(self, dataset_path: str = "core/datasets/master_dataset.json"):
        # Asegúrate de que esta ruta coincida con la que usaste en datas del .spec
        self.dataset_path = resource_path(dataset_path)
        self.master_dataset = self._load_master_dataset()
        
        self.generated_values_cache: Dict[str, Set[Any]] = {}
        self.unique_value_tracker: Dict[str, Set[Any]] = {}
        self.sequence_counters: Dict[str, int] = {}
        
    def _load_master_dataset(self) -> Dict:
        """Carga el dataset masivo desde archivo JSON"""
        try:
            # Ahora self.dataset_path ya apunta a la ubicación correcta
            if os.path.exists(self.dataset_path):
                with open(self.dataset_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"Advertencia: No se encontró el archivo de dataset en {self.dataset_path}")
        except Exception as e:
            print(f"Error cargando dataset desde {self.dataset_path}: {e}")
        
        # Fallback seguro
        return {"base_values": {}, "generation_patterns": {}, "table_context_hints": {}}
    
    def generate_value(self, data_type: str, column_name: str, max_length: int = None, 
                      sample_data: List[Any] = None, table_name: str = None, 
                      is_unique: bool = False, existing_data: List[Any] = None,
                      is_primary_key: bool = False, is_nullable: bool = True) -> Any:
        """Genera valores inteligentes basados en el dataset masivo"""
        
        # 1. PRIMERO: Buscar en hints contextuales de tabla
        table_context_value = self._generate_from_table_context(table_name, column_name, data_type)
        if table_context_value is not None:
            return self._ensure_uniqueness(table_context_value, column_name, is_unique)
        
        # 2. SEGUNDO: Generación por nombre de columna
        column_based_value = self._generate_from_column_name(column_name, data_type, max_length)
        if column_based_value is not None:
            return self._ensure_uniqueness(column_based_value, column_name, is_unique)
        
        # 3. TERCERO: Generación por tipo de dato PostgreSQL
        data_type_value = self._generate_by_data_type(data_type, column_name, max_length)
        return self._ensure_uniqueness(data_type_value, column_name, is_unique)
    
    def _generate_from_table_context(self, table_name: str, column_name: str, data_type: str) -> Any:
        """Genera valores basados en el contexto de la tabla"""
        if not table_name:
            return None
            
        hints = self.master_dataset.get("table_context_hints", {})
        
        # Buscar coincidencia exacta de tabla
        table_hint = hints.get(table_name.lower())
        if table_hint:
            column_hint = table_hint.get(column_name.lower())
            if column_hint:
                return self._generate_from_hint(column_hint, column_name, data_type)
        
        # Buscar por patrones en nombre de tabla
        for table_pattern, columns in hints.items():
            if table_pattern in table_name.lower():
                column_hint = columns.get(column_name.lower())
                if column_hint:
                    return self._generate_from_hint(column_hint, column_name, data_type)
        
        return None
    
    def _generate_from_hint(self, hint: str, column_name: str, data_type: str) -> Any:
        """Genera valores basados en pistas específicas"""
        if hint == "personal_names":
            return self._get_random_value("personal_names", "spanish_male_first") or self._get_random_value("personal_names", "spanish_female_first")
        elif hint == "email":
            return self._generate_email()
        elif hint == "phone":
            return self._generate_phone()
        elif hint == "date_age_based":
            return self._generate_birth_date()
        elif hint == "timestamp_recent":
            return self._generate_recent_timestamp()
        elif hint == "products_services":
            return self._get_random_product()
        elif hint == "product_description":
            return self._get_random_value("descriptive_content", "product_descriptions")
        elif hint == "price_realistic":
            return self._generate_realistic_price()
        elif hint == "animal_names":
            return self._get_random_value("animals_pets", "animal_names")
        elif hint == "animal_breeds":
            return self._get_random_animal_breed()
        elif hint == "companies":
            return self._get_random_company()
        elif hint == "address_business":
            return self._generate_business_address()
        elif hint == "job_titles":
            return self._get_random_value("professional", "job_titles")
        elif hint == "departments":
            return self._get_random_value("professional", "departments")
        elif hint == "code_sequential":
            return self._generate_sequential_code(column_name)
        elif hint == "order_status":
            return self._get_random_value("system_data", "status_types")
        elif hint == "email_business":
            return self._generate_business_email()
        
        return None
    
    def _generate_from_column_name(self, column_name: str, data_type: str, max_length: int = None) -> Any:
        """Genera valores basados en análisis del nombre de columna"""
        column_lower = column_name.lower()
        
        # Nombres personales
        if any(word in column_lower for word in ['name', 'nombre']):
            return self._generate_personal_name(column_name)
        
        # Contacto
        elif 'email' in column_lower:
            return self._generate_email()
        elif 'phone' in column_lower or 'tel' in column_lower or 'telefono' in column_lower:
            return self._generate_phone()
        elif 'address' in column_lower or 'direccion' in column_lower or 'dirección' in column_lower:
            return self._generate_address()
        
        # Fechas
        elif any(word in column_lower for word in ['date', 'fecha']):
            if 'birth' in column_lower or 'nacimiento' in column_lower:
                return self._generate_birth_date()
            elif 'created' in column_lower or 'creado' in column_lower:
                return self._generate_recent_timestamp()
            else:
                return self._generate_random_date()
        
        # Ubicaciones
        elif any(word in column_lower for word in ['city', 'ciudad']):
            return self._get_random_value("geographic", "international_cities")
        elif any(word in column_lower for word in ['province', 'provincia']):
            return self._get_random_value("geographic", "cuban_provinces")
        elif any(word in column_lower for word in ['country', 'pais', 'país']):
            return self._get_random_value("geographic", "countries")
        
        # Empresas y productos
        elif any(word in column_lower for word in ['company', 'empresa']):
            return self._get_random_company()
        elif any(word in column_lower for word in ['product', 'producto']):
            return self._get_random_product()
        
        # Descripciones
        elif any(word in column_lower for word in ['description', 'descripcion', 'descripción']):
            return self._get_random_value("descriptive_content", "product_descriptions")
        
        # Códigos
        elif any(word in column_lower for word in ['code', 'codigo', 'código']):
            return self._generate_code(column_name)
        
        # Precios
        elif any(word in column_lower for word in ['price', 'precio', 'cost', 'costo']):
            return self._generate_realistic_price()
        
        # Estados
        elif any(word in column_lower for word in ['status', 'estado']):
            return self._get_random_value("system_data", "status_types")
        
        return None
    
    def _generate_by_data_type(self, data_type: str, column_name: str, max_length: int = None) -> Any:
        """Genera valores basados en tipo de dato PostgreSQL"""
        data_type_lower = data_type.lower()
        
        if 'integer' in data_type_lower or 'int' in data_type_lower:
            if 'edad' in column_name.lower() or 'age' in column_name.lower():
                return random.randint(18, 80)
            return random.randint(1, 1000)
        
        elif 'boolean' in data_type_lower or 'bool' in data_type_lower:
            return random.choice([True, False])
        
        elif 'timestamp' in data_type_lower:
            return self._generate_recent_timestamp()
        
        elif 'date' in data_type_lower:
            return self._generate_random_date()
        
        elif 'time' in data_type_lower:
            return timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        
        elif 'character varying' in data_type_lower or 'varchar' in data_type_lower or 'text' in data_type_lower:
            return self._generate_smart_text(column_name, max_length)
        
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
            return f"value_{random.randint(1000, 9999)}"
    
    def _generate_smart_text(self, column_name: str, max_length: int = None) -> str:
        """Genera texto inteligente basado en nombre de columna"""
        # Intentar generar según el tipo de dato que sugiere la columna
        contextual_value = self._generate_from_column_name(column_name, 'text', max_length)
        if contextual_value:
            value = str(contextual_value)
        else:
            value = f"text_value_{random.randint(1000, 9999)}"
        
        if max_length and len(value) > max_length:
            value = value[:max_length]
        
        return value
    
    # MÉTODOS DE GENERACIÓN ESPECÍFICOS
    def _generate_email(self) -> str:
        """Genera email usando patrones del dataset"""
        first_names = self.master_dataset.get("base_values", {}).get("personal_names", {}).get("spanish_male_first", []) + \
                     self.master_dataset.get("base_values", {}).get("personal_names", {}).get("spanish_female_first", [])
        last_names = self.master_dataset.get("base_values", {}).get("personal_names", {}).get("spanish_last_names", [])
        domains = self.master_dataset.get("base_values", {}).get("contact_info", {}).get("email_domains", [])
        
        if not first_names or not last_names or not domains:
            return f"user{random.randint(1000, 9999)}@example.com"
        
        first = random.choice(first_names).lower().replace(' ', '')
        last = random.choice(last_names).lower().replace(' ', '')
        domain = random.choice(domains)
        
        patterns = self.master_dataset.get("generation_patterns", {}).get("email", {}).get("formats", [])
        pattern = random.choice(patterns) if patterns else "{first}.{last}@{domain}"
        
        return pattern.format(
            first=first,
            last=last,
            first_initial=first[0],
            domain=domain
        )
    
    def _generate_phone(self) -> str:
        """Genera números de teléfono"""
        patterns = self.master_dataset.get("generation_patterns", {}).get("phone", {})
        
        if random.choice([True, False]) and patterns.get("cuba_formats"):
            pattern = random.choice(patterns["cuba_formats"])
            digits = ''.join([str(random.randint(0, 9)) for _ in range(8)])
            return pattern.format(**{"8digits": digits})
        elif patterns.get("international_formats"):
            pattern = random.choice(patterns["international_formats"])
            return pattern.format(**{  
                "3digits": ''.join([str(random.randint(0, 9)) for _ in range(3)]),
                "4digits": ''.join([str(random.randint(0, 9)) for _ in range(4)]),
                "9digits": ''.join([str(random.randint(0, 9)) for _ in range(9)]),
                "10digits": ''.join([str(random.randint(0, 9)) for _ in range(10)]),
                "11digits": ''.join([str(random.randint(0, 9)) for _ in range(11)])
            })
        else:
            return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
    
    def _generate_address(self) -> str:
        """Genera direcciones"""
        patterns = self.master_dataset.get("generation_patterns", {}).get("address", {})
        
        if random.choice([True, False]) and patterns.get("cuba_formats"):
            pattern = random.choice(patterns["cuba_formats"])
            return pattern.format(
                street=self._get_random_value("geographic", "cuban_streets") or "Calle Unknown",
                number=random.randint(1, 9999),
                street1=self._get_random_value("geographic", "cuban_streets") or "Calle A",
                street2=self._get_random_value("geographic", "cuban_streets") or "Calle B",
                municipality=self._get_random_value("geographic", "cuban_municipalities") or "Municipio",
                province=self._get_random_value("geographic", "cuban_provinces") or "Provincia"
            )
        elif patterns.get("international_formats"):
            pattern = random.choice(patterns["international_formats"])
            return pattern.format(
                number=random.randint(1, 9999),
                street=self._get_random_value("geographic", "cuban_streets") or "Street",
                city=self._get_random_value("geographic", "international_cities") or "City",
                country=self._get_random_value("geographic", "countries") or "Country"
            )
        else:
            return f"{random.randint(1, 9999)} Main Street, City, Country"
    
    def _generate_personal_name(self, column_name: str) -> str:
        """Genera nombres personales"""
        first_names_male = self._get_random_value("personal_names", "spanish_male_first")
        first_names_female = self._get_random_value("personal_names", "spanish_female_first")
        last_names = self._get_random_value("personal_names", "spanish_last_names")
        
        if 'first' in column_name.lower() or 'nombre' in column_name.lower():
            return random.choice([first_names_male, first_names_female])
        elif 'last' in column_name.lower() or 'apellido' in column_name.lower():
            return last_names
        else:
            first = random.choice([first_names_male, first_names_female])
            return f"{first} {last_names}"
    
    def _generate_birth_date(self) -> datetime:
        """Genera fecha de nacimiento realista"""
        end_date = datetime.now() - timedelta(days=365*18)
        start_date = end_date - timedelta(days=365*62)
        random_days = random.randint(0, (end_date - start_date).days)
        return (start_date + timedelta(days=random_days)).date()
    
    def _generate_recent_timestamp(self) -> datetime:
        """Genera timestamp reciente"""
        start_date = datetime.now() - timedelta(days=365*2)
        random_seconds = random.randint(0, int((datetime.now() - start_date).total_seconds()))
        return start_date + timedelta(seconds=random_seconds)
    
    def _generate_random_date(self) -> datetime:
        """Genera fecha aleatoria"""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2023, 12, 31)
        random_days = random.randint(0, (end_date - start_date).days)
        return (start_date + timedelta(days=random_days)).date()
    
    def _generate_realistic_price(self) -> float:
        """Genera precio realista"""
        return round(random.uniform(5.0, 500.0), 2)
    
    def _generate_code(self, column_name: str) -> str:
        """Genera códigos"""
        code_patterns = self.master_dataset.get("base_values", {}).get("system_data", {}).get("codes", {})
        
        for code_type, patterns in code_patterns.items():
            if code_type in column_name.lower():
                pattern = random.choice(patterns)
                return self._fill_pattern(pattern)
        
        return f"CODE{random.randint(10000, 99999)}"
    
    def _generate_sequential_code(self, column_name: str) -> str:
        """Genera códigos secuenciales"""
        if column_name not in self.sequence_counters:
            self.sequence_counters[column_name] = 0
        
        self.sequence_counters[column_name] += 1
        return f"{column_name.upper()}-{self.sequence_counters[column_name]:06d}"
    
    def _generate_business_email(self) -> str:
        """Genera email empresarial"""
        companies = self._get_random_company()
        domains = self.master_dataset.get("base_values", {}).get("contact_info", {}).get("email_domains", ["company.com"])
        
        company_name = companies.split()[0].lower() if companies else "company"
        domain = random.choice([d for d in domains if 'company' in d or 'business' in d] or domains)
        
        return f"info@{company_name}.{domain.split('.')[-1]}"
    
    def _generate_business_address(self) -> str:
        """Genera dirección empresarial"""
        companies = self._get_random_company()
        cities = self._get_random_value("geographic", "international_cities")
        
        return f"Oficinas Centrales {companies}, {cities}"
    
    # MÉTODOS AUXILIARES PARA ACCEDER AL DATASET
    def _get_random_value(self, category: str, subcategory: str) -> Any:
        """Obtiene un valor aleatorio del dataset"""
        category_data = self.master_dataset.get("base_values", {}).get(category, {})
        values = category_data.get(subcategory, [])
        return random.choice(values) if values else None
    
    def _get_random_company(self) -> str:
        """Obtiene empresa aleatoria"""
        companies_data = self.master_dataset.get("base_values", {}).get("companies", {})
        all_companies = []
        for company_type in companies_data.values():
            if isinstance(company_type, list):
                all_companies.extend(company_type)
        return random.choice(all_companies) if all_companies else "Default Company"
    
    def _get_random_product(self) -> str:
        """Obtiene producto aleatorio"""
        products_data = self.master_dataset.get("base_values", {}).get("products_services", {})
        all_products = []
        for product_type in products_data.values():
            if isinstance(product_type, list):
                all_products.extend(product_type)
        return random.choice(all_products) if all_products else "Default Product"
    
    def _get_random_animal_breed(self) -> str:
        """Obtiene raza de animal aleatoria"""
        animals_data = self.master_dataset.get("base_values", {}).get("animals_pets", {})
        all_breeds = []
        for breed_type in ['dog_breeds', 'cat_breeds']:
            breeds = animals_data.get(breed_type, [])
            all_breeds.extend(breeds)
        return random.choice(all_breeds) if all_breeds else "Mixed Breed"
    
    def _fill_pattern(self, pattern: str) -> str:
        """Rellena patrones con datos aleatorios"""
        result = pattern
        result = re.sub(r'\{(\d+)digits\}', lambda m: ''.join([str(random.randint(0, 9)) for _ in range(int(m.group(1)))]), result)
        result = re.sub(r'\{(\d+)letters\}', lambda m: ''.join([random.choice(string.ascii_uppercase) for _ in range(int(m.group(1)))]), result)
        result = re.sub(r'\{(\d+)alphanum\}', lambda m: ''.join([random.choice(string.ascii_uppercase + string.digits) for _ in range(int(m.group(1)))]), result)
        return result
    
    def _ensure_uniqueness(self, value: Any, column_name: str, is_unique: bool) -> Any:
        """Asegura valores únicos si es necesario"""
        if not is_unique:
            return value
        
        if column_name not in self.unique_value_tracker:
            self.unique_value_tracker[column_name] = set()
        
        original_value = value
        attempts = 0
        while value in self.unique_value_tracker[column_name] and attempts < 100:
            if isinstance(value, (int, float)):
                value = value + random.randint(1, 1000)
            elif isinstance(value, str):
                value = f"{original_value}_{random.randint(1000, 9999)}"
            else:
                value = f"{original_value}_{random.randint(1000, 9999)}"
            attempts += 1
        
        self.unique_value_tracker[column_name].add(value)
        return value
    
    def generate_unique_values(self, data_type: str, column_name: str, count: int, 
                              max_length: int = None, sample_data: List[Any] = None,
                              table_name: str = None) -> List[Any]:
        """Genera valores únicos para evitar duplicados"""
        values = set()
        
        while len(values) < count:
            value = self.generate_value(
                data_type, column_name, max_length, sample_data, table_name, is_unique=True
            )
            if value is not None:
                values.add(value)
        
        return list(values)
    
    def clear_cache(self):
        """Limpia la caché de valores generados"""
        self.generated_values_cache.clear()
        self.unique_value_tracker.clear()
        self.sequence_counters.clear()
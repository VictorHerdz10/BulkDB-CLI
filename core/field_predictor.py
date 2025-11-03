# core/field_predictor.py
import re
from typing import Dict, List, Any, Optional
import random

class FieldPredictor:
    def __init__(self):
        self.field_patterns = self._initialize_advanced_patterns()
    
    def _initialize_advanced_patterns(self) -> Dict[str, Dict]:
        """Patrones avanzados para predicción de campos - más genéricos"""
        return {
            'personal_name': {
                'patterns': [
                    r'.*name.*', r'.*nombre.*', r'.*person.*', 
                    r'.*first.*', r'.*last.*', r'.*full.*',
                    r'.*fname.*', r'.*lname.*', r'.*username.*',
                    r'.*contact.*', r'.*individual.*'
                ],
                'context_keywords': ['user', 'customer', 'client', 'person', 'contact', 'individual', 'people'],
                'data_types': ['character varying', 'varchar', 'text', 'string'],
                'generator': 'name'
            },
            'email': {
                'patterns': [
                    r'.*email.*', r'.*mail.*', r'.*e-mail.*',
                    r'.*correo.*', r'.*contact.*mail.*'
                ],
                'context_keywords': ['user', 'customer', 'contact', 'login', 'account', 'notification'],
                'data_types': ['character varying', 'varchar', 'text', 'string'],
                'generator': 'email'
            },
            'phone': {
                'patterns': [
                    r'.*phone.*', r'.*tel.*', r'.*mobile.*',
                    r'.*cell.*', r'.*contact.*number.*',
                    r'.*telefono.*', r'.*movil.*', r'.*celular.*'
                ],
                'context_keywords': ['contact', 'customer', 'user', 'emergency', 'support'],
                'data_types': ['character varying', 'varchar', 'text', 'string'],
                'generator': 'phone'
            },
            'address': {
                'patterns': [
                    r'.*address.*', r'.*addr.*', r'.*street.*',
                    r'.*location.*', r'.*direccion.*', r'.*domicilio.*',
                    r'.*city.*', r'.*zip.*', r'.*postal.*'
                ],
                'context_keywords': ['shipping', 'billing', 'customer', 'user', 'location', 'delivery'],
                'data_types': ['character varying', 'varchar', 'text', 'string'],
                'generator': 'address'
            },
            'date': {
                'patterns': [
                    r'.*date.*', r'.*time.*', r'.*created.*',
                    r'.*updated.*', r'.*modified.*', r'.*timestamp.*',
                    r'.*birth.*', r'.*dob.*', r'.*fecha.*'
                ],
                'context_keywords': ['create', 'update', 'modify', 'birth', 'start', 'end', 'expire'],
                'data_types': ['date', 'timestamp', 'datetime', 'time'],
                'generator': 'date'
            },
            'price': {
                'patterns': [
                    r'.*price.*', r'.*cost.*', r'.*amount.*',
                    r'.*value.*', r'.*total.*', r'.*subtotal.*',
                    r'.*precio.*', r'.*costo.*', r'.*monto.*'
                ],
                'context_keywords': ['product', 'order', 'invoice', 'payment', 'financial', 'money'],
                'data_types': ['numeric', 'decimal', 'money', 'real', 'double precision'],
                'generator': 'price'
            },
            'quantity': {
                'patterns': [
                    r'.*quantity.*', r'.*qty.*', r'.*amount.*',
                    r'.*stock.*', r'.*inventory.*', r'.*count.*',
                    r'.*cantidad.*', r'.*stock.*', r'.*inventario.*'
                ],
                'context_keywords': ['product', 'order', 'inventory', 'stock', 'item'],
                'data_types': ['integer', 'bigint', 'smallint', 'numeric'],
                'generator': 'quantity'
            },
            'status': {
                'patterns': [
                    r'.*status.*', r'.*state.*', r'.*flag.*',
                    r'.*active.*', r'.*enabled.*', r'.*estado.*'
                ],
                'context_keywords': ['user', 'order', 'product', 'system', 'account'],
                'data_types': ['boolean', 'character varying', 'varchar', 'text', 'integer'],
                'generator': 'status'
            },
            'description': {
                'patterns': [
                    r'.*desc.*', r'.*detail.*', r'.*note.*',
                    r'.*comment.*', r'.*info.*', r'.*description.*'
                ],
                'context_keywords': ['product', 'item', 'service', 'general', 'additional'],
                'data_types': ['character varying', 'varchar', 'text'],
                'generator': 'description'
            },
            'code': {
                'patterns': [
                    r'.*code.*', r'.*sku.*', r'.*id.*',
                    r'.*reference.*', r'.*ref.*', r'.*codigo.*'
                ],
                'context_keywords': ['product', 'item', 'order', 'customer', 'unique'],
                'data_types': ['character varying', 'varchar', 'text'],
                'generator': 'code'
            },
            'url': {
                'patterns': [
                    r'.*url.*', r'.*link.*', r'.*website.*',
                    r'.*web.*', r'.*uri.*', r'.*site.*'
                ],
                'context_keywords': ['website', 'social', 'profile', 'resource', 'online'],
                'data_types': ['character varying', 'varchar', 'text'],
                'generator': 'url'
            },
            'category': {
                'patterns': [
                    r'.*category.*', r'.*type.*', r'.*kind.*',
                    r'.*group.*', r'.*class.*', r'.*categoria.*'
                ],
                'context_keywords': ['product', 'item', 'content', 'classification'],
                'data_types': ['character varying', 'varchar', 'text', 'integer'],
                'generator': 'category'
            }
        }
    
    def predict_field_type(self, column_name: str, table_name: str, data_type: str, 
                          sample_data: List = None, existing_data: List = None) -> Dict[str, Any]:
        """Predice el tipo de campo basado en múltiples factores"""
        
        # Análisis por nombre de columna
        column_analysis = self._analyze_column_name(column_name.lower())
        
        # Análisis por contexto de tabla
        table_analysis = self._analyze_table_context(table_name.lower())
        
        # Análisis por tipo de dato
        data_type_analysis = self._analyze_data_type(data_type.lower())
        
        # Análisis por datos de muestra
        sample_analysis = self._analyze_sample_data(sample_data) if sample_data else {}
        
        # Análisis por datos existentes (si hay)
        existing_analysis = self._analyze_existing_data(existing_data) if existing_data else {}
        
        # Combinar todos los análisis
        combined_score = {}
        analyses = [column_analysis, table_analysis, data_type_analysis, sample_analysis, existing_analysis]
        
        for analysis in analyses:
            for field_type, score in analysis.items():
                combined_score[field_type] = combined_score.get(field_type, 0) + score
        
        # Aplicar pesos diferentes según la confiabilidad de cada análisis
        weighted_score = {}
        for field_type, score in combined_score.items():
            # El análisis de nombre de columna es más confiable
            name_score = column_analysis.get(field_type, 0)
            # El análisis de datos existentes es muy confiable
            existing_score = existing_analysis.get(field_type, 0)
            
            weighted_score[field_type] = (name_score * 0.4) + (score * 0.3) + (existing_score * 0.3)
        
        # Normalizar scores
        total = sum(weighted_score.values())
        if total > 0:
            for field_type in weighted_score:
                weighted_score[field_type] = weighted_score[field_type] / total
        
        # Obtener mejor predicción
        best_prediction = max(weighted_score.items(), key=lambda x: x[1]) if weighted_score else ('unknown', 0)
        
        return {
            'predicted_type': best_prediction[0],
            'confidence': best_prediction[1],
            'all_scores': weighted_score,
            'recommended_generator': self.field_patterns.get(best_prediction[0], {}).get('generator', 'default')
        }
    
    def _analyze_column_name(self, column_name: str) -> Dict[str, float]:
        """Analiza el nombre de la columna"""
        scores = {}
        
        for field_type, config in self.field_patterns.items():
            score = 0
            
            # Verificar patrones regex en el nombre de columna
            for pattern in config['patterns']:
                if re.match(pattern, column_name, re.IGNORECASE):
                    score += 0.7  # Alto peso para coincidencia exacta
                    break
            
            # Verificar palabras clave en el nombre
            for keyword in config.get('context_keywords', []):
                if keyword in column_name:
                    score += 0.3
            
            if score > 0:
                scores[field_type] = score
        
        return scores
    
    def _analyze_table_context(self, table_name: str) -> Dict[str, float]:
        """Analiza el contexto de la tabla de manera genérica"""
        scores = {}
        
        # Patrones de tablas comunes en cualquier base de datos
        table_patterns = {
            'user': ['personal_name', 'email', 'phone', 'address', 'status'],
            'customer': ['personal_name', 'email', 'phone', 'address', 'status'],
            'client': ['personal_name', 'email', 'phone', 'address'],
            'person': ['personal_name', 'email', 'phone', 'address'],
            'product': ['product_name', 'price', 'description', 'category', 'code'],
            'item': ['product_name', 'price', 'description', 'category', 'code'],
            'order': ['order_date', 'quantity', 'price', 'status', 'total'],
            'invoice': ['invoice_date', 'amount', 'total', 'status'],
            'employee': ['personal_name', 'email', 'phone', 'salary'],
            'payment': ['amount', 'date', 'status', 'method'],
            'category': ['category_name', 'description'],
            'address': ['street', 'city', 'zip_code', 'country']
        }
        
        for table_pattern, field_types in table_patterns.items():
            if table_pattern in table_name.lower():
                for field_type in field_types:
                    scores[field_type] = scores.get(field_type, 0) + 0.4
        
        return scores
    
    def _analyze_data_type(self, data_type: str) -> Dict[str, float]:
        """Analiza el tipo de dato"""
        scores = {}
        
        for field_type, config in self.field_patterns.items():
            compatible_types = config.get('data_types', [])
            if any(tipo in data_type for tipo in compatible_types):
                scores[field_type] = scores.get(field_type, 0) + 0.5
        
        return scores
    
    def _analyze_sample_data(self, sample_data: List) -> Dict[str, float]:
        """Analiza datos de muestra"""
        scores = {}
        
        if not sample_data:
            return scores
        
        first_value = sample_data[0] if sample_data else None
        
        if isinstance(first_value, str):
            # Detectar emails
            if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', first_value):
                scores['email'] = 0.9
            
            # Detectar URLs
            if re.match(r'^https?://[^\s/$.?#].[^\s]*$', first_value, re.IGNORECASE):
                scores['url'] = 0.8
            
            # Detectar nombres (contiene espacios y letras)
            if ' ' in first_value and any(c.isalpha() for c in first_value) and len(first_value) > 3:
                scores['personal_name'] = 0.7
            
            # Detectar códigos (mezcla de letras y números)
            if any(c.isdigit() for c in first_value) and any(c.isalpha() for c in first_value):
                scores['code'] = 0.6
        
        elif isinstance(first_value, (int, float)):
            # Para números, verificar rangos típicos
            if isinstance(first_value, int):
                if 0 <= first_value <= 1000:
                    scores['quantity'] = 0.7
                elif 1 <= first_value <= 100:
                    scores['status'] = 0.5
            elif isinstance(first_value, float):
                if first_value > 10:
                    scores['price'] = 0.8
        
        elif isinstance(first_value, bool):
            scores['status'] = 0.9
        
        return scores
    
    def _analyze_existing_data(self, existing_data: List) -> Dict[str, float]:
        """Analiza datos existentes en la base de datos"""
        scores = {}
        
        if not existing_data:
            return scores
        
        # Contar valores únicos y patrones
        unique_values = set(existing_data)
        
        # Si hay muchos valores únicos, podría ser un código o nombre
        uniqueness_ratio = len(unique_values) / len(existing_data)
        
        if uniqueness_ratio > 0.8:
            first_val = existing_data[0]
            if isinstance(first_val, str):
                if '@' in str(first_val):
                    scores['email'] = 0.9
                else:
                    scores['code'] = 0.7
                    scores['personal_name'] = 0.6
        
        # Si hay pocos valores únicos, podría ser categoría o status
        elif uniqueness_ratio < 0.3:
            scores['category'] = 0.7
            scores['status'] = 0.6
        
        return scores
    
    def get_field_recommendations(self, column_name: str, table_name: str, data_type: str) -> List[Dict[str, Any]]:
        """Obtiene recomendaciones ordenadas por confianza"""
        prediction = self.predict_field_type(column_name, table_name, data_type)
        
        recommendations = []
        for field_type, confidence in sorted(prediction['all_scores'].items(), key=lambda x: x[1], reverse=True):
            if confidence > 0.1:  # Solo mostrar recomendaciones con al menos 10% de confianza
                recommendations.append({
                    'field_type': field_type,
                    'confidence': confidence,
                    'generator': self.field_patterns.get(field_type, {}).get('generator', 'default')
                })
        
        return recommendations[:5]  # Top 5 recomendaciones
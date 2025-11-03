import psycopg2
from psycopg2 import sql
from typing import List, Dict, Any, Tuple, Optional
import random
import time
from core.advanced_relationships import RelationshipManager

class DatabasePopulator:
    def __init__(self, database_manager, schema_analyzer, data_generator, progress_manager=None):
        self.db = database_manager
        self.analyzer = schema_analyzer
        self.generator = data_generator
        self.progress = progress_manager
        self.rel_manager = RelationshipManager(self.db, self.analyzer)
        self.column_configs = {}
    
    def set_column_configs(self, column_configs: Dict):
        """Establece la configuración de columnas"""
        self.column_configs = column_configs
    
    def populate_table(self, table_name: str, record_count: int, selected_columns: List[str], batch_size: int = 1000) -> Tuple[int, int]:
        """Pobla una tabla con datos de prueba usando columnas seleccionadas"""
        success_count = 0
        error_count = 0
        
        # Mostrar panel de inicialización si hay ProgressManager
        if self.progress:
            self.progress.show_initialization_panel(table_name, record_count)
        
        # Obtener información completa de columnas
        all_columns_info = self.db.get_table_columns(table_name)
        selected_columns_info = [col for col in all_columns_info if col[0] in selected_columns]
        
        # Analizar relaciones avanzadas solo para columnas seleccionadas
        all_relationships = self.rel_manager.analyze_advanced_relationships(table_name)
        relevant_relationships = {col: rel for col, rel in all_relationships.items() if col in selected_columns}
        
        # Preparar datos para cada columna seleccionada
        column_data = self._prepare_column_data(selected_columns_info, relevant_relationships, table_name)
        
        # Generar datos en lotes para mejor performance
        batches = self._generate_data_in_batches(column_data, record_count, batch_size, table_name, selected_columns)
        
        # Insertar datos con barra de progreso mejorada
        total_batches = len(batches)
        
        if self.progress:
            # Usar ProgressManager para mostrar progreso detallado
            with self.progress.create_progress() as progress:
                task = progress.add_task("📥 Insertando registros...", total=total_batches)
                
                for i, batch in enumerate(batches):
                    batch_success, batch_errors = self._insert_batch(table_name, batch, selected_columns)
                    success_count += batch_success
                    error_count += batch_errors
                    
                    progress.update(task, advance=1, 
                                  description=f"📥 Lote {i+1}/{total_batches} - Éxitos: {success_count}")
                    
                    # Pequeña pausa para no saturar la base de datos
                    time.sleep(0.01)
        else:
            # Fallback a progreso básico si no hay ProgressManager
            print(f"\n📥 Insertando {record_count} registros en {total_batches} lotes...")
            for i, batch in enumerate(batches):
                batch_success, batch_errors = self._insert_batch(table_name, batch, selected_columns)
                success_count += batch_success
                error_count += batch_errors
                print(f"   ✅ Lote {i+1}/{total_batches} completado - Éxitos: {success_count}, Errores: {error_count}")
                
                time.sleep(0.01)
        
        return success_count, error_count
    
    def _prepare_column_data(self, columns_info: List, advanced_relationships: Dict, table_name: str) -> Dict[str, Any]:
        """Prepara la configuración de datos para cada columna seleccionada"""
        column_data = {}
        
        for col_info in columns_info:
            col_name, data_type, is_nullable, default, max_length, numeric_precision, numeric_scale = col_info
            
            # Obtener datos de ejemplo existentes para análisis
            sample_data = self.db.get_column_sample_data(table_name, col_name, limit=5)
            
            # Verificar si es una relación foránea
            is_foreign_key = col_name in advanced_relationships
            
            if is_foreign_key:
                rel_info = advanced_relationships[col_name]
                foreign_values = self.rel_manager.generate_relationship_data(rel_info, 1000)
                
                if foreign_values:
                    column_data[col_name] = {
                        'type': 'foreign_key',
                        'values': foreign_values,
                        'data_type': data_type,
                        'is_nullable': is_nullable,
                        'max_length': max_length,
                        'sample_data': sample_data
                    }
                else:
                    column_data[col_name] = {
                        'type': 'generated',
                        'data_type': data_type,
                        'max_length': max_length,
                        'is_nullable': is_nullable,
                        'sample_data': sample_data
                    }
            else:
                # Aplicar configuración personalizada si existe
                if col_name in self.column_configs:
                    config = self.column_configs[col_name]
                    column_data[col_name] = {
                        'type': 'configured',
                        'data_type': data_type,
                        'max_length': max_length,
                        'is_nullable': is_nullable,
                        'sample_data': sample_data,
                        'config': config
                    }
                else:
                    # Columna normal - generar datos
                    column_data[col_name] = {
                        'type': 'generated',
                        'data_type': data_type,
                        'max_length': max_length,
                        'is_nullable': is_nullable,
                        'sample_data': sample_data
                    }
        
        return column_data
    
    def _generate_data_in_batches(self, column_data: Dict, total_records: int, batch_size: int, table_name: str, selected_columns: List[str]) -> List[List[Dict]]:
        """Genera datos en lotes para mejor performance"""
        batches = []
        records_generated = 0
        
        if self.progress:
            with self.progress.create_progress() as progress:
                task = progress.add_task("🔄 Generando datos...", total=total_records)
                
                while records_generated < total_records:
                    current_batch_size = min(batch_size, total_records - records_generated)
                    batch = []
                    
                    for i in range(current_batch_size):
                        record = {}
                        for col_name, col_config in column_data.items():
                            record[col_name] = self._generate_column_value(col_config, col_name, i, records_generated + i)
                        batch.append(record)
                    
                    batches.append(batch)
                    records_generated += current_batch_size
                    progress.update(task, advance=current_batch_size, 
                                  description=f"🔄 Generados {records_generated}/{total_records} registros")
        else:
            print(f"🔄 Generando {total_records} registros...")
            while records_generated < total_records:
                current_batch_size = min(batch_size, total_records - records_generated)
                batch = []
                
                for i in range(current_batch_size):
                    record = {}
                    for col_name, col_config in column_data.items():
                        record[col_name] = self._generate_column_value(col_config, col_name, i, records_generated + i)
                    batch.append(record)
                
                batches.append(batch)
                records_generated += current_batch_size
                print(f"   ✅ Generados {records_generated}/{total_records} registros")
        
        return batches
    
    def _generate_column_value(self, col_config: Dict, col_name: str, record_index: int, global_index: int) -> Any:
        """Genera un valor para una columna específica usando la configuración"""
        # Considerar nulabilidad (10% de chance de NULL si es nullable)
        if col_config['is_nullable'] == 'YES' and random.random() < 0.1:
            return None
        
        # Aplicar configuración personalizada si existe
        if col_config['type'] == 'configured':
            config = col_config['config']
            generation_type = config.get('generation_type', 'random')
            
            if generation_type == 'fixed':
                return config.get('fixed_value')
            elif generation_type == 'sequence' and 'int' in col_config['data_type']:
                return config.get('start_value', 1) + global_index
            elif generation_type == 'true':
                return True
            elif generation_type == 'false':
                return False
        
        # Lógica para relaciones foráneas
        if col_config['type'] == 'foreign_key':
            if col_config['values']:
                return random.choice(col_config['values'])
        
        # Generación normal
        return self.generator.generate_value(
            col_config['data_type'],
            col_name,
            col_config.get('max_length'),
            col_config.get('sample_data')
        )
    
    def _insert_batch(self, table_name: str, batch: List[Dict], selected_columns: List[str]) -> Tuple[int, int]:
        """Inserta un lote de registros en la base de datos"""
        if not batch:
            return 0, 0
        
        success_count = 0
        error_count = 0
        
        try:
            # Construir consulta INSERT dinámica solo con columnas seleccionadas
            columns = selected_columns
            placeholders = ', '.join(['%s'] * len(columns))
            
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(placeholders)
            )
            
            # Preparar valores para cada registro
            values_list = []
            for record in batch:
                values = []
                for col in columns:
                    value = record.get(col)
                    
                    # Manejar valores NULL de forma explícita
                    if value is None:
                        values.append(None)
                    else:
                        values.append(value)
                
                values_list.append(tuple(values))
            
            # Ejecutar inserción masiva
            self.db.cursor.executemany(query, values_list)
            self.db.connection.commit()
            
            success_count = len(batch)
            
        except psycopg2.Error as e:
            self.db.connection.rollback()
            error_count = len(batch)
            if self.progress:
                self.progress.console.print(f"❌ Error insertando lote: {e}")
            else:
                print(f"❌ Error insertando lote: {e}")
            
            # Intentar inserción individual para diagnóstico
            individual_success, individual_errors = self._insert_records_individually(table_name, batch, selected_columns)
            success_count += individual_success
            error_count = individual_errors
        
        return success_count, error_count
    
    def _insert_records_individually(self, table_name: str, batch: List[Dict], selected_columns: List[str]) -> Tuple[int, int]:
        """Inserta registros uno por uno (fallback para diagnóstico)"""
        success_count = 0
        error_count = 0
        
        for i, record in enumerate(batch):
            try:
                columns = selected_columns
                placeholders = ', '.join(['%s'] * len(columns))
                
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(placeholders)
                )
                
                values = []
                for col in columns:
                    value = record.get(col)
                    values.append(value)
                
                self.db.cursor.execute(query, values)
                self.db.connection.commit()
                success_count += 1
                
            except psycopg2.Error as e:
                self.db.connection.rollback()
                error_count += 1
                print(f"❌ Error en registro {i}: {e}")
                print(f"   Valores: {record}")
        
        return success_count, error_count
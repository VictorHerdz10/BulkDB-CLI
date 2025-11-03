import questionary
from typing import List, Dict, Any, Optional
from core.database import DatabaseManager
from core.schema_analyzer import SchemaAnalyzer
from core.data_generator import DataGenerator
from core.validators import DataValidator
from utils.config import ConfigManager
import os
import json
from datetime import datetime

class CLIInterface:
    def __init__(self, config_manager: ConfigManager = None):
        self.db = DatabaseManager()
        self.analyzer = None
        self.generator = DataGenerator()
        self.config = config_manager or ConfigManager()
        self.saved_connections = self.config.get_connections()
        self.current_connection_string = None
        self.selected_columns = []
    
    def manage_connections(self) -> str:
        """Gestiona conexiones guardadas y permite seleccionar una"""
        print("\n" + "="*50)
        print("🔗 GESTIÓN DE CONEXIONES A POSTGRESQL")
        print("="*50)
        
        # Mostrar conexiones guardadas si existen
        if self.saved_connections:
            choices = [
                questionary.Choice(title=f"🔗 {name} - {conn[:30]}...", value=(name, conn))
                for name, conn in self.saved_connections.items()
            ]
            choices.extend([
                questionary.Separator(),
                questionary.Choice(title="➕ Agregar nueva conexión", value="__new__"),
                questionary.Choice(title="🗑️  Gestionar conexiones guardadas", value="__manage__")
            ])
            
            selected = questionary.select(
                "Selecciona una conexión guardada:",
                choices=choices
            ).ask()
            
            if selected and selected != "__new__" and selected != "__manage__":
                name, connection_string = selected
                self.current_connection_string = connection_string
                # Establecer la conexión seleccionada
                self._establish_connection(connection_string)
                return connection_string
            elif selected == "__manage__":
                self._manage_saved_connections()
                return self.manage_connections()
        
        # Si no hay conexiones guardadas o se eligió nueva conexión
        return self.get_connection_string()
    
    def _establish_connection(self, connection_string: str):
        """Establece la conexión a la base de datos"""
        is_valid, message = self.db.validate_connection(connection_string)
        if is_valid:
            self.analyzer = SchemaAnalyzer(self.db)
            print(f"✅ {message}")
        else:
            print(f"❌ {message}")
            raise ConnectionError(f"No se pudo establecer la conexión: {message}")
    
    def _manage_saved_connections(self):
        """Gestiona las conexiones guardadas"""
        while True:
            choices = [
                questionary.Choice(title=f"🔗 {name} - {conn[:30]}...", value=name)
                for name, conn in self.saved_connections.items()
            ]
            
            if choices:
                choices.extend([
                    questionary.Separator(),
                    questionary.Choice(title="💾 Guardar conexión actual", value="__save__"),
                    questionary.Choice(title="⬅️  Volver", value="__back__")
                ])
            else:
                choices = [
                    questionary.Choice(title="💾 Guardar conexión actual", value="__save__"),
                    questionary.Choice(title="⬅️  Volver", value="__back__")
                ]
            
            action = questionary.select(
                "Gestionar conexiones guardadas:",
                choices=choices
            ).ask()
            
            if action == "__back__":
                break
            elif action == "__save__":
                self._save_current_connection()
            else:
                # Eliminar conexión seleccionada
                if questionary.confirm(f"¿Eliminar la conexión '{action}'?").ask():
                    self.config.delete_connection(action)
                    self.saved_connections = self.config.get_connections()
                    print(f"✅ Conexión '{action}' eliminada")
    
    def _save_current_connection(self):
        """Guarda la conexión actual"""
        name = questionary.text(
            "Nombre para esta conexión:",
            validate=lambda x: len(x) > 0 and x not in self.saved_connections
        ).ask()
        
        if name:
            if self.current_connection_string:
                connection_string = self.current_connection_string
            else:
                connection_string = questionary.text(
                    "String de conexión a guardar:",
                    default="postgresql://postgres:12345678@localhost:5432/postgres"
                ).ask()
            
            if connection_string:
                self.config.save_connection(name, connection_string)
                self.saved_connections = self.config.get_connections()
                print(f"✅ Conexión '{name}' guardada")
    
    def get_connection_string(self) -> str:
        """Solicita y valida el string de conexión"""
        print("\n" + "="*50)
        print("🔗 CONFIGURACIÓN DE CONEXIÓN A POSTGRESQL")
        print("="*50)
        
        while True:
            connection_string = questionary.text(
                "Ingresa el string de conexión:",
                default="postgresql://postgres:12345678@localhost:5432/postgres",
                instruction="(formato: postgresql://usuario:contraseña@host:puerto/base_datos)"
            ).ask()
            
            if not connection_string:
                print("❌ Debes ingresar un string de conexión")
                continue
            
            print("🔌 Validando conexión...")
            is_valid, message = self.db.validate_connection(connection_string)
            print(message)
            
            if is_valid:
                self.current_connection_string = connection_string
                self.analyzer = SchemaAnalyzer(self.db)
                
                # Preguntar si quiere guardar la conexión
                if questionary.confirm("¿Quieres guardar esta conexión para uso futuro?").ask():
                    self._save_connection_after_test(connection_string)
                
                return connection_string
            else:
                if not questionary.confirm("¿Quieres intentar con otro string de conexión?").ask():
                    print("👋 Saliendo del programa...")
                    exit()
    
    def _save_connection_after_test(self, connection_string: str):
        """Guarda una conexión después de validarla"""
        name = questionary.text(
            "Nombre para esta conexión:",
            validate=lambda x: len(x) > 0 and x not in self.saved_connections
        ).ask()
        
        if name:
            self.config.save_connection(name, connection_string)
            self.saved_connections = self.config.get_connections()
            print(f"✅ Conexión '{name}' guardada")
    
    def select_table(self) -> str:
        """Permite seleccionar una tabla de la base de datos"""
        try:
            # Verificar que tenemos conexión activa
            if not self.db.connection or self.db.connection.closed:
                raise ConnectionError("No hay conexión activa a la base de datos")
            
            print("\n📊 CARGANDO TABLAS DISPONIBLES...")
            
            # Obtener todas las tablas
            self.db.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in self.db.cursor.fetchall()]
            
            if not tables:
                print("❌ No se encontraron tablas en la base de datos")
                exit()
            
            print(f"✅ Se encontraron {len(tables)} tablas")
            
            selected_table = questionary.select(
                "Selecciona la tabla a poblar:",
                choices=tables,
                instruction="(Usa ↑↓ para navegar, Enter para seleccionar)"
            ).ask()
            
            return selected_table
            
        except Exception as e:
            print(f"❌ Error obteniendo tablas: {e}")
            exit()
    
    def get_insert_count(self) -> int:
        """Solicita la cantidad de registros a insertar"""
        app_config = self.config.get_config()
        default_count = app_config['defaults']['record_count']
        
        count = questionary.text(
            "¿Cuántos registros quieres insertar?",
            default=str(default_count),
            validate=lambda x: x.isdigit() and int(x) > 0 and int(x) <= 100000
        ).ask()
        
        return int(count)
    
    def select_columns(self, table_name: str) -> List[str]:
        """Permite al usuario seleccionar qué columnas poblar, excluyendo IDs auto-incrementales"""
        try:
            print(f"\n📋 CARGANDO COLUMNAS DE: {table_name}")
            
            # Obtener información de columnas
            columns_info = self.db.get_table_columns(table_name)
            
            if not columns_info:
                print("❌ No se encontraron columnas en la tabla")
                return []
            
            # Identificar columnas auto-incrementales y claves primarias para excluirlas
            auto_increment_columns = []
            primary_key = self.db.get_primary_key(table_name)
            
            column_choices = []
            for col in columns_info:
                col_name, data_type, is_nullable, default, max_length, numeric_precision, numeric_scale = col
                
                # Detectar columnas auto-incrementales
                is_auto_increment = (
                    (col_name.lower() == 'id' or col_name == primary_key) and 
                    default is not None and 'nextval' in str(default)
                )
                
                # Detectar columnas ID que probablemente sean auto-incrementales
                is_id_like = (
                    col_name.lower() in ['id', 'uuid', 'pk_id', 'table_id'] or 
                    col_name.endswith('_id') and col_name != primary_key
                )
                
                if is_auto_increment:
                    auto_increment_columns.append(col_name)
                    # Excluir por defecto de la selección
                    checked = False
                    note = " 🔄 (Auto-incremental - Excluida)"
                elif is_id_like and primary_key and col_name != primary_key:
                    # Columnas ID que no son PK - preguntar al usuario
                    checked = True
                    note = " ⚠️ (Posible ID - Verificar)"
                else:
                    checked = True
                    note = ""
                
                # Formatear información de la columna
                nullable_text = "NULL" if is_nullable == 'YES' else "NOT NULL"
                length_text = f"({max_length})" if max_length else ""
                default_text = f" [DEFAULT: {default}]" if default else ""
                
                display_text = f"{col_name}: {data_type}{length_text} [{nullable_text}]{default_text}{note}"
                
                column_choices.append(
                    questionary.Choice(
                        title=display_text,
                        value=col_name,
                        checked=checked
                    )
                )
            
            if auto_increment_columns:
                print(f"💡 Columnas auto-incrementales detectadas y excluidas: {', '.join(auto_increment_columns)}")
                print("   Estas columnas se generan automáticamente por la base de datos")
            
            # Permitir al usuario seleccionar columnas
            selected = questionary.checkbox(
                "Selecciona las columnas a poblar:",
                choices=column_choices,
                instruction="(Usa espacio para seleccionar, Enter para confirmar)"
            ).ask()
            
            if not selected:
                print("❌ Debes seleccionar al menos una columna")
                return self.select_columns(table_name)
            
            # Filtrar columnas auto-incrementales (por si el usuario las incluyó manualmente)
            final_selected = [col for col in selected if col not in auto_increment_columns]
            
            if len(final_selected) != len(selected):
                print(f"💡 Se excluyeron {len(selected) - len(final_selected)} columnas auto-incrementales")
            
            print(f"✅ Columnas seleccionadas: {len(final_selected)}")
            return final_selected
            
        except Exception as e:
            print(f"❌ Error obteniendo columnas: {e}")
            return []
    
    def configure_column_data(self, table_name: str, selected_columns: List[str]) -> Dict[str, Dict]:
        """Permite configurar cómo generar datos para cada columna"""
        print(f"\n⚙️  CONFIGURACIÓN DE DATOS PARA COLUMNAS")
        print("-" * 50)
        
        column_configs = {}
        columns_info = self.db.get_table_columns(table_name)
        columns_dict = {col[0]: col for col in columns_info}
        
        for col_name in selected_columns:
            if col_name not in columns_dict:
                continue
                
            col_info = columns_dict[col_name]
            data_type = col_info[1]
            is_nullable = col_info[2]
            max_length = col_info[4]
            
            print(f"\n📝 Configurando: {col_name} ({data_type})")
            
            # Opciones de generación basadas en el tipo de dato REAL de PostgreSQL
            if 'int' in data_type:
                choices = [
                    questionary.Choice("Números aleatorios", value="random"),
                    questionary.Choice("Secuencia automática", value="sequence"),
                    questionary.Choice("Valor fijo", value="fixed")
                ]
            elif 'char' in data_type or 'text' in data_type:
                choices = [
                    questionary.Choice("Texto aleatorio", value="random"),
                    questionary.Choice("Valor fijo", value="fixed"),
                    questionary.Choice("Basado en nombre de columna", value="smart")
                ]
            elif 'bool' in data_type:
                choices = [
                    questionary.Choice("Valores booleanos aleatorios", value="random"),
                    questionary.Choice("Solo True", value="true"),
                    questionary.Choice("Solo False", value="false")
                ]
            elif 'date' in data_type or 'time' in data_type:
                choices = [
                    questionary.Choice("Fechas aleatorias", value="random"),
                    questionary.Choice("Fecha fija", value="fixed")
                ]
            else:
                choices = [
                    questionary.Choice("Valores aleatorios", value="random"),
                    questionary.Choice("Valor fijo", value="fixed")
                ]
            
            generation_type = questionary.select(
                "Tipo de generación:",
                choices=choices
            ).ask()
            
            config = {
                'data_type': data_type,
                'is_nullable': is_nullable,
                'max_length': max_length,
                'generation_type': generation_type
            }
            
            # Configuraciones adicionales según el tipo
            if generation_type == "fixed":
                if 'int' in data_type:
                    fixed_value = questionary.text(
                        "Valor fijo (número):",
                        validate=lambda x: x.isdigit()
                    ).ask()
                    config['fixed_value'] = int(fixed_value)
                elif 'bool' in data_type:
                    fixed_value = questionary.confirm("¿Valor True?").ask()
                    config['fixed_value'] = fixed_value
                elif 'date' in data_type or 'time' in data_type:
                    fixed_value = questionary.text(
                        "Valor fijo (formato YYYY-MM-DD para fecha, HH:MM:SS para tiempo):"
                    ).ask()
                    config['fixed_value'] = fixed_value
                else:
                    fixed_value = questionary.text("Valor fijo:").ask()
                    config['fixed_value'] = fixed_value
            
            elif generation_type == "sequence" and 'int' in data_type:
                start_value = questionary.text(
                    "Valor inicial:",
                    default="1",
                    validate=lambda x: x.isdigit()
                ).ask()
                config['start_value'] = int(start_value)
            
            column_configs[col_name] = config
        
        return column_configs
    
    def analyze_and_confirm(self, table_name: str, record_count: int, selected_columns: List[str]) -> bool:
        """Analiza la tabla y muestra información al usuario"""
        print(f"\n🔍 ANALIZANDO TABLA: {table_name}")
        print("-" * 40)
        
        # Verificar que la tabla existe
        if not self.db.table_exists(table_name):
            print(f"❌ La tabla '{table_name}' no existe")
            return False
        
        # Validar restricciones de claves foráneas
        validator = DataValidator(self.db)
        fk_valid, fk_warnings, empty_related_tables = validator.validate_foreign_key_constraints(table_name)
        
        # Si hay tablas relacionadas vacías, priorizar su población
        if empty_related_tables:
            print(f"\n🚨 TABLAS RELACIONADAS VACÍAS DETECTADAS:")
            for empty_table in empty_related_tables:
                print(f"   ⚠️  {empty_table} - VACÍA (necesita datos primero)")
            
            print(f"\n💡 RECOMENDACIÓN:")
            print(f"   Debes poblar estas tablas en el siguiente orden:")
            
            priority_order = validator.get_population_priority(table_name)
            for i, tbl in enumerate(priority_order, 1):
                print(f"   {i}. {tbl}")
            
            print(f"\n📋 ACCIÓN REQUERIDA:")
            print(f"   1. Poblar primero: {priority_order[0]}")
            print(f"   2. Luego poblar: {table_name}")
            
            if questionary.confirm(f"¿Quieres cambiar y poblar '{priority_order[0]}' primero?").ask():
                # Cambiar a la tabla prioritaria
                return self.switch_to_priority_table(priority_order[0], table_name)
            else:
                print("❌ No se puede continuar sin poblar las tablas relacionadas vacías")
                return False
        
        # Mostrar columnas seleccionadas
        print(f"📋 Columnas a poblar ({len(selected_columns)}):")
        for col_name in selected_columns:
            print(f"   ├─ {col_name}")
        
        # Obtener información completa de columnas para análisis
        columns_info = self.db.get_table_columns(table_name)
        selected_columns_info = [col for col in columns_info if col[0] in selected_columns]
        
        # Analizar relaciones solo para columnas seleccionadas
        all_relationships = self.analyzer.analyze_relationships(table_name)
        relevant_relationships = [rel for rel in all_relationships if rel['column'] in selected_columns]
        
        if relevant_relationships:
            print(f"\n🔗 Relaciones encontradas en columnas seleccionadas:")
            for rel in relevant_relationships:
                has_data, data_count = self.analyzer.check_foreign_table_data(rel['foreign_table'])
                status = "✅ Con datos" if has_data else "⚠️  VACÍA"
                print(f"   ├─ {rel['column']} → {rel['foreign_table']}.{rel['foreign_column']}")
                print(f"   │  Tipo: {rel['relationship_type']} - {status} ({data_count} registros)")
        else:
            print(f"\n🔗 No se encontraron relaciones foráneas en las columnas seleccionadas")
        
        # Mostrar resumen
        current_count = self.analyzer.get_table_row_count(table_name)
        print(f"\n📊 RESUMEN:")
        print(f"   ├─ Tabla: {table_name}")
        print(f"   ├─ Columnas a poblar: {len(selected_columns)}")
        print(f"   ├─ Registros actuales: {current_count}")
        print(f"   ├─ Registros a insertar: {record_count}")
        print(f"   ╰─ Total después: {current_count + record_count}")
        
        # Preguntar si quiere continuar
        should_continue = questionary.confirm(
            f"¿Continuar con la inserción de {record_count} registros?"
        ).ask()
        
        if not should_continue:
            # Si el usuario dice que NO, preguntar qué quiere hacer
            print(f"\n🔄 Operación cancelada para la tabla '{table_name}'")
            
            choices = [
                questionary.Choice(title="🔄 Reconfigurar esta tabla", value="reconfigure"),
                questionary.Choice(title="📊 Seleccionar otra tabla", value="new_table"),
                questionary.Choice(title="🏠 Volver al menú principal", value="main_menu"),
                questionary.Choice(title="🚪 Salir del programa", value="exit")
            ]
            
            action = questionary.select(
                "¿Qué quieres hacer ahora?",
                choices=choices
            ).ask()
            
            if action == "reconfigure":
                # Volver a configurar la misma tabla
                return self._reconfigure_table(table_name)
            elif action == "new_table":
                # Seleccionar una nueva tabla
                new_table = self.select_table()
                if new_table:
                    return self.analyze_and_confirm(new_table, record_count, [])
                return False
            elif action == "main_menu":
                # Devolver False para indicar que no continuar y volver al menú principal
                return False
            else:  # exit
                print("👋 ¡Hasta pronto!")
                exit()
        
        return should_continue

    def _reconfigure_table(self, table_name: str) -> bool:
        """Permite reconfigurar la misma tabla con nuevos parámetros"""
        print(f"\n🔄 RECONFIGURANDO TABLA: {table_name}")
        
        # Permitir cambiar las columnas seleccionadas
        selected_columns = self.select_columns(table_name)
        if not selected_columns:
            return False
        
        # Permitir cambiar la configuración de columnas
        if questionary.confirm("¿Quieres configurar cómo generar datos para cada columna?").ask():
            column_configs = self.configure_column_data(table_name, selected_columns)
        else:
            column_configs = {}
        
        # Permitir cambiar el número de registros
        record_count = self.get_insert_count()
        
        # Volver a analizar y confirmar
        return self.analyze_and_confirm(table_name, record_count, selected_columns)
    
    def switch_to_priority_table(self, priority_table: str, original_table: str) -> bool:
        """Cambia a poblar la tabla prioritaria primero"""
        print(f"\n🔄 CAMBIANDO A TABLA PRIORITARIA: {priority_table}")
        
        # Preguntar si quiere configurar columnas para la tabla prioritaria
        selected_columns = self.select_columns(priority_table)
        if not selected_columns:
            return False
        
        # Configurar datos por columna (OPCIONAL)
        if questionary.confirm("¿Quieres configurar cómo generar datos para cada columna?").ask():
            column_configs = self.configure_column_data(priority_table, selected_columns)
        else:
            column_configs = {}
        
        # Validación avanzada para la tabla prioritaria
        validator = DataValidator(self.db)
        is_valid, warnings = validator.validate_table_structure(priority_table)
        
        if not is_valid:
            print("❌ No se puede continuar debido a errores de validación")
            return False
        
        # Configuración de inserción
        record_count = self.get_insert_count()
        
        # Análisis y confirmación
        if not self.analyze_and_confirm(priority_table, record_count, selected_columns):
            print("❌ Operación cancelada por el usuario")
            return False
        
        # Poblar la tabla prioritaria
        from core.populator import DatabasePopulator
        from cli.progress import ProgressManager
        
        progress = ProgressManager()
        populator = DatabasePopulator(self.db, self.analyzer, self.generator, progress)
        populator.set_column_configs(column_configs)
        
        success_count, error_count = populator.populate_table(
            priority_table, record_count, selected_columns, 1000
        )
        
        if success_count > 0:
            print(f"\n✅ Tabla '{priority_table}' poblada exitosamente con {success_count} registros")
            print(f"💡 Ahora puedes poblar la tabla original: {original_table}")
            
            if questionary.confirm(f"¿Quieres poblar '{original_table}' ahora?").ask():
                # Volver a la tabla original
                return self.analyze_and_confirm(original_table, record_count, [])
        
        return False
    
    def final_confirmation(self, table_name: str, record_count: int, relationships: Dict) -> bool:
        """Confirmación final antes de la inserción"""
        # Limpiar consola para mostrar solo información esencial
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print(f"\n🎯 CONFIRMACIÓN FINAL")
        print("=" * 40)
        print(f"📊 Tabla: {table_name}")
        print(f"🔢 Registros: {record_count}")
        
        if relationships:
            print(f"🔗 Relaciones: {len(relationships)} encontradas")
        
        app_config = self.config.get_config()
        batch_size = app_config['defaults']['batch_size']
        print(f"📦 Tamaño de lote: {batch_size}")
        print("=" * 40)
        
        return questionary.confirm(
            "¿Ejecutar la población de datos?",
            default=True
        ).ask()
    
    def offer_export_options(self, table_name: str, record_count: int, selected_columns: List[str] = None, column_configs: Dict = None):
        """Ofrece opciones de exportación después de la inserción"""
        if not questionary.confirm("¿Quieres exportar la configuración de esta sesión?").ask():
            return
        
        choices = [
            questionary.Choice(title="💾 Exportar configuración JSON", value="config"),
            questionary.Choice(title="📝 Exportar script SQL", value="sql"),
            questionary.Choice(title="📊 Exportar datos de ejemplo (CSV)", value="csv"),
            questionary.Choice(title="❌ No exportar", value="none")
        ]
        
        option = questionary.select(
            "Selecciona una opción de exportación:",
            choices=choices
        ).ask()
        
        if option == "config":
            self._export_configuration(table_name, record_count, selected_columns, column_configs)
        
        elif option == "sql":
            self._export_sql_script(table_name, record_count, selected_columns, column_configs)
        
        elif option == "csv":
            self._export_sample_data(table_name, selected_columns)
    
    def _export_configuration(self, table_name: str, record_count: int, selected_columns: List[str], column_configs: Dict):
        """Exporta la configuración actual a JSON"""
        config_data = {
            "export_info": {
                "timestamp": datetime.now().isoformat(),
                "version": "1.0",
                "tool": "PostgreSQL Data Populator"
            },
            "table_config": {
                "table_name": table_name,
                "record_count": record_count,
                "selected_columns": selected_columns or []
            },
            "column_configurations": column_configs or {},
            "generation_settings": {
                "batch_size": self.config.get_config()['defaults']['batch_size'],
                "null_probability": 0.1,
                "data_generator": "smart"
            }
        }
        
        file_path = questionary.text(
            "Ruta para guardar la configuración:",
            default=f"./{table_name}_config.json"
        ).ask()
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                print(f"✅ Configuración exportada a {file_path}")
                
                # Mostrar resumen de lo exportado
                print(f"\n📁 RESUMEN DE EXPORTACIÓN:")
                print(f"   ├─ Tabla: {table_name}")
                print(f"   ├─ Columnas: {len(selected_columns)} configuradas")
                print(f"   ├─ Registros: {record_count}")
                print(f"   ╰─ Archivo: {file_path}")
                
            except Exception as e:
                print(f"❌ Error exportando configuración: {e}")
    
    def _export_sql_script(self, table_name: str, record_count: int, selected_columns: List[str], column_configs: Dict):
        """Exporta un script SQL reproducible"""
        file_path = questionary.text(
            "Ruta para guardar el script SQL:",
            default=f"./{table_name}_population_script.sql"
        ).ask()
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(f"-- Script de población automática para tabla: {table_name}\n")
                    f.write(f"-- Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"-- Registros a insertar: {record_count}\n")
                    f.write(f"-- Columnas: {', '.join(selected_columns)}\n\n")
                    
                    f.write("-- CONFIGURACIÓN PARA REUTILIZAR:\n")
                    f.write(f"/*\n")
                    f.write(f"Tabla: {table_name}\n")
                    f.write(f"Columnas seleccionadas: {selected_columns}\n")
                    f.write(f"Cantidad de registros: {record_count}\n")
                    if column_configs:
                        f.write(f"Configuraciones especiales:\n")
                        for col, config in column_configs.items():
                            f.write(f"  - {col}: {config.get('generation_type', 'random')}\n")
                    f.write(f"*/\n\n")
                    
                    f.write("-- INSTRUCCIONES:\n")
                    f.write("-- 1. Conectar a tu base de datos PostgreSQL\n")
                    f.write("-- 2. Ejecutar este script o usar el poblador automático\n")
                    f.write("-- 3. Ajustar los valores según sea necesario\n\n")
                    
                    f.write("-- EJEMPLO de inserción manual:\n")
                    columns_str = ", ".join(selected_columns)
                    placeholders = ", ".join(["%s"] * len(selected_columns))
                    f.write(f"-- INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});\n")
                
                print(f"✅ Script SQL exportado a {file_path}")
                print(f"📝 El script contiene la configuración para reproducir esta población")
                
            except Exception as e:
                print(f"❌ Error exportando script SQL: {e}")
    
    def _export_sample_data(self, table_name: str, selected_columns: List[str]):
        """Exporta datos de ejemplo de la tabla"""
        print("🔄 Preparando datos de ejemplo para exportación...")
        
        try:
            # Obtener algunos registros de ejemplo de la tabla
            limit = 10
            columns_str = ", ".join(selected_columns)
            self.db.cursor.execute(f"SELECT {columns_str} FROM {table_name} LIMIT {limit}")
            sample_data = self.db.cursor.fetchall()
            
            if sample_data:
                print(f"✅ Se encontraron {len(sample_data)} registros de ejemplo")
                print(f"📊 Muestra de datos:")
                print(f"   Columnas: {', '.join(selected_columns)}")
                for i, row in enumerate(sample_data[:3]):  # Mostrar solo 3 registros
                    print(f"   Registro {i+1}: {row}")
                
                # Exportar a CSV
                file_path = questionary.text(
                    "Ruta para guardar el CSV:",
                    default=f"./{table_name}_sample_data.csv"
                ).ask()
                
                if file_path:
                    self._export_to_csv(file_path, selected_columns, sample_data)
            else:
                print("ℹ️ No se encontraron datos para exportar")
                
        except Exception as e:
            print(f"❌ Error obteniendo datos de ejemplo: {e}")
    
    def _export_to_csv(self, file_path: str, columns: List[str], data: List):
        """Exporta datos a formato CSV"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                # Escribir encabezados
                f.write(",".join(columns) + "\n")
                
                # Escribir datos
                for row in data:
                    formatted_row = []
                    for value in row:
                        if value is None:
                            formatted_row.append("NULL")
                        elif isinstance(value, str):
                            # Escapar comas y comillas en strings
                            escaped_value = str(value).replace('"', '""')
                            formatted_row.append(f'"{escaped_value}"')
                        else:
                            formatted_row.append(str(value))
                    f.write(",".join(formatted_row) + "\n")
            
            print(f"✅ Datos exportados a CSV: {file_path}")
            print(f"📊 {len(data)} registros exportados")
            
        except Exception as e:
            print(f"❌ Error exportando CSV: {e}")
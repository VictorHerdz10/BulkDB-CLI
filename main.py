#!/usr/bin/env python3
import sys
import os
import time
import questionary

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from cli.interface import CLIInterface
from cli.progress import ProgressManager
from core.populator import DatabasePopulator
from core.validators import DataValidator
from core.advanced_relationships import RelationshipManager
from utils.config import ConfigManager
from cli.interface import Colors

def main():
    print(f"{Colors.OKCYAN}{Colors.BOLD}")
    print("🚀 BULKDB -- POBLADOR AVANZADO DE BASES DE DATOS POSTGRESQL")
    print("=" * 60)
    print(f"{Colors.ENDC}")
    
    cli = None
    progress = ProgressManager()
    config = ConfigManager()
    
    try:
        # Cargar configuración
        app_config = config.get_config()
        
        # Interfaz de usuario mejorada
        cli = CLIInterface(config)
        
        while True:  # Bucle principal para permitir múltiples operaciones
            # MOSTRAR MENÚ PRINCIPAL
            action = cli.show_main_menu()
            
            if action == "exit":
                print(f"\n{Colors.OKGREEN}👋 ¡Hasta pronto!{Colors.ENDC}")
                break
            elif action == "manage_connections":
                # Solo gestionar conexiones sin continuar con el proceso completo
                cli.manage_connections()
                continue
            elif action == "connect":
                # Flujo completo de conexión y población
                connection_string = cli.manage_connections()
                if not connection_string:  # Si el usuario cancela o hay error
                    continue
                
                # 2. Selección de tabla
                table_name = cli.select_table()
                if not table_name:  # Si el usuario cancela
                    continue
                    
                # 3. Selección de columnas
                selected_columns = cli.select_columns(table_name)
                if not selected_columns:  # Si el usuario cancela
                    continue
                
                # 4. Configuración de datos por columna (OPCIONAL)
                if questionary.confirm("¿Quieres configurar cómo generar datos para cada columna?").ask():
                    column_configs = cli.configure_column_data(table_name, selected_columns)
                else:
                    column_configs = {}
                
                # 5. Validación avanzada
                validator = DataValidator(cli.db)
                is_valid, warnings = validator.validate_table_structure(table_name)
                
                # Verificar tablas relacionadas vacías que necesitan prioridad
                fk_valid, fk_warnings, empty_related_tables = validator.validate_foreign_key_constraints(table_name)
                
                if empty_related_tables:
                    priority_order = validator.get_population_priority(table_name)
                    print(f"\n{Colors.RED}🚨 SE DETECTARON TABLAS RELACIONADAS VACÍAS:{Colors.ENDC}")
                    for i, tbl in enumerate(priority_order, 1):
                        print(f"   {i}. {tbl}")
                    
                    print(f"\n{Colors.BLUE}💡 Debes poblar '{priority_order[0]}' primero antes de '{table_name}'{Colors.ENDC}")
                    
                    if questionary.confirm("¿Quieres cambiar a poblar '{priority_order[0]}' primero?").ask():
                        # Cambiar a la tabla prioritaria
                        if cli.switch_to_priority_table(priority_order[0], table_name):
                            # Después de poblar la tabla prioritaria, preguntar si continuar
                            if not questionary.confirm("¿Quieres realizar otra operación?").ask():
                                break
                            else:
                                continue
                        else:
                            continue
                    else:
                        print(f"{Colors.RED}❌ No se puede continuar sin poblar las tablas relacionadas vacías{Colors.ENDC}")
                        if questionary.confirm("¿Quieres seleccionar otra tabla?").ask():
                            continue
                        else:
                            break
                
                progress.show_validation_results(is_valid, warnings)
                if not is_valid:
                    print(f"{Colors.RED}❌ No se puede continuar debido a errores de validación{Colors.ENDC}")
                    if questionary.confirm("¿Quieres seleccionar otra tabla?").ask():
                        continue
                    else:
                        break
                
                # 6. Análisis de relaciones avanzado (solo para columnas seleccionadas)
                rel_manager = RelationshipManager(cli.db, cli.analyzer)
                all_relationships = rel_manager.analyze_advanced_relationships(table_name)
                relevant_relationships = {col: rel for col, rel in all_relationships.items() if col in selected_columns}
                
                if relevant_relationships:
                    progress.show_relationship_progress([
                        {
                            'column': rel['column'],
                            'foreign_table': rel['foreign_table'],
                            'relationship_type': rel['cardinality'],
                            'has_data': rel['data_availability']['has_data'],
                            'data_count': rel['data_availability']['total_records']
                        }
                        for rel in relevant_relationships.values()
                    ])
                
                # 7. Configuración de inserción
                record_count = cli.get_insert_count()
                batch_size = app_config['defaults']['batch_size']
                
                # 8. Análisis y confirmación con columnas seleccionadas
                if not cli.analyze_and_confirm(table_name, record_count, selected_columns):
                    print(f"{Colors.YELLOW}❌ Operación cancelada por el usuario{Colors.ENDC}")
                    if questionary.confirm("¿Quieres seleccionar otra tabla?").ask():
                        continue
                    else:
                        break
                
                # 9. Confirmación final
                if not cli.final_confirmation(table_name, record_count, relevant_relationships):
                    print(f"{Colors.YELLOW}❌ Operación cancelada por el usuario{Colors.ENDC}")
                    if questionary.confirm("¿Quieres seleccionar otra tabla?").ask():
                        continue
                    else:
                        break
                
                # 10. Poblar la base de datos
                progress.show_initialization_panel(table_name, record_count)
                start_time = time.time()
                
                # Configurar el populator con las columnas seleccionadas
                populator = DatabasePopulator(cli.db, cli.analyzer, cli.generator, progress)
                populator.set_column_configs(column_configs)
                
                success_count, error_count = populator.populate_table(
                    table_name, record_count, selected_columns, batch_size
                )
                
                duration = time.time() - start_time
                
                # 11. Mostrar resultados finales
                progress.show_completion_panel(success_count, error_count, duration)
                
                # 12. Ofrecer exportación CON PARÁMETROS COMPLETOS
                cli.offer_export_options(table_name, record_count, selected_columns, column_configs)
                
                # Preguntar si quiere realizar otra operación
                if not questionary.confirm("¿Quieres realizar otra operación?").ask():
                    break
                    
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}⚠️  Operación interrumpida por el usuario.{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.FAIL}💥 Error inesperado: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
    finally:
        if cli and hasattr(cli, 'db'):
            cli.db.close_connection()
            print(f"\n{Colors.OKGREEN}🔌 Conexión a la base de datos cerrada{Colors.ENDC}")

if __name__ == "__main__":
    main()
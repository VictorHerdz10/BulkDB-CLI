from rich.progress import (
    Progress, 
    SpinnerColumn, 
    TextColumn, 
    BarColumn, 
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn
)
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.layout import Layout
import time

class ProgressManager:
    def __init__(self):
        self.console = Console()
        self.progress = None
    
    def create_progress(self):
        """Crea una instancia de Progress con configuración personalizada"""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("•"),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=self.console,
            expand=True
        )
    
    def show_initialization_panel(self, table_name: str, record_count: int):
        """Muestra panel de inicialización"""
        table = Table(show_header=False, box=None)
        table.add_column("", style="bold cyan")
        table.add_column("", style="white")
        
        table.add_row("📊 Tabla", f"[bold]{table_name}[/bold]")
        table.add_row("🎯 Registros", f"[bold]{record_count}[/bold]")
        table.add_row("🕒 Inicio", f"[bold]{time.strftime('%H:%M:%S')}[/bold]")
        
        panel = Panel(
            table,
            title="[bold blue]🚀 INICIANDO POBLACIÓN DE DATOS[/bold blue]",
            border_style="blue"
        )
        
        self.console.print(panel)
    
    def show_completion_panel(self, success_count: int, error_count: int, duration: float):
        """Muestra panel de finalización"""
        table = Table(show_header=False, box=None)
        table.add_column("", style="bold")
        table.add_column("", style="white")
        
        total = success_count + error_count
        success_rate = (success_count / total * 100) if total > 0 else 0
        
        table.add_row("✅ Éxitos", f"[green]{success_count}[/green]")
        table.add_row("❌ Errores", f"[red]{error_count}[/red]" if error_count > 0 else f"[green]{error_count}[/green]")
        table.add_row("📈 Tasa de éxito", f"[bold]{success_rate:.1f}%[/bold]")
        table.add_row("⏱️  Duración", f"[bold]{duration:.2f}s[/bold]")
        table.add_row("🕒 Final", f"[bold]{time.strftime('%H:%M:%S')}[/bold]")
        
        status_style = "green" if error_count == 0 else "yellow"
        status_text = "COMPLETADO" if error_count == 0 else "COMPLETADO CON ADVERTENCIAS"
        
        panel = Panel(
            table,
            title=f"[bold {status_style}]🎉 {status_text}[/bold {status_style}]",
            border_style=status_style
        )
        
        self.console.print(panel)
    
    def show_relationship_progress(self, relationships: list):
        """Muestra progreso de procesamiento de relaciones"""
        if not relationships:
            return
        
        table = Table(title="🔗 Procesando Relaciones")
        table.add_column("Relación", style="cyan")
        table.add_column("Tipo", style="magenta")
        table.add_column("Estado", style="green")
        table.add_column("Registros", style="yellow")
        
        for rel in relationships:
            table.add_row(
                f"{rel['column']} → {rel['foreign_table']}",
                rel['relationship_type'],
                "✅ Lista" if rel['has_data'] else "⚠️  Vacía",
                str(rel['data_count'])
            )
        
        self.console.print(table)
    
    def show_validation_results(self, is_valid: bool, warnings: list):
        """Muestra resultados de validación"""
        if is_valid and not warnings:
            self.console.print("✅ [green]Validación completada sin problemas[/green]")
            return
        
        if warnings:
            self.console.print("⚠️  [yellow]Validación completada con advertencias:[/yellow]")
            for warning in warnings:
                self.console.print(f"   • {warning}")
        
        if not is_valid:
            self.console.print("❌ [red]Validación falló. No se puede continuar.[/red]")
    
    def create_detailed_progress(self):
        """Crea una barra de progreso detallada para operaciones largas"""
        layout = Layout()
        
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3)
        )
        
        layout["main"].split_row(
            Layout(name="progress"),
            Layout(name="stats")
        )
        
        return layout

# Actualizar core/populator.py para usar el nuevo ProgressManager
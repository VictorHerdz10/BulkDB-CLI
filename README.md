# 🚀 BulkDB - Poblador Avanzado de Bases de Datos

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supported-green)
![License](https://img.shields.io/badge/License-MIT-yellow)
![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey)

<div align="center">

# 🗄️ BulkDB-CLI

**Herramienta CLI inteligente para operaciones masivas en bases de datos**

*Genera datos realistas y coherentes para tus bases de datos PostgreSQL*

</div>

## 📋 Tabla de Contenidos

- [Características](#-características)
- [Instalación](#-instalación)
- [Uso Rápido](#-uso-rápido)
- [Compilación](#-compilación)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Roadmap](#-roadmap)
- [Contribución](#-contribución)
- [Licencia](#-licencia)

## ✨ Características

### 🎯 Generación Inteligente de Datos
- **🤖 Contexto Inteligente**: Datos basados en nombres de columnas y tablas
- **🔗 Relaciones Automáticas**: Respeta foreign keys y relaciones
- **🎲 Datos Realistas**: Nombres, emails, direcciones, teléfonos realistas
- **⚡ Rendimiento Optimizado**: Inserción por lotes y progreso en tiempo real

### 🛠️ Funcionalidades Técnicas
- **🔍 Análisis de Esquema**: Detección automática de estructura de BD
- **✅ Validación Avanzada**: Verificación de constraints e integridad
- **🎛️ Configuración Flexible**: Personalización por columna y tipo de dato
- **📊 Exportación Múltiple**: JSON, SQL, CSV para reutilización

### 🎨 Experiencia de Usuario
- **🌈 Interfaz Colorida**: CLI interactiva con emojis y colores
- **📈 Progreso Visual**: Barras de progreso en tiempo real
- **🔄 Gestión de Conexiones**: Múltiples conexiones guardadas
- **🐛 Manejo de Errores**: Diagnóstico detallado y recuperación

## 🚀 Instalación

### Prerrequisitos
- **Python 3.8** o superior
- **PostgreSQL 12+** (actualmente soportado)
- **pip** (gestor de paquetes Python)

### Instalación desde Código Fuente

```bash
# Clonar el repositorio
git clone https://github.com/VictorHerdz10/BulkDB-CLI.git
cd BulkDB-CLI

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar la aplicación
python main.py
```

### Dependencias Principales
```txt
psycopg2-binary>=2.9.9      # Conexión PostgreSQL
questionary>=1.10.0         # CLI interactiva
rich>=13.7.0               # Interfaz visual
typing-extensions>=4.8.0   # Tipado avanzado
pyinstaller>=6.16.0        # Creación de ejecutables
```

## 💻 Uso Rápido

### Flujo Básico de Trabajo

1. **🔗 Conectar a PostgreSQL**
   ```bash
   # String de conexión ejemplo:
   postgresql://usuario:contraseña@localhost:5432/mi_base_datos
   ```

2. **📊 Seleccionar Tabla**
   - Listado automático de todas las tablas
   - Análisis de estructura y relaciones

3. **⚙️ Configurar Columnas**
   - Selección de columnas específicas
   - Configuración por tipo de dato
   - Detección de auto-incrementales

4. **🎲 Generar Datos**
   - Configurar cantidad de registros
   - Personalizar generación por columna
   - Validar relaciones foreign key

5. **📥 Insertar Masivamente**
   - Inserción por lotes optimizada
   - Progreso en tiempo real
   - Manejo de errores automático

### Ejemplo de Configuración por Columna

```python
# Tipos de generación disponibles:
- "random": Valores aleatorios contextuales
- "sequence": Valores secuenciales numéricos  
- "fixed": Valores constantes predefinidos
- "smart": Basado en análisis de nombres
```

## 🔨 Compilación

### Crear Ejecutable con PyInstaller

#### 1. Archivo de Especificación (`bulkdb.spec`)

```python
# -*- mode: python ; coding: utf-8 -*-

APP_NAME = "BulkDB-CLI"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = "Herramienta CLI para operaciones masivas en bases de datos."
APP_AUTHOR = "Victor Hernandez"
APP_COPYRIGHT = f"Copyright (C) 2025 {APP_AUTHOR}"

# ICON_FILE = "icon.ico"  # Descomenta si tienes un icono

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('core/datasets/master_dataset.json', 'core/datasets'),
        # Agregar más archivos de datos si es necesario
    ],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=APP_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Metadatos de la aplicación
    company_name=APP_AUTHOR,
    product_name=APP_NAME,
    copyright=APP_COPYRIGHT,
    description=APP_DESCRIPTION,
    # icon=ICON_FILE,  # Descomenta si usas icono
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name=APP_NAME,
)
```

#### 2. Comandos de Compilación

**Para Windows:**
```bash
pyinstaller bulkdb.spec
# O directamente:
pyinstaller --onefile --name BulkDB --add-data "core/datasets;core/datasets" main.py
```

**Para Linux/macOS:**
```bash
pyinstaller bulkdb.spec  
# O directamente:
pyinstaller --onefile --name BulkDB --add-data "core/datasets:core/datasets" main.py
```

#### 3. Estructura de Output
```
dist/
└── BulkDB-CLI.exe    # Ejecutable final (Windows)
    # O BulkDB-CLI en Linux/macOS
```

## 📁 Estructura del Proyecto

```
BulkDB-CLI/
├── 📄 main.py                    # Punto de entrada principal
├── 📄 requirements.txt           # Dependencias del proyecto
├── 📄 bulkdb.spec               # Configuración PyInstaller
├── 📄 icon.ico                  # Icono del ejecutable de la cli
├── 🔧 core/                     # Núcleo de la aplicación
│   ├── database.py              # Gestión de conexiones PostgreSQL
│   ├── schema_analyzer.py       # Análisis de esquema de BD
│   ├── data_generator.py        # Generación inteligente de datos
│   ├── populator.py             # Inserción masiva de datos
│   ├── validators.py            # Validación de datos y estructura
│   ├── advanced_relationships.py # Gestión de relaciones complejas
│   └── 📊 datasets/             # Datos para generación realista
│       └── master_dataset.json  # Dataset masivo contextual
├── 🖥️ cli/                       # Interfaz de línea de comandos
│   ├── interface.py             # Menús y flujos de usuario
│   └── progress.py              # Gestión de progreso visual
└── ⚙️ utils/                     # Utilidades
    └── config.py                # Gestión de configuración
```

### Dataset Contextual Incluido

El archivo `core/datasets/master_dataset.json` contiene:

- **👥 Nombres Personales**: Españoles, realistas
- **🏢 Empresas**: Nombres de empresas realistas
- **📧 Contacto**: Dominios de email, formatos de teléfono
- **🌍 Ubicaciones**: Ciudades, provincias, direcciones
- **🛍️ Productos**: Nombres y descripciones realistas
- **💼 Profesional**: Puestos de trabajo, departamentos

## 🗺️ Roadmap

### 🔄 Soporte Multi-Base de Datos
- [ ] **MySQL** - Soporte completo
- [ ] **SQLite** - Bases de datos embebidas  
- [ ] **SQL Server** - Entornos empresariales
- [ ] **MongoDB** - Expansión NoSQL

### 🚀 Mejoras Planificadas
- [ ] **Interfaz Web** - Dashboard visual
- [ ] **API REST** - Integración CI/CD
- [ ] **Plantillas** - Configuraciones por industria
- [ ] **Cloud Integration** - Bases de datos en la nube
- [ ] **Más Formatos de Exportación** - XML, YAML

### 🎨 Mejoras de UX
- [ ] **Temas Personalizables** - Colores y estilos
- [ ] **Modo Silencioso** - Para scripts automáticos
- [ ] **Logs Detallados** - Para debugging avanzado

## 🤝 Contribución

¡Contribuciones son bienvenidas! Este proyecto está en activo desarrollo.

### Áreas de Contribución
1. **🔌 Nuevos Drivers de BD**: MySQL, SQLite, etc.
2. **🎲 Mejoras en Generación de Datos**: Más datasets, más inteligencia
3. **📊 Nuevas Funcionalidades de Exportación**: Más formatos, más opciones
4. **🐛 Reporte de Bugs**: Ayuda a mejorar la estabilidad
5. **📚 Documentación**: Mejora guías y ejemplos

### Proceso de Contribución
1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para detalles.

## 👨‍💻 Autor

**Victor Hernandez** 
- 🐙 GitHub: [@VictorHerdz10](https://github.com/VictorHerdz10)
- 💼 LinkedIn: [Victor Hernandez](https://www.linkedin.com/in/victor-hern%C3%A1ndez-salcedo-08348132a)
- 📧 Email: victor.herdz@example.com

## 🙏 Agradecimientos

- **psycopg2** - Por el excelente driver de PostgreSQL
- **rich** - Por hacer las CLIs hermosas
- **questionary** - Por las interfaces interactivas
- **PyInstaller** - Por hacer la distribución sencilla

---

<div align="center">

### ⭐ ¿Te gusta este proyecto? Dale una estrella en GitHub!

**¡BulkDB hace que el poblado de bases de datos sea rápido, inteligente y sin dolor! 🎉**

</div>
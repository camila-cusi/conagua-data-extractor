# Conagua Data Extractor

**Conagua Data Extractor** es una herramienta en Python diseñada para extraer y procesar información meteorológica de la CONAGUA. Permite obtener datos históricos y actuales sobre temperatura y precipitación a nivel estatal, estructurarlos en DataFrames y exportarlos en formatos como Excel o CSV.

## 📌 Características

- **Extracción Automática:** Descarga datos de temperatura y precipitación desde los portales de datos abiertos de CONAGUA.
- **Procesamiento de Datos:** Limpia, transforma y estructura los datos para su análisis.
- **Filtrado y Ordenación:** Permite seleccionar rangos de fechas y organizar columnas según una configuración establecida.
- **Exportación Flexible:** Guarda los datos procesados en archivos Excel y CSV organizados por año y estado.
- **Modularidad y Reutilización:** Diseñado para facilitar la integración con otros sistemas, ya sea como paquete, script o API.

## 🚀 Instalación

### Clonar el repositorio
   ```bash
   git clone https://github.com/camila-cusi/conagua-data-extractor.git
   cd conagua-data-extractor
```

## 📂 Estructura del Proyecto

- **`extract/`** → Módulo encargado de la descarga de datos desde CONAGUA. Contiene la lógica para acceder a los archivos ZIP, extraer su contenido y almacenarlos localmente.

- **`transform/`** → Contiene las funciones y clases necesarias para procesar los datos descargados:
  - **`processor.py`** → Estandariza, limpia y estructura la información.
  - **`filter_data.py`** → Permite filtrar los datos por fechas o por criterios específicos.
  - **`utils.py`** → Funciones auxiliares para manejo de archivos y configuración.

- **`load/`** → *(Opcional)* Módulo dedicado a la carga y almacenamiento de los datos en otros formatos o bases de datos.

## 🛠️ Uso

```
from src.conagua_datos.load.read_conagua import ReadConaguaDatos

conagua_reader = ReadConaguaDatos()
precip_df = conagua_reader.get_conagua_df()
```
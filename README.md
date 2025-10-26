# ETL_Airbnb — Proyecto de Extracción, Transformación y Carga (ETL)

# Integrantes
1. Santiago Varela Jiménez 
2. Santiago Álzate Munera
3. Franyelica García Fernández


## Descripción del proyecto y objetivo
Este proyecto realiza la extracción desde una instancia local de MongoDB de colecciones relacionadas con Airbnb (Ciudad de México), ejecuta pasos de transformación y limpieza, y prepara datos para su posterior carga en un Data Warehouse. El objetivo es limpiar y normalizar los datos (precios, fechas, campos anidados) y generar artefactos listos para análisis y modelado.

## Requisitos e instalación
Sigue estos pasos en Windows (PowerShell o CMD). Se asume que tienes Python instalado.
1. Abrir la terminal (PowerShell o CMD) en la carpeta del proyecto:
    cd C:\Users\Santiago\Desktop\etl_airbnb
2. Crear entorno virtual:
    python -m venv venv
3. Activar el entorno virtual
    .\venv\Scripts\Activate.ps1 ó venv\Scripts\activate.bat
4. Actualizar pip:
    python -m pip install --upgrade pip
5. Instalar dependencias:
    pip install -r requirements.txt


## Crear la base de datos (para el Data Warehouse)
Instrucción SQL (ejecutar en tu servidor de bases de datos, p. ej. MySQL, PostgreSQL o SQL Server según tu entorno):
CREATE DATABASE Airbnb_DW;
Nota: Asegúrate de ejecutar el comando en el SGBD correcto y con los permisos adecuados. El nombre `Airbnb_DW` es el ejemplo solicitado.


## Borrar entorno virtual actual
Si necesitas eliminar el entorno y crear uno nuevo:
rmdir /s /q venv
O eliminar la carpeta `venv` desde el Explorador de archivos.


## Usar el entorno correcto en Jupyter / VS Code
1. En la parte superior derecha del notebook o en la paleta de comandos, selecciona el kernel/interpretador correspondiente a `venv`. Buscá algo como: `Python 3.12 (venv)`.
2. Si no aparece, registra el entorno para Jupyter:
pip install ipykernel
python -m ipykernel install --user --name=venv --display-name "Python (venv)"
Luego reinicia VS Code/Jupyter y selecciona ese kernel.


## Ejemplo de ejecución del ETL
Con el entorno activo, ejecuta el script principal de test_transformacion y luego el de test_carga.
python .\src\test_transformacion.py
python .\src\test_carga.py


## Troubleshooting / notas
Asegúrate de activar el venv antes de instalar dependencias o ejecutar scripts.


# ETL Airbnb (Proyecto)
Este repositorio contiene un pipeline ETL (Extracción, Transformación y Carga) para datos de Airbnb. El objetivo del proyecto es extraer datos desde una fuente (simulada con MongoDB o generadores de prueba), aplicar limpiezas y transformaciones (normalización de precios, manejo de nulos, derivación de variables temporales, expansión de amenities, eliminación de duplicados) y finalmente cargar los datos transformados a SQL Server y/o exportarlos a archivos Excel.

1. Estructura principal
- `src/` : Código fuente con módulos de extracción (`extraccion.py`), transformación (`test_transformacion.py`) y carga (`test_carga.py`). y conexión a MongoDB (`mongodb.py`).
- `data/` : Salida de archivos Excel generados por la carga.
- `logs/` o `Logs/` : Archivos de logs generados por las distintas etapas.

2. Objetivo
- Comprobar transformaciones de datos con conjuntos sintéticos (scripts de prueba).
- Persistir los datos transformados en SQL Server para análisis posterior.
- Exportar resultados a Excel para inspección manual.

3. Requisitos
- Windows (PowerShell) — las instrucciones de activación del entorno están orientadas a PowerShell.
- Python 3.10+ instalado y disponible en `PATH`.
- SQL Server (opcional, sólo necesario si desea ejecutar la etapa de carga hacia base de datos).
- (Opcional) MongoDB si desea probar la extracción real desde una base Mongo.


# Integrantes del grupo
Santiago Varela Jiménez
Santiago Álzate Munera
Franyelica García Fernández


# resumen
1. Crear y activar entorno virtual (PowerShell).
2. Instalar dependencias con `pip install -r requirements.txt`.
3. Ejecutar transformaciones de prueba: `python .\src\test_transformacion.py`.
4. Ejecutar carga de prueba (configurar credenciales en `src/test_carga.py`): `python .\src\test_carga.py`.
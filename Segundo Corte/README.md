# Proyecto de Análisis de Estadísticas y Valor de Jugadores de Fútbol

## Rusumen

Este repositorio contiene el código y la documentación para un proyecto de análisis de datos que utiliza dos conjuntos de datos relacionados con el fútbol: uno que contiene estadísticas de jugadores de fútbol para los años 2021 y 2022 y otro que proporciona información sobre el valor de cada jugador.

## Objetivos
El objetivo principal de este proyecto es llevar a cabo un análisis completo de los datos de los jugadores de fútbol y sus valores para ayudar a los equipos y clubes a tomar decisiones informadas en el proceso de selección y contratación de jugadores. Para lograr este objetivo, hemos realizado las siguientes tareas:

## Exploración y Limpieza de Datos (EDA):
Realizamos un proceso de exploración y limpieza de datos exhaustivo en ambos conjuntos de datos. Esto incluyó la identificación y manejo de valores nulos, la revisión de tipos de datos, y la selección de las columnas más relevantes para nuestro análisis.

Adicionalmente se hizo el analisis exploratorio al conjunto de datos extraido mediante WebScrapping.

## Selección de una API:
Como requisito teniamos la seleccion de una API que nos ofreciera informacion adicional de los jugadores de futbol, debido a que la informacion de estadiscas de los jugadores no es tan abierta al publico el encontrar una API util y gratis fue imposible. Por lo que se opto por realizar WebScrapping, una forma de extraer informacion de sitios web mediante un script de python de la pagina Transfermartk.

## Análisis de Estadísticas de Jugadores:
Utilizamos las estadísticas de jugadores para comprender mejor el rendimiento de cada jugador en el campo. Esto incluyó el análisis de goles marcados, asistencias, tarjetas amarillas, tarjetas rojas y otras métricas clave.

## Requisitos de Software

Antes de ejecutar los scripts de análisis, asegúrate de tener instalados los siguientes paquetes y herramientas:

(NOTA: para el uso de la libreria de selenium es necesario obtener una version de pruebas de chrome, este nos permite mediante scripts acceder a sitios, realizar acciones y extraer informacion)

- Python 3.x
- Jupyter Notebooks
- Power BI
- MySQL

Además, necesitarás las siguientes bibliotecas de Python:

- Pandas
- MySQL.Connector
- PySQL
- Matplotlib
- Selenium

Puedes instalar estas bibliotecas utilizando el administrador de paquetes pip:

```
pip install pandas psycopg2 matplotlib selenium
```

La ejecucion del EDA fue realizada en 2 jupyter notebooks (uno por dataset).

Por otro lado carga y eliminacion de columnas de los csv esta en los scripts de python que puedes correr en consola:
```python3 dataset.py``` ó ```python3 dataset2.py```

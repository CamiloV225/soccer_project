# Proyecto de Análisis de Estadísticas y Valor de Jugadores de Fútbol

## Rusumen

Este repositorio contiene el código y la documentación para un proyecto de análisis de datos que utiliza dos conjuntos de datos relacionados con el fútbol: uno que contiene estadísticas de jugadores de fútbol para los años 2021 y 2022 y otro que proporciona información sobre el valor de cada jugador.

## Objetivos
El objetivo principal de este proyecto es llevar a cabo un análisis completo de los datos de los jugadores de fútbol y sus valores para ayudar a los equipos y clubes a tomar decisiones informadas en el proceso de selección y contratación de jugadores, y crear la funcionalidad de realizar una estimación del precio de jugador basado en las estadisticas disponibles. Para lograr este objetivo, hemos realizado las siguientes tareas:

## Exploración y Limpieza de Datos (EDA):
Realizamos un proceso de exploración y limpieza de datos exhaustivo en ambos conjuntos de datos. Esto incluyó la identificación y manejo de valores nulos, la revisión de tipos de datos, y la selección de las columnas más relevantes para nuestro análisis.

Adicionalmente se hizo el analisis exploratorio al conjunto de datos extraido mediante WebScrapping.

## Selección de una API:
Como requisito teniamos la seleccion de una API que nos ofreciera informacion adicional de los jugadores de futbol, debido a que la informacion de estadiscas de los jugadores no es tan abierta al publico el encontrar una API util y gratis fue imposible. Por lo que se opto por realizar WebScrapping, una forma de extraer informacion de sitios web mediante un script de python de la pagina Transfermartk.

## Análisis de Estadísticas de Jugadores:
Utilizamos las estadísticas de jugadores para comprender mejor el rendimiento de cada jugador en el campo. Esto incluyó el análisis de goles marcados, asistencias, tarjetas amarillas, tarjetas rojas y otras métricas clave.

## Selección de Features:
Se seleccionaron las columnas necesarias para el modelo de regresion lineal, dentro de las cuales se escogieron las columnas con una correlación mayor al 20%.

## Selección del modelo:
Se decidió utilizar el Hist Gradient Boosting Regressor como el modelo final para predecir el precio de los jugadores, ya que mostró el mejor desempeño tanto en en las métricas mencionadas en comparación con los otros modelos evaluados

## Requisitos de Software

Antes de ejecutar los scripts de análisis, asegúrate de tener instalados los siguientes paquetes y herramientas:

(NOTA: para el uso de la libreria de selenium es necesario obtener una version de pruebas de chrome, este nos permite mediante scripts acceder a sitios, realizar acciones y extraer informacion)

- Python 3.x
- Jupyter Notebooks
- Power BI
- Postgres
- Apache Airflow
- Apache Kafka
- Docker

Además, necesitarás las siguientes bibliotecas de Python:

- Pandas
- Numpy
- Psycopg2-binary
- Joblib
- SkLearn
- Selenium
- Airflow
- Python-Kafka

Puedes instalar estas bibliotecas utilizando el administrador de paquetes pip:

```
pip install pandas selenium psycopg2-binary joblib scikit-learn airflow python-kafka
```

La ejecucion del EDA fue realizada en 2 jupyter notebooks (uno por dataset).

Una vez hayas copiado el repositorio, en tu maquina virtual puedes realizar el Setup de las herramientas con los siguientes pasos:

## Docker:
Para iniciar los contenedores de docker, debes de ubicarte dentro de la carpeta del repositorio y ejecutar el siguiente comando, el cual iniciara el docker compose con todas las imagenes necesarias para el proyecto:

```
sudo docker compose up -d
```

Luego deberas entrar al contenedor de Kafka para crear el topic, que es el canal de comunicación por donde se enviaran los mensajes:

```
docker exec -it kafka-test bash
```

Y por ultimo creas el topic con el siguiente comando:

```
kafka-topics --bootstrap-server kafka-test:9092 --create --topic soccer
```

## Consumer-kafka:
Dentro de la carpeta ```/services``` podras encontrar el archivo ```consumer.py```, que es el encargado de recibir los mensajes en tiempo real:

```
python3 consumer.py
```

## Apache Airflow:
Una vez hayas hecho todos estos pasos, puedes iniciar kafka pero antes una pequeña recomendación y es primero ejecutar el ```scheduler``` para actualizar la lista de DAG's en el dashboard de airflow:

```
airflow scheduler
```

Y luego podras iniciar airflow con el siguiente comando:

```
airflow standalone
```

Dentro encontraras la lista de DAG's, busca el DAG llamado ```soccer_api_dag``` y ejecutalo.

Esperamos que te haya gustado.


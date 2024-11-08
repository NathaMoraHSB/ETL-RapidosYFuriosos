# ETL-RapidosYFuriosos

## Integrantes
- Natalia Andrea Marin Hernandez - 2041622
- Nathalia Carolina Mora Arciniegas - 2413217
- Herney Eduardo Quintero Trochez - 1528556

## Descripción

El presente proyecto tiene como objetivo realizar un proceso de ETL (Extracción, Transformación y Carga) de datos de
una base de datos de de una empresa de mensajería llamada Rapidos y Furiosos para la asignatura Ciencia de datos.

## Estructura del proyecto

El proyecto se encuentra organizado de la siguiente manera:

```
ETL-RapidosYFuriosos
│   etl
│   │   answers.py
│   │   dim_cliente.py
│   │   dim_fecha.py
│   │   dim_hora.py
│   │   dim_mensajero.py
│   │   dim_novedad.py
│   │   dim_sede.py
│   │   hecho_novedades.py
│   │   hecho_servicios_acumulating.py
│   │   hecho_servicios_dia.py
│   │   hecho_servicios_hora.py
│   │   helper.py
│   │   main.py
│   │   trans_novedades.py
│   │   trans_servicios.py
│   notebooks
│   │   Visualizacion_respuestas.ipynb
│   environment.yml
│   config..yml
│   config_example.yml
│   README.md
```
## Requerimientos

Para poder ejecutar el proyecto es necesario tener:

- PostgreSQL
- Python 3.12

## Instrucciones de uso

1. Instalar las dependencias del proyecto con el siguiente comando:

```bash
pip3.12 install -r requirements.txt
```

2. Crear una base de datos en PostgreSQL con el nombre `rapidos_y_furiosos`.
3. Crear una base de datos en PostgreSQL con el nombre `ETL-rapidos-y-furiosos`.
4. Clonar el archivo `config_example.yml` y renombrarlo a `config.yml` y modificar los valores de acuerdo a la configuración de su base de datos.

```bash
cp config_example.yml config.yml
```
5. Ejecutar el archivo `main.py` para realizar el proceso de ETL.

```bash
python3.12 main.py
```

6. Con Jupyter Notebook abrir el archivo `Visualizacion_respuestas.ipynb` para visualizar las respuestas a las preguntas planteadas en el enunciado del proyecto.
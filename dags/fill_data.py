from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
import pendulum
import pandas as pd
import datetime
from td7.data_generator import DataGenerator
from td7.schema import Schema

EVENTS_PER_DAY = 10_000

file_path = 'td7/books.csv'
books_df = pd.read_csv(file_path) #no entiendo, esta importado generator

def obtener_idiomas_editoriales(base_time: str, n: int, m:int):
    """Generates synth data and saves to DB.

    Parameters
    ----------
    base_time: strpoetry export --without-hashes --format=requirements.txt > requirements.txt

        Base datetime to start events from.
    n : int
        Number of events to generate.
    """
    generator = DataGenerator()
    schema = Schema()
    idiomas = generator.generate_idiomas(n)
    schema.insert(idiomas, "idiomas")
    editoriales=  generator.generate_editoriales(m)
    schema.insert(editoriales, "editoriales")

def obtener_autores(base_time: str):
    generator = DataGenerator()
    schema = Schema()
    autores = generator.generate_autores()
    schema.insert(autores, "autores")
    
def obtener_libros_escribio(base_time: str, n: int, z : int, m:int) :
    generator = DataGenerator()
    schema = Schema()
    idiomas_sample = schema.get_idiomas(z)
    editoriales_sample = schema.get_editoriales(m)
    libros, generos_libros, escribio = generator.generate_libros(
        idiomas_sample, #tenia parametros de más 
        editoriales_sample,
        n
    )
    schema.insert(libros, "libros")
    schema.insert(generos_libros, "generos_libros")
    schema.insert(escribio, "escribio")

def obtener_fisicos_y_digitales(base_time:str, libros): ## LIBROS DEL ESQUEMA !!!
    #los libros deberia buscarlos en el schema, pero solo los que se acaban de cargar (lote)
    generator = DataGenerator()
    schema = Schema()
    fisicos, digitales = generator.generate_libros_fyd(libros)
    schema.insert(fisicos, "libros_fisicos"),
    schema.insert(digitales, "libros_digitales")    

def obtener_audiolibros(base_time:str, n:int, z:int):
    generator = DataGenerator()
    schema = Schema()
    idiomas_sample = schema.get_idiomas(z)
    audiolibros = generator.generate_audiolibros(
        idiomas_sample,
        n)
    schema.insert(audiolibros, "audiolibros")

def obtener_usuarios(base_time:str, n:int):
    generator = DataGenerator()
    schema = Schema()
    usuarios = generator.generate_usuarios(n)
    schema.insert(usuarios, "usuarios")

def obtener_ejemplares(base_time:str, n:int, libros_fisicos): ## LIBROS DEL ESQUEMA !!!
    generator = DataGenerator()
    schema = Schema()
    ejemplares = generator.generate_ejemplares(libros_fisicos, n)
    schema.insert(ejemplares, "ejemplares")

def obtener_reservas(base_time:str, n:int, f:int, u:int):
    #f:cantidad de libros fisicos, u:cantidad de usuarios, n:cantidad de reservas a generar
    #n <= u*f (todos los usuarios reservan todos los libros)
    generator = DataGenerator()
    schema = Schema()
    fisicos = schema.get_libros_fisicos(f)
    usuarios = schema.get_usuarios(u)
    reservas = generator.generate_reservas(fisicos, usuarios, n)
    schema.insert(reservas, "reservas")

def obtener_prestamos(base_time:str, n:int, e:int, u:int):
    #e:cantidad de ejemplares
    #n<=e (no puede haber más préstamos que ejemplares)
    generator = DataGenerator()
    schema = Schema()
    ejemplares = schema.get_ejemplares(e)
    usuarios = schema.get_usuarios(u)
    prestamos = generator.generate_prestamos(ejemplares, usuarios, n)
    schema.insert(prestamos, "prestamos")

def es_principio_de_mes(base_time:str):
    base_time = datetime.strptime(base_time, '%Y-%m-%d')
    if base_time.day == 1:
        return ['autores','usuarios']
    else: #cambiar que sea siempre (incluyendo primero de mes)
        'usuarios'



#---------------------DAGs-----------------------

#----------ONCE---------
with DAG(
    "fill_data_once",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@once",
    catchup=True,
) as dag:
    task_idiomas_editoriales = PythonOperator(
        task_id="idiomas-editoriales",
        python_callable=obtener_idiomas_editoriales,
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 15,  #idiomas
            "m": 25, #editoriales
        },
    )

task_idiomas_editoriales

#----------FREQ---------
with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@daily", # CAMBIAR !!
    catchup=True,
) as dag:
    
#----branch----
    branch_task = BranchPythonOperator(
        task_id='branch-task',
        python_callable=es_principio_de_mes,
        dag=dag,
)
    
#----rama mensual----
    task_autores = PythonOperator(
        task_id="autores",
        python_callable = obtener_autores,
        op_kwargs={
            "base_time": "{{ ds }}",  
        },
    )
    task_libros = PythonOperator(
        task_id="libros",
        python_callable=obtener_libros_escribio, #como tiene el 1/6 y catchup true deberia ejecutarse 2 veces
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 3000,  #libros
            "z": 5,    #idiomas
            "m": 5 #editoriales
        },
    )
    task_libros_fyd = PythonOperator(
        task_id="libros-fyd",
        python_callable=obtener_fisicos_y_digitales, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            #"libros":  # PENSAR COMO SACAR DEL ESQUEMA !
        },
    )
    task_ejemplares = PythonOperator(
        task_id="ejemplares",
        python_callable=obtener_ejemplares, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 10000
            #"libros":  # PENSAR COMO SACAR DEL ESQUEMA !
        },
    )
    task_audiolibros = PythonOperator (
        task_id="audiolibros",
        python_callable=obtener_audiolibros, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 500,  #libros
            "z": 3,    #idiomas
        },
    )

#----rama diaria----
    task_usuarios = PythonOperator (
        task_id="usuarios",
        python_callable=obtener_usuarios, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 5,  #numero?? depende de la cadencia
        },  
    )

#----dependencias----
    task_reservas = PythonOperator (
        task_id="reservas",
        python_callable=obtener_reservas, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 30, 
            "f": 200,
            "u": 150,
        },  
        
    )

    task_prestamos = PythonOperator (
        task_id="prestamos",
        python_callable=obtener_prestamos, 
        op_kwargs={
            "base_time": "{{ ds }}",  
            "n": 30, 
            "e": 200,
            "u": 150,
        },  
        trigger_rule="one_success"
    )

branch_task >> [task_autores, task_usuarios]

task_autores >> task_libros >> task_libros_fyd >> task_ejemplares
task_autores >> task_audiolibros

[task_usuarios, task_libros_fyd] >> task_reservas
[task_usuarios, task_ejemplares] >> task_prestamos
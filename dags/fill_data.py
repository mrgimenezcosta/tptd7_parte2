from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import datetime
from td7.data_generator import DataGenerator
from td7.schema import Schema

EVENTS_PER_DAY = 10_000

file_path = 'td7/books.csv'
books_df = pd.read_csv(file_path) #no entiendo, esta importado generator


def obtener_idiomas_editoriales(base_time: str, n: int):
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
    editoriales=  generator.generate_editoriales(n)
    schema.insert(editoriales, "editoriales")

def generate_data_monthly(base_time: str, n: int, z : int) :
    generator = DataGenerator()
    schema = Schema()
    idiomas_sample = schema.get_idiomas(z)
    editoriales_sample = schema.get_editoriales(z)
    libros, autores, generos_libros, libro_autor = generator.generate_libro_escribio(
        books_df,idiomas_sample, editoriales_sample,
        datetime.datetime.fromisoformat(base_time),
        datetime.timedelta(days=1),
        n,
    )
    schema.insert(libros, "libros")
    schema.insert(autores, "autores")
    schema.insert(generos_libros, "generos_libros")
    schema.insert(libro_autor, "escribio")


with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@once",
    catchup=True,
) as dag:
    op = PythonOperator(
        task_id="task",
        python_callable=obtener_idiomas_editoriales(,20),
        op_kwargs=dict(n=EVENTS_PER_DAY, base_time="{{ ds }}"),
    )

    with DAG(
    "fill_data",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="@monthly",
    catchup=True,
) as dag:
    op = PythonOperator(
        task_id="task",
        python_callable=generate_data_monthly(6000, 20),
        op_kwargs=dict(n=EVENTS_PER_DAY, base_time="{{ ds }}"),
    )

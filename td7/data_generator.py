from faker import Faker
from faker.providers import address, date_time, internet, passport, phone_number
import uuid
import random
from td7.custom_types import Records
import datetime
import pandas as pd  

# Inicializar Faker
fake = Faker()
fake.add_provider(address)
fake.add_provider(date_time)
fake.add_provider(internet)
fake.add_provider(company)
fake.add_provider(person)

# Leer el archivo CSV proporcionado
file_path = 'td7/books.csv'
books_df = pd.read_csv(file_path)

# Función para generar datos para la tabla 'libros'
def generate_libros_data(books_df, n):
    libros = []
    for _ in range(n):
        book = books_df.sample(1).iloc[0]
        libro = {
            "ISBN": book["isbn13"],
            "titulo": book["title"],
            "nro_serie": random.randint(1, 10),
            "edicion": random.randint(1, 5),
            "lang_code": fake.language_code(),
            "editorial_razon": fake.company(),
            "fecha_publicacion": fake.date_between(start_date="-50y", end_date="today")
        }
        libros.append(libro)
    return libros

# Generar datos para 'libros_fisicos'
def generate_libros_fisicos(libros):
    libros_fisicos = []
    for libro in libros:
        libro_fisico = {
            "ISBN": libro["ISBN"],
            "cant_copias": random.randint(1, 20)
        }
        libros_fisicos.append(libro_fisico)
    return libros_fisicos

# Generar datos para 'libros_digitales'
def generate_libros_digitales(libros):
    libros_digitales = []
    for libro in libros:
        libro_digital = {
            "ISBN": libro["ISBN"],
            "link": fake.url()
        }
        libros_digitales.append(libro_digital)
    return libros_digitales

# Generar datos para 'audiolibros'
def generate_audiolibros(libros):
    audiolibros = []
    for libro in libros:
        audiolibro = {
            "id_audiolibro": random.randint(1000, 9999),
            "titulo": libro["titulo"],
            "duracion": random.randint(60, 1200),  # duración en minutos
            "nro_serie": libro["nro_serie"],
            "lang_code": libro["lang_code"],
            "fecha_publicacion": libro["fecha_publicacion"]
        }
        audiolibros.append(audiolibro)
    return audiolibros

# Generar datos para 'generos_libros'
def generate_generos_libros(libros):
    generos_libros = []
    for libro in libros:
        generos = random.sample(["Ficción", "No Ficción", "Misterio", "Romance", "Ciencia Ficción", "Biografia"], random.randint(1, 3))
        for genero in generos:
            generos_libro = {
                "ISBN": libro["ISBN"],
                "genero": genero
            }
            generos_libros.append(generos_libro)
    return generos_libros

# Generar datos para 'generos_audiolibros'
def generate_generos_audiolibros(audiolibros):
    generos_audiolibros = []
    for audiolibro in audiolibros:
        generos = random.sample(["Ficción", "No Ficción", "Misterio", "Romance", "Ciencia Ficción", "Biografia"], random.randint(1, 3))
        for genero in generos:
            generos_audiolibro = {
                "id_audiolibro": audiolibro["id_audiolibro"],
                "genero": genero
            }
            generos_audiolibros.append(generos_audiolibro)
    return generos_audiolibros

# Generar datos para 'escribio' y 'creo'
def generate_escribio(libros, autores):
    escribio = []
    for libro in libros:
        autor = random.choice(autores)
        escribio.append({
            "ISBN": libro["ISBN"],
            "id_autor": autor["id_autor"]
        })
    return escribio

def generate_creo(audiolibros, autores):
    creo = []
    for audiolibro in audiolibros:
        autor = random.choice(autores)
        creo.append({
            "id_audiolibro": audiolibro["id_audiolibro"],
            "id_autor": autor["id_autor"]
        })
    return creo

# Generar datos para 'narro'
def generate_narro(audiolibros, narradores):
    narro = []
    for audiolibro in audiolibros:
        narrador = random.choice(narradores)
        narro.append({
            "id_audiolibro": audiolibro["id_audiolibro"],
            "id_narrador": narrador["id_narrador"]
        })
    return narro

# Generar datos para 'ejemplares'
def generate_ejemplares(libros_fisicos):
    ejemplares = []
    for libro_fisico in libros_fisicos:
        for _ in range(libro_fisico["cant_copias"]):
            ejemplar = {
                "ISBN": libro_fisico["ISBN"],
                "id_ejemplar": random.randint(1000, 9999),
                "pasillo": random.randint(1, 10),
                "estante": random.randint(1, 20),
                "altura": random.randint(1, 5)
            }
            ejemplares.append(ejemplar)
    return ejemplares

# Generar datos para 'usuarios'
def generate_usuarios(n):
    usuarios = []
    for _ in range(n):
        usuario = {
            "DNI": fake.unique.random_int(min=10000000, max=99999999),
            "nombres": fake.first_name(),
            "apellidos": fake.last_name(),
            "direccion": fake.address(),
            "email": fake.email()
        }
        usuarios.append(usuario)
    return usuarios

# Generar datos para 'telefonos_usuarios'
def generate_telefonos_usuarios(usuarios):
    telefonos_usuarios = []
    for usuario in usuarios:
        for _ in range(random.randint(1, 3)):
            telefono_usuario = {
                "DNI": usuario["DNI"],
                "telefono": fake.unique.random_int(min=600000000, max=699999999)
            }
            telefonos_usuarios.append(telefono_usuario)
    return telefonos_usuarios

# Generar datos para 'prestamos'
def generate_prestamos(ejemplares, usuarios, n):
    prestamos = []
    for _ in range(n):
        ejemplar = random.choice(ejemplares)
        usuario = random.choice(usuarios)
        fecha_inicio = fake.date_time_this_decade()
        fecha_devolucion = fake.date_time_between(start_date=fecha_inicio) if random.random() > 0.5 else None ## ???
        prestamo = {
            "id_ejemplar": ejemplar["id_ejemplar"],
            "DNI": usuario["DNI"],
            "fecha_inicio": fecha_inicio,
            "fecha_devolucion": fecha_devolucion,
            "nro_renovacion": random.randint(0, 3)
        }
        prestamos.append(prestamo)
    return prestamos

# Generar datos para 'reservas'
def generate_reservas(libros_fisicos, usuarios, n):
    reservas = []
    for _ in range(n):
        libro_fisico = random.choice(libros_fisicos)
        usuario = random.choice(usuarios)
        reserva = {
            "ISBN": libro_fisico["ISBN"],
            "DNI": usuario["DNI"],
            "fecha": fake.date_time_this_year()
        }
        reservas.append(reserva)
    return reservas

# Generar 5 registros de ejemplo para todas las tablas
example_libros = generate_libros_data(books_df, 5)
example_libros_fisicos = generate_libros_fisicos(example_libros)
example_libros_digitales = generate_libros_digitales(example_libros)
example_audiolibros = generate_audiolibros(example_libros)
example_generos_libros = generate_generos_libros(example_libros)
example_generos_audiolibros = generate_generos_audiolibros(example_audiolibros)

# Generar una lista combinada de personas que pueden ser autores y narradores
autores_narradores = [{"id_persona": i, "nombres": fake.first_name(), "apellidos": fake.last_name(), "fecha_nac": fake.date_of_birth(), "nacionalidad": fake.country()} for i in range(1, 11)]

# Separar autores y narradores
example_autores = [{"id_autor": person["id_persona"], "nombres": person["nombres"], "apellidos": person["apellidos"], "fecha_nac": person["fecha_nac"], "nacionalidad": person["nacionalidad"]} for person in autores_narradores]
example_narradores = [{"id_narrador": person["id_persona"], "nombres": person["nombres"], "apellidos": person["apellidos"], "fecha_nac": person["fecha_nac"], "nacionalidad": person["nacionalidad"]} for person in autores_narradores]

example_escribio = generate_escribio(example_libros, example_autores)
example_creo = generate_creo(example_audiolibros, example_autores)
example_narro = generate_narro(example_audiolibros, example_narradores)

example_ejemplares = generate_ejemplares(example_libros_fisicos)

example_usuarios = generate_usuarios(5)
example_telefonos_usuarios = generate_telefonos_usuarios(example_usuarios)
example_prestamos = generate_prestamos(example_ejemplares, example_usuarios, 5)
example_reservas = generate_reservas(example_libros_fisicos, example_usuarios, 5)

# Mostrar ejemplos generados
example_libros, example_libros_fisicos, example_libros_digitales, example_audiolibros, example_generos_libros, example_generos_audiolibros, example_escribio, example_creo, example_narro, example_ejemplares, example_usuarios, example_telefonos_usuarios, example_prestamos, example_reservas

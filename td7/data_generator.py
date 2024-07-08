from faker import Faker
from faker.providers import address, date_time, internet, company, person
import pandas as pd
import random
from td7.schema import Schema

# Inicializar Faker
fake = Faker()
fake.add_provider(address)
fake.add_provider(date_time)
fake.add_provider(company)
fake.add_provider(person)

# Leer el archivo CSV proporcionado
file_path = 'td7/books.csv'
books_df = pd.read_csv(file_path)

class DataGenerator:
    def __init__(self):
        self.db = Schema()  
# Función para generar datos para la tabla 'editoriales'
    def generate_editoriales(n):
        editoriales = []
        for _ in range(n):
            editorial = {
                "razon_social": fake.unique.company(),
                "nombre": fake.company_suffix(),
                "pais_origen": fake.country(),
                "anio_fundacion": fake.year()
            }
            editoriales.append(editorial)
        return editoriales

    # Función para generar datos para la tabla 'idiomas'
    def generate_idiomas(n):
        idiomas = []
        for _ in range(n):
            idioma = {
                "lang_code": fake.language_code(),
                "nombre_completo": fake.language_name(),
                "pais_origen": fake.country()
            }
            idiomas.append(idioma)
        return idiomas

    def generate_autores(books_df, n):
        autores = []
        books = books_df.sample(n)
        for book in books:
            nombres, apellidos = book["authors"].split(' ', 1)
            autor = {
                "id_autor": fake.incremental_id(),
                "nombres": nombres, 
                "apellidos": apellidos,
                "fecha_nac": fake.date_of_birth(minimum_age=25, maximum_age=70),
                "nacionalidad": fake.country()   
            }  
            autores.append(autor)
        return autores

    def generate_autores(books_df, start_row, end_row):
        autores = []
        books = books_df.iloc[start_row:end_row]
        for book in books:
            nombres, apellidos = book["authors"].split(' ', 1)
            autor = {
                "id_autor": fake.incremental_id(),
                "nombres": nombres, 
                "apellidos": apellidos,
                "fecha_nac": fake.date_of_birth(minimum_age=25, maximum_age=70),
                "nacionalidad": fake.country()   
            }  
            autores.append(autor)
        return autores

    def generate_libro_escribio(idiomas, editoriales, n):
        libros = []
        escribio = []
        generos=[]
        books = books_df.sample(n)
        for book in books:     # ¿Manejan el caso de que el libro ya exista en la base de datos con ese ISBN?
            
            libros.append({
                "ISBN": book["isbn13"],
                "titulo": book["title"],
                "nro_serie": random.randint(1, 10),
                "edicion": random.randint(1, 5),
                "lang_code": random.choice(idiomas)["lang_code"],
                "editorial_razon": random.choice(editoriales)["razon_social"],
                "fecha_publicacion": book["published_year"]
            })
            generos.append({
                    "ISBN": libros["ISBN"], #tengo que linkearlo al libro que corresponde
                    "genero": random.sample(["Ficción", "No Ficción", "Misterio", "Romance", "Ciencia Ficción", "Biografia"], random.randint(1, 3)) #o book["categories"] pero es raro el formato
            })
            
            nombres, apellidos = book["authors"].split(' ', 1)
            autor = Schema.get_autor(nombres, apellidos)
            escribio.append({
                "ISBN": book["isbn13"],
                "id_autor": autor["id_autor"]
            })
        return libros, generos, escribio


    # Generar datos para 'libros_fisicos'
    def generate_libros_fisicos_libros_digitales(libros): 
        libros_fisicos = []
        libros_digitales = []
        #libros = #quedarte con los primeros n/2
        for libro in range(0, length(libros)/2):
            libros_fisicos.append({
                "ISBN": libro["isbn13"],
                "cant_copias": random.randint(1, 20)
            })
        for libro in range(length(libros)/2, lenght(libros)):
            libros_digitales.append({
                "ISBN": libro["isbn13"], #corregir: el libro fisico y digital tienen diferente ISBN aunque sean el mismo libro -> random.randint(1000000000000, 9999999999999)
                "link": fake.url()
            })
        return libros_fisicos, libros_digitales

    # Generar datos para 'libros_digitales'
    # def generate_libros_digitales(libros):
    #     libros_digitales = []
    #     #libros = #quedarte con los restantes n/2
    #     for libro in libros:
    #         libros_digitales.append({
    #             "ISBN": libro["isbn13"], #corregir: el libro fisico y digital tienen diferente ISBN aunque sean el mismo libro -> random.randint(1000000000000, 9999999999999)
    #             "link": fake.url()
    #         })
    #     return libros_digitales


    #-> este esta bien hacer todo de una porque es todo fake y no se tiene que corresponder un narrador con un libro
    # Generar datos para 'audiolibros'
    def generate_audiolibros_narrador(books_df,idiomas,n): #creo y narro ????
        audiolibros = []
        narradores = []
        narro = []
        creo = []
        generos_audiolibros = []
        for _ in range(n):
            book = books_df.sample(1).iloc[0]
            audiolibro = {
                "id_audiolibro": fake.unique.random_int(1000, 9999),
                "titulo": book["title"],
                "duracion": random.randint(60, 1200),  # duración en minutos
                "nro_serie": random.randint(1, 10),
                "lang_code": random.choice(idiomas)["lang_code"],
                "fecha_publicacion": book["published_year"] #fake.date_between(start_date="-50y", end_date="-1y")
            }
            narrador = {
                "id_narrador": random.randint(1000, 9999), #fijarse, un autor puede narrar su propio libro
                "nombres": fake.first_name(),
                "apellidos": fake.last_name(),
                "fecha_nac": fake.date_of_birth(minimum_age=25, maximum_age=70),
                "nacionalidad": fake.country() 
            }
            generos_audiolibro = {
                    "id_audiolibro": audiolibro["id_audiolibro"],
                    "generos": random.sample(["Ficción", "No Ficción", "Misterio", "Romance", "Ciencia Ficción", "Biografia"], random.randint(1, 3))
                }
            narro.append({
                "id_audiolibro": audiolibro["id_audiolibro"],
                "id_narrador": narrador["id_narrador"]
            })
            creo.append({
                "id_audiolibro": audiolibro["id_audiolibro"],
                "id_autor": autor["id_autor"]#tengo que linkearlo al libro que corresponde
            })
            narradores.append(narrador)
            audiolibros.append(audiolibro)
            generos_audiolibros.append(generos_audiolibro)
        return audiolibros, narradores, generos_audiolibros, narro, creo


    # def generate_creo(audiolibros, autores):
    #     creo = []
    #     for audiolibro in audiolibros:
    #         autor = random.choice(autores)
    #         creo.append({
    #             "id_audiolibro": audiolibro["id_audiolibro"],
    #             "id_autor": autor["id_autor"]#tengo que linkearlo al libro que corresponde
    #         })
    #     return creo

    # # Generar datos para 'narro'
    # def generate_narro(audiolibros, narradores):
    #     narro = []
    #     for audiolibro in audiolibros:
    #         narrador = random.choice(narradores)
    #         narro.append({
    #             "id_audiolibro": audiolibro["id_audiolibro"],
    #             "id_narrador": narrador["id_narrador"]#tengo que linkearlo al libro que corresponde
    #         })
    #     return narro

    # Generar datos para 'ejemplares'
    def generate_ejemplares(libros_fisicos):
        ejemplares = []
        for libro_fisico in libros_fisicos:
            for _ in range(libro_fisico["cant_copias"]):
                ejemplar = {
                    "ISBN": libro_fisico["ISBN"],
                    "id_ejemplar": random.randint(1, 100), #incremental !!
                    "pasillo": random.randint(1, 10), 
                    "estante": random.randint(1, 20),
                    "altura": random.randint(1, 5)
                }
                ejemplares.append(ejemplar)
        return ejemplares

    # Generar datos para 'usuarios'
    def generate_usuarios(n):
        usuarios = []
        telefono_usuarios = []
        for _ in range(n):
            usuario = {
                "DNI": fake.unique.random_int(min=3000000, max=99999999),
                "nombres": fake.first_name(),
                "apellidos": fake.last_name(),
                "direccion": fake.address(),
                "email": fake.email()
            }
            telefono_usuario = {
                    "DNI": usuario["DNI"],
                    "telefono": fake.unique.random_int(min=600000000, max=699999999)
                }
            usuarios.append(usuario)
            telefono_usuarios(telefono_usuario)
        return usuarios, telefono_usuario


    # Generar datos para 'prestamos'
    def generate_prestamos(ejemplares, usuarios, n):
        prestamos = []
        for _ in range(n):
            ejemplar = random.choice(ejemplares)
            usuario = random.choice(usuarios)
            fecha_inicio = fake.date_time_this_year() #formato?
            fecha_devolucion = fake.date_time_between(start_date=fecha_inicio) if random.random() > 0.5 else None  #asi algunos prestamos son null
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
example_libros_fisicos = generate_libros_fisicos(books_df, 5)
example_libros_digitales = generate_libros_digitales(books_df, 5)
example_audiolibros = generate_audiolibros(books_df, 5)
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

from faker import Faker
from faker.providers import address, date_time, internet, company, person
import pandas as pd
import random
import json
import datetime
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
with open('languages.json', 'r') as file:
    languages = json.load(file)


class DataGenerator:
    def __init__(self):
        self.db = Schema()  

 #---------------EDITORIALES E IDIOMAS----------------------------

# Función para generar datos para la tabla 'editoriales'
    def generate_editoriales(self, n):
        editoriales = []
        for _ in range(n):
            commercial_name = fake.unique.company()
            legal_suffix = fake.company_suffix()
            legal_name = f"{commercial_name} {legal_suffix}"

            editorial = {
                "razon_social": legal_name, #es el nombre comercial y algún suffix random
                "nombre": commercial_name, #suffix devolvía LLC, Group, etc... y necesitamos un nombre
                "pais_origen": fake.country(),
                "anio_fundacion": fake.random_int(min=1900, max=2023) #year podría haber devuelto 1300 o algo así
            }
            editoriales.append(editorial)
        return editoriales

    # Función para generar datos para la tabla 'idiomas'
    def generate_idiomas(self, n):
        idiomas = []
        
        for _ in range(n):
            lang = random.choice(languages)  # Selecciona un idioma al azar del JSON
            lang_code = lang['lang_code']
            idioma = {
                "lang_code": lang_code,  # Usar el código del idioma del JSON
                "nombre_completo": lang['language'],
                "pais_origen": lang['country']
            }
            idiomas.append(idioma)
        
        return idiomas


#---------------------------GENEROS----------------------------

    def generate_generos(self):
        genero = ""
        if random.random() < 0.5:  
            genero = random.sample((["No Ficción", "Biografía"]), 1) 
        else:
            genero = random.sample((["Ficción", "Misterio", "Romance", "Ciencia Ficción"]), random.randint(1, 2))
        return genero
    
#------------------------AUTORES------------------------------------

    def generate_autores(self):
        #chequear que no esté cargado el autor (para no asignarle 2 id) - hecho
        #chequear que no esté asignado el id
        autores = []
        autores_lote_actual = {} #registro de los autores que cargue en el lote y que todavia no estan en el esquema

        max_id_autor = self.db.get_max_id_autor()
        if max_id_autor is None:
            max_id_autor = 0  # Asignar 0 si no hay autores en el esquema

        next_autor_id = max_id_autor + 1  # ID inicial para el nuevo lote

        for book in books_df:
            autores = book["authors"].split(';') #puede haber más de un autor por libro
            for autor in autores:
                nombres, apellidos = book["authors"].split(' ', 1)

                autor_existente_schema = self.db.get_autor(nombres, apellidos)
                autor_existente_lote = autores_lote_actual.get((nombres, apellidos))

                if autor_existente_schema:
                    autor_id = autor_existente_schema["id_autor"]
                else:
                    if autor_existente_lote:
                        autor_id = autor_existente_lote["id_autor"]
                    else:
                        autor_id = next_autor_id 
                        next_autor_id += 1
                        autores_lote_actual[(nombres, apellidos)] = {"id_autor": autor_id}

                fecha_in = datetime(year=1850, month=1, day=1),
                fecha_end = datetime(year=2000, month=12, day=31),
                autor = {
                    "id_autor": autor_id,
                    "nombres": nombres, 
                    "apellidos": apellidos,
                    "fecha_nac": fake.date_time_between(start_date=fecha_in, end_date=fecha_end), #cambio de modo age a year porque podrían estar muertos (y habría que ajustar la edad hipotética todos los años)
                    "nacionalidad": fake.country()   
                }  
                autores.append(autor)
        return autores

#----------------------------LIBROS---------------------------------

    def generate_libros(self, idiomas, editoriales, n):
        libros = []
        generos=[]
        escribio = []
        
        books = books_df.sample(n)

        for book in books:     # ¿Manejan el caso de que el libro ya exista en la base de datos con ese ISBN?
            generos.append({
                "ISBN": book["isbn13"], #tengo que linkearlo al libro que corresponde
                "genero": self.generate_generos() # para que no se solapen no ficcion y ficcion
            })
            
            libros.append({
                "ISBN": book["isbn13"],
                "titulo": book["title"],
                "nro_serie": random.randint(1, 10),
                "edicion": random.randint(1, 5),
                "lang_code": random.choice(idiomas)["lang_code"],
                "editorial_razon": random.choice(editoriales)["razon_social"],
                "fecha_publicacion": book["published_year"]
            })

            autores = book["authors"].split(';')  # Separar los autores por punto y coma si hay más de uno
            for autor in autores:
                nombres, apellidos = autor.split(' ', 1)  # Dividir el nombre y apellido del autor
                autor = self.db.get_autor(nombres, apellidos)  # Obtener el autor desde alguna función o método Schema
                escribio.append({
                    "ISBN": book["isbn13"],
                    "id_autor": autor["id_autor"]
                })

        return libros, generos, escribio

   
#-------------------FISICOS Y DIGITALES------------------------------

    # Generar datos para 'libros_fisicos' y 'libros_digitales'
    def generate_libros_fyd(self, libros): 
        libros_fisicos_count = int(len(libros) * 0.85)
        offset = max(int(len(libros) * 0.02), 1)
       
        random.shuffle(libros)

        libros_fisicos = self.generate_libros_fisicos(libros, libros_fisicos_count)
        libros_digitales = self.generate_libros_digitales(libros, libros_fisicos_count, offset)
        
        return libros_fisicos, libros_digitales
    
    def generate_libros_fisicos(self, libros, count):
        libros_fisicos = []

        for i in range(count):
            libros_fisicos.append({
                "ISBN": libros[i]["ISBN"],
                "cant_copias": random.randint(1, 20)
            })
        
        return libros_fisicos
    
    def generate_libros_digitales(libros, count, offset):
        libros_digitales = []

        for i in range(count - offset, len(libros)):
            isbn = libros[i]["ISBN"]
            nuevo_isbn = isbn[:2] + '6' + isbn[3:]
            libros_digitales.append({
                "ISBN": nuevo_isbn,
                "link": fake.url()
            })

        return libros_digitales

       
#-------------------AUDIOLIBROS------------------------------

    # Generar datos para 'audiolibros'
    def generate_audiolibros(self, idiomas, n): #creo y narro ????
        audiolibros = []
        narradores = []
        narro = []
        creo = []
        
        generos_audiolibros = []
        for _ in range(n):
            book = books_df.sample(n)
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
                    "genero": self.generate_generos()
                }
            
            narro.append({
                "id_audiolibro": audiolibro["id_audiolibro"],
                "id_narrador": narrador["id_narrador"]
            })

            autores = book["authors"].split(';')  # Separar los autores por punto y coma si hay más de uno
            for autor in autores:
                nombres, apellidos = autor.split(' ', 1)  # Dividir el nombre y apellido del autor
                autor = self.db.get_autor(nombres, apellidos)  # Obtener el autor desde alguna función o método Schema

            creo.append({
                "id_audiolibro": audiolibro["id_audiolibro"],
                "id_autor": autor["id_autor"]#tengo que linkearlo al libro que corresponde
            })

            narradores.append(narrador)
            audiolibros.append(audiolibro)
            generos_audiolibros.append(generos_audiolibro)

        return audiolibros, narradores, generos_audiolibros, narro, creo

#-------------------EJEMPLARES-----------------------------

    def generate_ejemplares(self, libros_fisicos):
        ejemplares = []
        for libro_fisico in libros_fisicos:
            #para solucionar que ejemplares de un libro estén cercanos, definimos pasillo, estante y altura por fuera de cant_copias, y simplificamos si decimos que estan exactamente en el mismo (que no puedan correr de estante o altura x ej)
            pasillo = random.randint(1, 10), 
            estante = random.randint(1, 20),
            altura = random.randint(1, 5)
            for _ in range(libro_fisico["cant_copias"]):
                ejemplar = {
                    "ISBN": libro_fisico["ISBN"],
                    "id_ejemplar": fake.incremental_id(),
                    "pasillo": pasillo, 
                    "estante": estante,
                    "altura": altura
                }
                ejemplares.append(ejemplar)
        return ejemplares

#-------------------USUARIOS-----------------------------

    # Generar datos para 'usuarios'
    def generate_usuarios(self, n):
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
                    "telefono": fake.unique.random_int(min=60000000, max=69999999)
                }
            usuarios.append(usuario)
            telefono_usuarios(telefono_usuario)
        return usuarios, telefono_usuario

#-------------------PRESTAMOS Y RESERVAS-----------------------------

    # Generar datos para 'prestamos'
    def generate_prestamos(self, ejemplares, usuarios, n):
        prestamos = []
        ejemplares_seleccionados = random.sample(ejemplares, n)
        for ejemplar in ejemplares_seleccionados:
            usuario = random.choice(usuarios)
            fecha_inicio = fake.date_time_this_year() #formato?
            fecha_devolucion = fake.date_time_between(start_date=fecha_inicio) if random.random() > 0.5 else None  #asi algunos prestamos son null
            prestamo = {
                "id_ejemplar": ejemplar["id_ejemplar"],
                "DNI": usuario["DNI"],
                "fecha_inicio": fecha_inicio,
                "fecha_devolucion": fecha_devolucion, 
                "nro_renovacion": random.randint(0, 3) # y no guardamos mas viejos? tipo va x la nro 2 pero nunca tuvo la 0 y la 1 registrada
            }
            prestamos.append(prestamo)
        return prestamos

    # Generar datos para 'reservas'
    def generate_reservas(self, libros_fisicos, usuarios, n):
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



from faker import Faker
from faker.providers import address, date_time, internet, company, person
import pandas as pd
import random
import json
import datetime
from datetime import datetime,timedelta
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
with open('td7/languages.json', 'r') as file:
    languages = json.load(file)


class DataGenerator:
    def __init__(self):
        self.db = Schema()  

 #---------------EDITORIALES E IDIOMAS----------------------------

# Función para generar datos para la tabla 'editoriales'
    def generate_editoriales(self, n):
        editoriales = []
        editoriales_elegidas = set()
        for _ in range(n):
            commercial_name = fake.unique.company()
            if commercial_name not in editoriales_elegidas:
                legal_suffix = fake.company_suffix()
                legal_name = f"{commercial_name} {legal_suffix}"

                editorial = {
                    "razon_social": legal_name, #es el nombre comercial y algún suffix random
                    "nombre": commercial_name, #suffix devolvía LLC, Group, etc... y necesitamos un nombre
                    "pais_origen": fake.country(),
                    "anio_fundacion": fake.random_int(min=1900, max=2023) #year podría haber devuelto 1300 o algo así
                }
                editoriales.append(editorial)
                editoriales_elegidas.add(commercial_name)
        return editoriales

    # Función para generar datos para la tabla 'idiomas'
    def generate_idiomas(self, n):
        idiomas = []
        codigos_usados = set()
        for _ in range(n):
            lang = random.choice(languages)  # Selecciona un idioma al azar del JSON
            if lang['lang_code'] not in codigos_usados:
                lang_code = lang['lang_code']
                idioma = {
                    "lang_code": lang_code,  # Usar el código del idioma del JSON
                    "nombre_completo": lang['language'],
                    "pais_origen": lang['country']
                }
                codigos_usados.add(lang['lang_code'])
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

    def generate_autores(self, start, end):
        #chequear que no esté cargado el autor (para no asignarle 2 id) - hecho
        #chequear que no esté asignado el id
        autores_final = []
        autores_lote_actual = {} #registro de los autores que cargue en el lote y que todavia no estan en el esquema

        max_id_autor = self.db.get_max_id_autor()
        if max_id_autor is None:
            max_id_autor = 0  # Asignar 0 si no hay autores en el esquema

        next_autor_id = max_id_autor + 1  # ID inicial para el nuevo lote
        books = books_df.iloc[start : end]

        for index, book in books.iterrows():
            if isinstance(book["authors"], float):
                nombres, apellidos = "A","A"

            else:
                authors_cleaned = book["authors"].replace("'", "")
                autores = authors_cleaned.split(';') if authors_cleaned else ['']
                for autor in autores:
                    names = autor.split(' ', 1)
                    if len(names) < 2:
                        nombres = names[0]  # Assign the entire name as nombres
                        apellidos = "A"      
                    else:
                        nombres, apellidos = names

                    autor_existente_schema = self.db.get_autor(nombres, apellidos)
                    autor_existente_lote = autores_lote_actual.get((nombres, apellidos))

                    if autor_existente_schema or autor_existente_lote:
                        continue
                    else:
                        autor_id = next_autor_id 
                        next_autor_id += 1
                        autores_lote_actual[(nombres, apellidos)] = {"id_autor": autor_id}

                    fecha_in = datetime(year=1850, month=1, day=1)
                    fecha_end = datetime(year=2000, month=12, day=31)
                    autores_final.append({
                        "id_autor": autor_id,
                        "nombres": nombres, 
                        "apellidos": apellidos,
                        "fecha_nac": fake.date_time_between(fecha_in, fecha_end).strftime('%Y %m %d'), #cambio de modo age a year porque podrían estar muertos (y habría que ajustar la edad hipotética todos los años)
                        "nacionalidad": fake.country()   
                    }) 
        return autores_final

#----------------------------LIBROS---------------------------------

    def generate_libros(self, idiomas, editoriales, start, end):
        libros = []
        generos=[]
        escribio = []
        
        books = books_df.iloc[start : end]

        for index, book in books.iterrows():     # ¿Manejan el caso de que el libro ya exista en la base de datos con ese ISBN?
            generos.append({
                "isbn": int(book["isbn13"]), #tengo que linkearlo al libro que corresponde
                "genero": self.generate_generos() # para que no se solapen no ficcion y ficcion
            })
            
            libros.append({
                "isbn": int(book["isbn13"]),
                "titulo": book["title"],
                "nro_serie": random.randint(1, 10),
                "edicion": random.randint(1, 5),
                "lang_code": random.choice(idiomas),
                "editorial_razon": random.choice(editoriales),
                "fecha_publicacion": int(book["published_year"])
            })

            
            if isinstance(book["authors"], float):
                nombres, apellidos = "A","A"

            else:
                authors_cleaned = book["authors"].replace("'", "")
                autores = authors_cleaned.split(';') if authors_cleaned else ['']
            
            for autor in autores:
                names = autor.split(' ', 1)
                if len(names) < 2:
                    nombres = names[0]  # Assign the entire name as nombres
                    apellidos = "A"      
                else:
                    nombres, apellidos = names

                autor_actual = self.db.get_autor(nombres, apellidos) # Obtener el autor desde alguna función o método Schema
                
                escribio.append({
                    "isbn": int(book["isbn13"]),
                    "id_autor": int(autor_actual)
                })

        return libros, generos, escribio

   
#-------------------FISICOS Y DIGITALES------------------------------

    # Generar datos para 'libros_fisicos' y 'libros_digitales'
    def generate_libros_fyd(self, start, end): 
        
        libros = self.db.get_libros(start, end)
        libros_fisicos_count = int(len(libros) * 0.85)
        offset = max(int((len(libros) * 0.02)), 1)
       
        random.shuffle(libros)

        libros_fisicos = self.generate_libros_fisicos(libros, libros_fisicos_count)
        libros_digitales = self.generate_libros_digitales(libros, libros_fisicos_count, offset)
        
        return libros_fisicos, libros_digitales
    
    def generate_libros_fisicos(self, libros, count):
        libros_fisicos = []

        for i in range(count):
            libros_fisicos.append({
                "isbn": libros[i]["isbn"],
                "cant_copias": random.randint(1, 20)
            })
        
        return libros_fisicos
    
    def generate_libros_digitales(self, libros, count, offset):
        libros_digitales = []

        for i in range(count - offset, len(libros)):
            isbn = libros[i]["isbn"]

            libros_digitales.append({
                "isbn": isbn,
                "link": fake.url()
            })

        return libros_digitales

       
#-------------------AUDIOLIBROS------------------------------

    # Generar datos para 'audiolibros'
    def generate_audiolibros(self, idiomas, start, end): 
        audiolibros = []
        narradores = []
        narro = []
        creo = []
        generos_audiolibros = []

        n = 10
        libros = self.db.get_libros(start, end)
        
        for book in random.sample(libros, n):
            audiolibro = {
                "id_audiolibro": fake.unique.random_int(1000, 9999),
                "titulo": book["titulo"],
                "duracion": random.randint(60, 1200),  # duración en minutos
                "nro_serie": random.randint(1, 10),
                "lang_code": random.choice(idiomas),
                "fecha_publicacion": int(book["fecha_publicacion"]) #fake.date_between(start_date="-50y", end_date="-1y")
            }

            narrador = {
                "id_narrador": random.randint(1000, 9999), #fijarse, un autor puede narrar su propio libro
                "nombres": fake.first_name(),
                "apellidos": fake.last_name(),
                "fecha_nac": fake.date_of_birth(minimum_age=25, maximum_age=70),
                "nacionalidad": fake.country() 
            }

            generos_audiolibros.append({
                    "id_audiolibro": audiolibro["id_audiolibro"],
                    "genero": self.generate_generos()
                })
            
            narro.append({
                "id_audiolibro": audiolibro["id_audiolibro"],
                "id_narrador": narrador["id_narrador"]
            })
            escribio = self.db.get_escribio(book["isbn"])
            for id_autor in escribio:
                creo.append({
                    "id_audiolibro": audiolibro["id_audiolibro"],
                    "id_autor": id_autor #tengo que linkearlo al libro que corresponde
                })

            narradores.append(narrador)
            audiolibros.append(audiolibro)

        return audiolibros, narradores, generos_audiolibros, narro, creo

#-------------------EJEMPLARES-----------------------------

    def generate_ejemplares(self, start, end):
        ejemplares = []
        max_id_ejemplar = self.db.get_max_id_ejemplar()
        if max_id_ejemplar is None:
            max_id_ejemplar = 0  # Asignar 0 si no hay autores en el esquema

        next_ejemplar_id = max_id_ejemplar + 1 

        libros_fisicos = self.db.get_libros_fisicos(start=start, end=end)
        for libro_fisico in libros_fisicos:
            #para solucionar que ejemplares de un libro estén cercanos, definimos pasillo, estante y altura por fuera de cant_copias, y simplificamos si decimos que estan exactamente en el mismo (que no puedan correr de estante o altura x ej)
            pasillo = random.randint(1, 10), 
            estante = random.randint(1, 20),
            altura = random.randint(1, 5)
            for _ in range(libro_fisico["cant_copias"]):
                ejemplar = {
                    "isbn": libro_fisico["isbn"],
                    "id_ejemplar": next_ejemplar_id,
                    "pasillo": pasillo, 
                    "estante": estante,
                    "altura": altura
                }
                ejemplares.append(ejemplar)
                next_ejemplar_id += 1
        return ejemplares

#-------------------USUARIOS-----------------------------

    # Generar datos para 'usuarios'
    def generate_usuarios(self, n):
        usuarios = []
        telefono_usuarios = []
        for _ in range(n):
            nombres = fake.first_name()
            apellidos = fake.last_name()
            usuario = {
                "dni": fake.unique.random_int(min=3000000, max=99999999),
                "nombres": nombres,
                "apellidos": apellidos,
                "direccion": fake.address(),
                "email": f"{nombres[0].lower()}{apellidos.lower()}@example.com"
            }
            telefono_usuario = {
                    "dni": usuario["dni"],
                    "telefono": fake.unique.random_int(min=60000000, max=69999999)
                }
            usuarios.append(usuario)
            telefono_usuarios.append(telefono_usuario)
        return usuarios, telefono_usuarios

#-------------------PRESTAMOS Y RESERVAS-----------------------------

    # Generar datos para 'prestamos'
    def generate_prestamos(self, ejemplares, usuarios, n):
        prestamos = []
        ejemplares_seleccionados = random.sample(ejemplares, n)
        for ejemplar in ejemplares_seleccionados:
            usuario = random.choice(usuarios)
            fecha_inicio = fake.date_time_this_year() #formato?
            fecha_vencimiento = fecha_inicio + timedelta(days = 7)
            fecha_devolucion = fake.date_time_between(fecha_inicio, fecha_vencimiento) if random.random() > 0.5 else None  #asi algunos prestamos son null
            prestamos.append({
                "id_ejemplar": ejemplar["id_ejemplar"],
                "dni": int(usuario["dni"]),
                "fecha_inicio": fecha_inicio,
                "fecha_devolucion": fecha_devolucion, 
                "fecha_vencimiento" : fecha_vencimiento,
                "nro_renovacion": random.randint(0, 3) # y no guardamos mas viejos? tipo va x la nro 2 pero nunca tuvo la 0 y la 1 registrada
            })
        return prestamos

    # Generar datos para 'reservas'
    def generate_reservas(self, libros_fisicos, usuarios, n):
        reservas = []
        for _ in range(n):
            libro_fisico = random.choice(libros_fisicos)
            usuario = random.choice(usuarios)
            reservas.append({
                "isbn": libro_fisico["isbn"],
                "dni": int(usuario["dni"]),
                "fecha": fake.date_time_this_year()
            })
        return reservas



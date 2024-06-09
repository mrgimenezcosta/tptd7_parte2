-- Creamos tablas

create table if not exists autores(
	id_autor int PRIMARY KEY, 
	nombres varchar(50),
	apellidos varchar(50),
	fecha_nac date,
	nacionalidad varchar(50)
);

create table if not exists narradores(
	id_narrador int PRIMARY KEY, 
	nombres varchar(50),
	apellidos varchar(50),
	fecha_nac date,
	nacionalidad varchar(50)
);

create table if not exists idiomas(
	lang_code varchar(5) PRIMARY KEY, -- clave subrogada
	nombre_completo varchar(30),
	pais_origen varchar(30)
);

create table if not exists editoriales(
	razon_social varchar(30) PRIMARY KEY, -- clave subrogada
	nombre varchar(30),
	pais_origen varchar(30),
	anio_fundacion numeric(4)
);

create table if not exists libros(
    ISBN bigint,
    titulo varchar(50),
    nro_serie int,
    edicion int,
    lang_code varchar(5) NOT NULL, --language code (EN-au, ES-ar, FR,...)
    editorial_razon varchar(30) NOT NULL,
    fecha_publicacion DATE,
    PRIMARY KEY(ISBN),
    FOREIGN KEY(lang_code) REFERENCES idiomas,
    FOREIGN KEY(editorial_razon) REFERENCES editoriales
);

create table if not exists libros_fisicos(
    ISBN bigint,
    cant_copias int,
    PRIMARY KEY(ISBN),
    FOREIGN KEY (ISBN) REFERENCES libros
);

create table if not exists libros_digitales(
    ISBN bigint,
    link varchar(200),
    PRIMARY KEY(ISBN),
    FOREIGN KEY (ISBN) REFERENCES libros
);


create table if not exists audiolibros(
    id_audiolibro int,
    titulo varchar(50),
    duracion int,
    nro_serie int,
    lang_code varchar(5) NOT NULL,
    fecha_publicacion DATE,
    PRIMARY KEY(id_audiolibro),
    FOREIGN KEY(lang_code) REFERENCES idiomas
);

create table if not exists generos_libros(
    ISBN bigint,
    genero varchar(50),
    PRIMARY KEY(ISBN, genero),
    FOREIGN KEY(ISBN) REFERENCES libros
);


create table if not exists generos_audiolibros(
    id_audiolibro int,
    genero varchar(50),
    PRIMARY KEY(id_audiolibro, genero),
    FOREIGN KEY(id_audiolibro) REFERENCES audiolibros
);

create table if not exists escribio(
    ISBN bigint,
    id_autor int,
    PRIMARY KEY(ISBN, id_autor),
    FOREIGN KEY(ISBN) REFERENCES libros,
    FOREIGN KEY(id_autor) REFERENCES autores
);

create table if not exists creo(
    id_audiolibro int,
    id_autor int,
    PRIMARY KEY(id_audiolibro, id_autor),
    FOREIGN KEY(id_audiolibro) REFERENCES audiolibros,
    FOREIGN KEY(id_autor) REFERENCES autores
);

create table if not exists narro(
    id_audiolibro int,
    id_narrador int,
    PRIMARY KEY(id_audiolibro, id_narrador),
    FOREIGN KEY(id_audiolibro) REFERENCES audiolibros,
    FOREIGN KEY(id_narrador) REFERENCES narradores
);

create table if not exists ejemplares(
    ISBN bigint NOT NULL,
    id_ejemplar int,
    pasillo int, --
    estante int, --
    altura int,  --
    PRIMARY KEY(id_ejemplar),
    FOREIGN KEY(ISBN) REFERENCES libros_fisicos
);


create table if not exists usuarios(
    DNI numeric(8),
    nombres varchar(50),
	apellidos varchar(50),
	direccion varchar(50),
	email varchar(50),
	PRIMARY KEY(DNI)
);

create table if not exists telefonos_usuarios(
    DNI numeric(8),
    telefono bigint,
    PRIMARY KEY(DNI, telefono),
    FOREIGN KEY(DNI) REFERENCES usuarios
);

create table if not exists prestamos(
   	id_ejemplar int,
    DNI numeric(8),
	fecha_inicio timestamp, --TIMESTAMP
	fecha_devolucion timestamp,
	nro_renovacion int,
	PRIMARY KEY(id_ejemplar, DNI, fecha_inicio),
    FOREIGN KEY(id_ejemplar) REFERENCES ejemplares,
    FOREIGN KEY(DNI) REFERENCES usuarios,
   	CONSTRAINT check_fecha_valida CHECK ((fecha_devolucion is not null and fecha_inicio <= fecha_devolucion) OR (fecha_devolucion is null)),
    CONSTRAINT check_renovaciones_vigentes CHECK (nro_renovacion <= 3)
);

create table if not exists reservas(
	ISBN bigint,
	DNI numeric(8),
	fecha timestamp,
	PRIMARY KEY (ISBN, DNI, fecha),
    FOREIGN KEY(ISBN) REFERENCES libros_fisicos,
    FOREIGN KEY(DNI) REFERENCES usuarios
);

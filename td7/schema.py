from typing import Optional, Dict, List, Any

from td7.custom_types import Records
from td7.database import Database

class Schema:
    def __init__(self):
        self.db = Database()        
    
    def get_idiomas(self, sample_n: Optional[int] = None) -> List[str]:
        query = "SELECT * FROM idiomas"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        records =  self.db.run_select(query)
        idiomas= [record['lang_code'] for record in records]
        return idiomas

    def get_editoriales(self, sample_n: Optional[int] = None) -> List[str]:
        query = "SELECT * FROM editoriales"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        records =  self.db.run_select(query)
        editoriales = [record['razon_social'] for record in records]
        return editoriales
    
    def get_autor(self, nombres: str, apellidos: str):
        query = f"""
        SELECT id_autor 
        FROM autores
        WHERE nombres = '{nombres}' AND apellidos = '{apellidos}'
        """
        result = self.db.run_select(query)
        
        if result:
            return result[0]["id_autor"] 
        else:
            return None

    def get_max_id_autor(self):
        query = """
        SELECT MAX(id_autor) AS max_id 
        FROM autores
        """
        result = self.db.run_select(query)
        if result and result[0]['max_id'] is not None:
            return result[0]['max_id']
        else:
            return None

    def get_narradores(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM narradores"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_libros(self, start, end) -> Records:
        query = "SELECT * FROM libros"
        query += f" LIMIT {end}"
        query += f" OFFSET {start}"
        return self.db.run_select(query)

    def get_libros_fisicos(self, sample_n: Optional[int] = None, start: Optional[int] = None, end: Optional[int] = None) -> Records:
        query = "SELECT * FROM libros_fisicos"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        if start is not None and end is not None:
            query += f" LIMIT {end}"
            query += f" OFFSET {start}"
        return self.db.run_select(query)

    def get_libros_digitales(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM libros_digitales"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_audiolibros(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM audiolibros"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_generos_libros(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM generos_libros"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_generos_audiolibros(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM generos_audiolibros"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

        
    def get_escribio(self, isbn, sample_n: Optional[int] = None) ->  List[int]:
        query = f"SELECT id_autor FROM escribio WHERE isbn='{isbn}'"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        records =  self.db.run_select(query)
        ids = [record['id_autor'] for record in records]
        return ids

    def get_creo(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM creo"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_narro(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM narro"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_ejemplares(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM ejemplares"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)
    
    def get_max_id_ejemplar(self):
        query = """
        SELECT MAX(id_ejemplar) AS max_id 
        FROM ejemplares
        """
        result = self.db.run_select(query)
        if result and result[0]['max_id'] is not None:
            return result[0]['max_id']
        else:
            return None

    def get_usuarios(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM usuarios"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_telefonos_usuarios(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM telefonos_usuarios"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_prestamos(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM prestamos"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_reservas(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM reservas"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def insert(self, records: Records, table: str):
        self.db.run_insert(records, table)
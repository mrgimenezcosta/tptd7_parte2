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
    
    def get_autor(self, nombres: str, apellidos: str) -> Optional[Dict[str, Any]]:
        query = """
        SELECT id_autor 
        FROM autores
        WHERE nombres = %s AND apellidos = %s
        """
        params = (nombres, apellidos)
        result = self.db.run_select(query, params)
        
        if result:
            return result[0]
        else:
            return None


    def get_narradores(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM narradores"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_libros(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM libros"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_libros_fisicos(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM libros_fisicos"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
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

    def get_escribio(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM escribio"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

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

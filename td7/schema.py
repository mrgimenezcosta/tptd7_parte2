from typing import Optional

from td7.custom_types import Records
from td7.database import Database

class Schema:
    def __init__(self):
        self.db = Database()        
    
    def get_people(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM people"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        
        return self.db.run_select(query)

    def get_sessions(self) -> Records:
        return self.db.run_select("SELECT * FROM sessions")
    
    def insert(self, records: Records, table: str):
        self.db.run_insert(records, table)
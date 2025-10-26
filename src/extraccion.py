import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime



class Extraccion:
    
    def __init__(self, uri: str, database: str, nombre_archivo: str = "logs_extraccion.txt"):
        
        ruta_base = os.path.dirname(os.path.abspath(__file__))
        carpeta_logs = os.path.join(ruta_base, "Logs")
        os.makedirs(carpeta_logs, exist_ok=True)
        self.archivo_log = os.path.join(carpeta_logs, nombre_archivo)
        
        self.uri = uri
        self.database = database
        self.db = None

    def conectar_mongodb(self):
        try:
            client = MongoClient(self.uri)
            self.db = client[self.database]
            self.db.list_collection_names()
            print(f"Conexion exitosa: {self.database}")
            self.registrar_log("Conexion", 0, True)
            return self.db
        except Exception as e:
            print(f"Error de Conexion: {e}")
            self.registrar_log("Conexion", 0, False)
            return None
    
    def obtener_datos(self, nombre_coleccion: str, limite: int = 0):
        if self.db is None:
            print("Error de Conexion")
            self.registrar_log(nombre_coleccion, 0, False)
            return pd.DataFrame()

        try:
            coleccion = self.db[nombre_coleccion]
            cursor = coleccion.find().limit(limite) if limite > 0 else coleccion.find()
            datos = list(cursor)
            df = pd.DataFrame(datos)
            
            if df.empty:
                print(f"Error'{nombre_coleccion}' no encontrada o sin registros.")
                self.registrar_log(nombre_coleccion, 0, False)
            else:
                print(f"Extraccion '{nombre_coleccion}' ({len(df)} registros)")
                self.registrar_log(nombre_coleccion, len(df), True)
            return df
        
        except Exception as e:
            print(f"Error '{nombre_coleccion}': {e}")
            self.registrar_log(nombre_coleccion, 0, False)
            return pd.DataFrame()
        
    def registrar_log(self, coleccion, cantidad, estado):
        try:
            with open(self.archivo_log, "a", encoding="utf-8") as archivo:
                log_entry = (
                    f"{datetime.now().isoformat()} | "
                    f"Colección: {coleccion} | "
                    f"Cantidad: {cantidad} | "
                    f"Estado: {'Éxito' if estado else 'Error'}\n"
                )
                archivo.write(log_entry)
        except Exception as err:
            print(f"No se pudo registrar el log: {err}")
import pandas as pd
import os
import pyodbc
from datetime import datetime

class Carga:
    def __init__(self, servidor, base_datos, usuario, password, nombre_archivo_log="logs_carga.txt"):
        self.servidor = servidor
        self.base_datos = base_datos
        self.usuario = usuario
        self.password = password
        self.conexion = None

        ruta_base = os.path.dirname(os.path.abspath(__file__))
        carpeta_logs = os.path.join(ruta_base, "Logs")
        os.makedirs(carpeta_logs, exist_ok=True)
        self.archivo_log = os.path.join(carpeta_logs, nombre_archivo_log)

    # ---------------------------------------
    # 1️⃣ Conexión a SQL Server
    # ---------------------------------------
    def conectar_sqlserver(self):
        try:
            self.conexion = pyodbc.connect(
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.servidor};"
                f"DATABASE={self.base_datos};"
                f"UID={self.usuario};"
                f"PWD={self.password}"
            )
            self.registrar_log("Conexión a SQL Server", True)
            return True
        except Exception as e:
            print(f"❌ Error de conexión: {e}")
            self.registrar_log(f"Error de conexión: {e}", False)
            return False

    # ---------------------------------------
    # 2️⃣ Carga de DataFrames a SQL Server
    # ---------------------------------------
    def cargar_a_sqlserver(self, df: pd.DataFrame, nombre_tabla: str):
        if self.conexion is None:
            print("❌ No hay conexión activa con SQL Server")
            return False

        cursor = self.conexion.cursor()
        try:
            # Crear tabla dinámica según columnas del DataFrame
            columnas = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
            sql_crear = f"IF OBJECT_ID('{nombre_tabla}', 'U') IS NOT NULL DROP TABLE {nombre_tabla}; CREATE TABLE {nombre_tabla} ({columnas});"
            cursor.execute(sql_crear)

            # Insertar filas
            for _, fila in df.iterrows():
                valores = [str(x) if x is not None else None for x in fila]
                placeholders = ", ".join(["?" for _ in valores])
                cursor.execute(f"INSERT INTO {nombre_tabla} VALUES ({placeholders})", valores)

            self.conexion.commit()
            print(f"✅ Datos cargados en tabla '{nombre_tabla}' ({len(df)} registros)")
            self.registrar_log(f"Carga tabla {nombre_tabla}", True)
            return True
        except Exception as e:
            print(f"❌ Error al cargar {nombre_tabla}: {e}")
            self.registrar_log(f"Error carga {nombre_tabla}: {e}", False)
            return False

    # ---------------------------------------
    # 3️⃣ Verificar cantidad de registros
    # ---------------------------------------
    def verificar_carga(self, nombre_tabla: str):
        if self.conexion is None:
            return 0
        try:
            cursor = self.conexion.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
            count = cursor.fetchone()[0]
            return count
        except Exception:
            return 0

    # ---------------------------------------
    # 4️⃣ Exportar a Excel
    # ---------------------------------------
    def exportar_a_excel(self, df: pd.DataFrame, ruta_salida: str):
        try:
            df.to_excel(ruta_salida, index=False)
            self.registrar_log(f"Exportación Excel {ruta_salida}", True)
            return True
        except Exception as e:
            print(f"❌ Error al exportar a Excel: {e}")
            self.registrar_log(f"Error exportar Excel: {e}", False)
            return False

    # ---------------------------------------
    # 5️⃣ Cerrar conexión
    # ---------------------------------------
    def cerrar_conexion(self):
        if self.conexion:
            self.conexion.close()
            print("🔌 Conexión cerrada")

    # ---------------------------------------
    # 6️⃣ Log interno
    # ---------------------------------------
    def registrar_log(self, mensaje, exito):
        try:
            with open(self.archivo_log, "a", encoding="utf-8") as archivo:
                estado = "Éxito" if exito else "Error"
                archivo.write(f"{datetime.now().isoformat()} | {mensaje} | {estado}\n")
        except Exception:
            pass

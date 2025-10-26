import pandas as pd
import numpy as np
import os
from datetime import datetime


class Transformacion:
    """
    Clase encargada de aplicar transformaciones de limpieza y normalización
    a los datos extraídos de MongoDB para Airbnb.
    """

    def __init__(self, nombre_archivo_log="logs_transformacion.txt"):
        ruta_base = os.path.dirname(os.path.abspath(__file__))
        carpeta_logs = os.path.join(ruta_base, "Logs")
        os.makedirs(carpeta_logs, exist_ok=True)
        self.archivo_log = os.path.join(carpeta_logs, nombre_archivo_log)

    # ------------------------------------------
    # UTILIDAD: REGISTRAR LOG
    # ------------------------------------------
    def registrar_log(self, mensaje):
        try:
            with open(self.archivo_log, "a", encoding="utf-8") as log:
                log.write(f"{datetime.now().isoformat()} | {mensaje}\n")
        except Exception as e:
            print(f"No se pudo escribir el log: {e}")

    # ------------------------------------------
    # TRANSFORMACIÓN DE LISTINGS
    # ------------------------------------------
    def transformar_listings(self, df):
        if df.empty:
            print("⚠️ No hay datos en LISTINGS para transformar")
            return df

        self.registrar_log("Inicio transformación de LISTINGS")

        df = df.copy()

        # Eliminar duplicados por _id
        if "_id" in df.columns:
            df = df.drop_duplicates(subset=["_id"])
        
        # Normalizar columnas numéricas
        if "price" in df.columns:
            df["price"] = (
                df["price"].astype(str)
                .str.replace("[^0-9.]", "", regex=True)
                .replace("", np.nan)
                .astype(float)
            )

        # Rellenar nulos
        for col in ["bedrooms", "beds", "bathrooms_text"]:
            if col in df.columns:
                df[col] = df[col].fillna(0)

        # Convertir host_since a datetime
        if "host_since" in df.columns:
            df["host_since"] = pd.to_datetime(df["host_since"], errors="coerce")

        # Agregar columna de timestamp de transformación
        df["fecha_transformacion"] = datetime.now()

        self.registrar_log(f"LISTINGS transformado: {len(df)} registros finales")
        return df

    # ------------------------------------------
    # TRANSFORMACIÓN DE REVIEWS
    # ------------------------------------------
    def transformar_reviews(self, df):
        if df.empty:
            print("⚠️ No hay datos en REVIEWS para transformar")
            return df

        self.registrar_log("Inicio transformación de REVIEWS")

        df = df.copy()

        # Eliminar duplicados
        if "_id" in df.columns:
            df = df.drop_duplicates(subset=["_id"])

        # Convertir fechas
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Limpiar comentarios
        if "comments" in df.columns:
            df["comments"] = df["comments"].fillna("").astype(str).str.strip()

        # Agregar columna de longitud del comentario
        if "comments" in df.columns:
            df["comment_length"] = df["comments"].apply(len)

        df["fecha_transformacion"] = datetime.now()

        self.registrar_log(f"REVIEWS transformado: {len(df)} registros finales")
        return df

    # ------------------------------------------
    # TRANSFORMACIÓN DE CALENDAR
    # ------------------------------------------
    def transformar_calendar(self, df):
        if df.empty:
            print("⚠️ No hay datos en CALENDAR para transformar")
            return df

        self.registrar_log("Inicio transformación de CALENDAR")

        df = df.copy()

        # Eliminar duplicados por listing_id + date
        if "listing_id" in df.columns and "date" in df.columns:
            df = df.drop_duplicates(subset=["listing_id", "date"])

        # Convertir fecha
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # Limpiar precios
        for col in ["price", "adjusted_price"]:
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace("[^0-9.]", "", regex=True)
                    .replace("", np.nan)
                    .astype(float)
                )

        # Convertir disponibilidad a booleano
        if "available" in df.columns:
            df["available"] = df["available"].astype(str).str.lower().map({"t": True, "f": False})

        # Agregar columna de transformación
        df["fecha_transformacion"] = datetime.now()

        self.registrar_log(f"CALENDAR transformado: {len(df)} registros finales")
        return df

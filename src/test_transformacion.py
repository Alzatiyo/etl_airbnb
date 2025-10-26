import pandas as pd
from extraccion import Extraccion
from transformacion import Transformacion


def mostrar_comparacion(df_antes, df_despues, nombre):
    """
    Muestra una comparación visual entre el DataFrame antes y después.
    """
    print(f"\n{'='*70}")
    print(f"📊 COMPARACIÓN: {nombre}")
    print(f"{'='*70}")
    
    print(f"\n🔢 REGISTROS:")
    print(f"   Antes:    {len(df_antes):,}")
    print(f"   Después:  {len(df_despues):,}")
    print(f"   Diferencia: {len(df_antes) - len(df_despues):,} registros eliminados")
    
    print(f"\n📋 COLUMNAS:")
    print(f"   Antes:    {len(df_antes.columns)}")
    print(f"   Después:  {len(df_despues.columns)}")
    print(f"   Nuevas:   {len(df_despues.columns) - len(df_antes.columns)}")
    
    nuevas_cols = set(df_despues.columns) - set(df_antes.columns)
    if nuevas_cols:
        print(f"\n✨ COLUMNAS NUEVAS: {', '.join(sorted(nuevas_cols))}")
    
    print(f"\n❌ VALORES NULOS:")
    print(f"   Antes:    {df_antes.isnull().sum().sum():,}")
    print(f"   Después:  {df_despues.isnull().sum().sum():,}")
    
    print(f"\n👀 PRIMERAS 3 FILAS DEL RESULTADO:")
    print(df_despues.head(3).to_string())
    print(f"\n{'='*70}\n")


def main():
    print("="*70)
    print("🧪 INICIANDO TRANSFORMACIÓN DE DATOS DESDE MONGODB")
    print("="*70)

    # --------------------------------------------
    # PASO 1: CONEXIÓN Y EXTRACCIÓN DESDE MONGODB
    # --------------------------------------------
    uri = "mongodb://localhost:27017/"
    nombre_db = "MX_DB"

    extraccion = Extraccion(uri, nombre_db)
    db = extraccion.conectar_mongodb()

    if db is None:
        print("❌ No se pudo conectar a MongoDB.")
        return

    print("\n📦 Extrayendo datos reales desde MongoDB...")
    df_listings_original = extraccion.obtener_datos("MX_listings", limite=10)
    df_reviews_original = extraccion.obtener_datos("MX_reviews", limite=10)
    df_calendar_original = extraccion.obtener_datos("MX_calendar", limite=10)

    print(f"\n✅ Datos obtenidos:")
    print(f"   Listings: {len(df_listings_original)} registros")
    print(f"   Reviews:  {len(df_reviews_original)} registros")
    print(f"   Calendar: {len(df_calendar_original)} registros")

    # --------------------------------------------
    # PASO 2: APLICAR TRANSFORMACIONES
    # --------------------------------------------
    transformador = Transformacion(nombre_archivo_log="test_transformacion_log.txt")

    print("\n🔄 Aplicando transformaciones...")

    print("\n🏠 Transformando LISTINGS...")
    df_listings_transformado = transformador.transformar_listings(df_listings_original)
    mostrar_comparacion(df_listings_original, df_listings_transformado, "LISTINGS")

    print("\n⭐ Transformando REVIEWS...")
    df_reviews_transformado = transformador.transformar_reviews(df_reviews_original)
    mostrar_comparacion(df_reviews_original, df_reviews_transformado, "REVIEWS")

    print("\n📅 Transformando CALENDAR...")
    df_calendar_transformado = transformador.transformar_calendar(df_calendar_original)
    mostrar_comparacion(df_calendar_original, df_calendar_transformado, "CALENDAR")

    # --------------------------------------------
    # PASO 3: GUARDAR RESULTADOS PARA CARGA
    # --------------------------------------------
    print("\n💾 Guardando resultados transformados...")
    df_listings_transformado.to_csv("test_listings_transformado.csv", index=False)
    df_reviews_transformado.to_csv("test_reviews_transformado.csv", index=False)
    df_calendar_transformado.to_csv("test_calendar_transformado.csv", index=False)

    print("\n✅ Archivos CSV generados:")
    print("   - test_listings_transformado.csv")
    print("   - test_reviews_transformado.csv")
    print("   - test_calendar_transformado.csv")

    print("\n" + "="*70)
    print("✨ TRANSFORMACIÓN COMPLETADA EXITOSAMENTE")
    print("="*70)
    print("\n💡 Ahora puedes ejecutar: python test_carga.py para cargar los datos a SQL Server")
    print("\n📝 Logs en 'Logs/test_transformacion_log.txt'")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Proceso interrumpido por el usuario.")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()

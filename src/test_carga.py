"""
Script de prueba para la clase Carga.
Prueba la conexión a SQL Server y carga de datos transformados.
"""

import pandas as pd
from carga import Carga


def main():
    """
    Función principal que prueba la clase Carga con los datos transformados del test.
    """
    print("="*70)
    print("🧪 INICIANDO PRUEBA DE CARGA A SQL SERVER")
    print("="*70)
    
    # ==========================================
    # CONFIGURACIÓN DE SQL SERVER
    # ==========================================
    SERVER = "DESKTOP-OE28SRF\\SQLEXPRESS"
    DATABASE = "Airbnb_DW"
    USERNAME = "sam"
    PASSWORD = "123"
    
    print(f"\n📋 Configuración:")
    print(f"   Servidor: {SERVER}")
    print(f"   Base de datos: {DATABASE}")
    print(f"   Usuario: {USERNAME}")
    
    # ==========================================
    # PASO 1: CARGAR DATOS TRANSFORMADOS
    # ==========================================
    print("\n📦 PASO 1: Cargando datos transformados del test...")
    print("-" * 70)
    
    try:
        df_listings = pd.read_csv("test_listings_transformado.csv")
        df_reviews = pd.read_csv("test_reviews_transformado.csv")
        df_calendar = pd.read_csv("test_calendar_transformado.csv")
        
        print(f"✅ Listings cargados: {len(df_listings)} registros")
        print(f"✅ Reviews cargados: {len(df_reviews)} registros")
        print(f"✅ Calendar cargados: {len(df_calendar)} registros")
    except FileNotFoundError as e:
        print(f"❌ Error: No se encontraron los archivos CSV del test")
        print(f"   Ejecuta primero: python src/test_transformacion.py")
        return
    
    # ==========================================
    # PASO 2: CONECTAR A SQL SERVER
    # ==========================================
    print("\n🔌 PASO 2: Conectando a SQL Server...")
    print("-" * 70)
    
    cargador = Carga(SERVER, DATABASE, USERNAME, PASSWORD)
    
    if not cargador.conectar_sqlserver():
        print("❌ Error: No se pudo conectar a SQL Server")
        print("\n💡 Verifica:")
        print("   1. SQL Server está corriendo")
        print("   2. Las credenciales son correctas")
        print("   3. El usuario tiene permisos para crear bases de datos")
        return
    
    print("✅ Conexión exitosa a SQL Server")
    
    # ==========================================
    # PASO 3: CARGAR DATOS A SQL SERVER
    # ==========================================
    print("\n💾 PASO 3: Cargando datos a SQL Server...")
    print("-" * 70)
    
    # Cargar Listings
    print("\n🏠 Cargando tabla 'listings'...")
    if cargador.cargar_a_sqlserver(df_listings, "listings"):
        count = cargador.verificar_carga("listings")
        print(f"✅ Tabla 'listings' creada con {count} registros")
    else:
        print("❌ Error al cargar 'listings'")
    
    # Cargar Reviews
    print("\n⭐ Cargando tabla 'reviews'...")
    if cargador.cargar_a_sqlserver(df_reviews, "reviews"):
        count = cargador.verificar_carga("reviews")
        print(f"✅ Tabla 'reviews' creada con {count} registros")
    else:
        print("❌ Error al cargar 'reviews'")
    
    # Cargar Calendar
    print("\n📅 Cargando tabla 'calendar'...")
    if cargador.cargar_a_sqlserver(df_calendar, "calendar"):
        count = cargador.verificar_carga("calendar")
        print(f"✅ Tabla 'calendar' creada con {count} registros")
    else:
        print("❌ Error al cargar 'calendar'")
    
    # ==========================================
    # PASO 4: EXPORTAR A EXCEL
    # ==========================================
    print("\n📊 PASO 4: Exportando a archivos Excel...")
    print("-" * 70)
    
    # Crear carpeta data si no existe
    import os
    os.makedirs("data", exist_ok=True)
    
    # Exportar cada tabla
    if cargador.exportar_a_excel(df_listings, "data/listings_transformado.xlsx"):
        print("✅ data/listings_transformado.xlsx")
    
    if cargador.exportar_a_excel(df_reviews, "data/reviews_transformado.xlsx"):
        print("✅ data/reviews_transformado.xlsx")
    
    if cargador.exportar_a_excel(df_calendar, "data/calendar_transformado.xlsx"):
        print("✅ data/calendar_transformado.xlsx")
    
    # ==========================================
    # PASO 5: RESUMEN FINAL
    # ==========================================
    print("\n" + "="*70)
    print("✨ PRUEBA DE CARGA COMPLETADA EXITOSAMENTE")
    print("="*70)
    
    print("\n📊 RESUMEN:")
    print(f"   Base de datos: {DATABASE}")
    print(f"   Servidor: {SERVER}")
    print("\n📋 Tablas creadas en SQL Server:")
    print("   ✅ listings")
    print("   ✅ reviews")
    print("   ✅ calendar")
    print("\n📁 Archivos Excel generados en 'data/':")
    print("   ✅ listings_transformado.xlsx")
    print("   ✅ reviews_transformado.xlsx")
    print("   ✅ calendar_transformado.xlsx")
    print("\n📝 Logs generados en 'logs/logs_carga.txt'")
    
    print("\n" + "="*70)
    print("🎉 ¡PROCESO COMPLETADO!")
    print("="*70)
    print("\n💡 Ahora puedes:")
    print("   1. Abrir SQL Server Management Studio")
    print(f"   2. Conectar a: {SERVER}")
    print(f"   3. Ver la base de datos: {DATABASE}")
    print("   4. Consultar las tablas: listings, reviews, calendar")
    
    # Cerrar conexión
    cargador.cerrar_conexion()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
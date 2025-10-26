from extraccion import Extraccion

uri = "mongodb://localhost:27017/"
nombre_db = "MX_DB"

extraccion = Extraccion(uri, nombre_db)

extraccion.conectar_mongodb()

df_reviews = extraccion.obtener_datos("MX_reviews", limite=1)
df_listings = extraccion.obtener_datos("MX_listings", limite=1)
df_calendar = extraccion.obtener_datos("MX_calendar", limite=1)

print("\n\n-------------- Reviews  --------------\n")
print(df_reviews.head())
print("\n\n-------------- Listings --------------\n")
print(df_listings.head())
print("\n\n-------------- Calendar --------------\n")
print(df_calendar.head())

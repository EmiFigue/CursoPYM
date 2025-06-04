import random as r
import pandas as pd
import sqlite3 as sql
import time as t
class tools:
  #Valores constantes que se utilizaran en el programa
  abcdario = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
              'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
              'U', 'V', 'W', 'X', 'Y', 'Z']
  papelerias = ['Xochimilco', 'Cuemanco', 'Coapa', 'Milpa Alta', 'CU', 'Zócalo',
                'Narvarte', 'Santa Fé', 'Polanco', 'Centro']
  lineas = ['Cuadernos', 'Libretas', 'Lápices', 'Plumones', 'Borradores', 'Sacapuntas',
            'Laptops', 'Tablets', 'Mochilas', 'Bolsas', 'Cajas', 'Pegamento', 'Tijeras',
            'Monitores', 'Teclados', 'Mouse', 'Audífonos', 'Cables', 'Cargadores', 'Baterías',
            'Pc', 'Uniformes', 'Pinturas', 'Pinceles', 'Papel', 'Cartulinas']
  ventas_por_fecha = r.randint(1, 1000)
  

  def _generar_info(fecha_reporte):
        """
        Genera un DataFrame de ventas simuladas para una fecha específica.
        El número de ventas generadas varía aleatoriamente entre 1 y 1000.

        Args:
            fecha_reporte (str): Fecha del reporte que se va a generar (formato YYYY-MM-DD).

        Returns:
            pd.DataFrame: DataFrame con la información de las ventas simuladas.
        """
        fechas = []
        sucursales = []
        productos = []
        claves_producto = []
        precios = []
        cantidades_vendidas = []
        totales_ticket = []
        for _ in range(tools.ventas_por_fecha):
            sucursal = r.choice(tools.papelerias)
            producto = r.choice(tools.lineas)
            clave_producto = (
                r.choice(tools.abcdario) +
                r.choice(tools.abcdario) +
                r.choice(tools.abcdario) + "-" +
                ''.join(str(r.randint(1, 9)) for _ in range(3))
            )
            precio = round(r.uniform(1, 10000), 2)
            cantidad_vendida = r.randint(1, 1000)
            total_ticket = round(precio * cantidad_vendida, 2)

            fechas.append(fecha_reporte)
            sucursales.append(sucursal)
            productos.append(producto)
            claves_producto.append(clave_producto)
            precios.append(precio)
            cantidades_vendidas.append(cantidad_vendida)
            totales_ticket.append(total_ticket)

        diccionario_ventas_df = {
            "Fecha": fechas,
            "Sucursal": sucursales,
            "Producto": productos,
            "Clave_Producto": claves_producto,
            "Precio": precios,
            "Cantidad_Vendida": cantidades_vendidas,
            "Total_Ticket": totales_ticket
        }

        df_ventas = pd.DataFrame(diccionario_ventas_df)
        print(f"Generación exitosa al {fecha_reporte} con {tools.ventas_por_fecha} ventas")
        return df_ventas

  def inicializar_bdd():

    conn = sql.connect("Ventas.db")
    cursor = conn.cursor()

    query_create = """
      CREATE TABLE Ventas(
        Fecha            TEXT,
        Sucursal         TEXT,
        Producto         TEXT,
        Clave_Producto   TEXT,
        Precio           REAL,
        Cantidad_Vendida INTEGER,
        Total_Ticket     REAL
      )
    """
    cursor.execute(query_create)
    conn.commit()
    conn.close()

    print("Base de datos creada/conectada")
    print("Tabla creada")

  def _inserciones_mult(df):
      
      conn = sql.connect("Ventas.db")
      cursor = conn.cursor()

      for i in range(tools.ventas_por_fecha):
          query_insert = f"""
          INSERT INTO
            Ventas
          VALUES(
            '{df.loc[i, "Fecha"]}',
            '{df.loc[i, "Sucursal"]}',
            '{df.loc[i, "Producto"]}',
            '{df.loc[i, "Clave_Producto"]}',
             {df.loc[i, "Precio"]},
             {df.loc[i, "Cantidad_Vendida"]},
             {df.loc[i, "Total_Ticket"]}
          )
          """
          cursor.execute(query_insert)
          conn.commit()

      conn.close()
      print(f"Inserción existosa")


  def rangos(f_init, f_fin):
    
    import pandas as pd
    
    # 1. Generamos el rango de fechas con la funcion date_range(f_init, f_fin)
    rango_fechas = pd.date_range(f_init, f_fin)

    # 2. Con base en ese rango, creamos un dataframe
    rango_df = pd.DataFrame(rango_fechas)

    # 3. Cambiamos de nombre la columna
    rango_df = rango_df.rename(columns={0: "Rango Fechas"})

    # 4. Cambiamos de tipo de dato objeto fecha (datetime/timestamp) ---> str
    rango_df['Rango Fechas'] = rango_df['Rango Fechas'].astype(str)

    rango_f = list(rango_df['Rango Fechas'])

    return rango_f
  # None ----> Vacio/Nada
  def generar_bdd_rango(fecha_ini, fecha_fin=None):
      """Metodo en el cual simulamos la informacion de las ventas y adicionalmente realizamos
      las inserciones. Se podra generar informacion de un solo dia o informacion de
      todo un rango de fechas."""

      # Pantallazo inicial del tiempo
      inicio = t.time()

      if fecha_fin == None:
        # si no pusiste una fecha de fin, entonces significa
        # que solo quieres insertar la info de un dia nomas
        print("Solo estás insertando info de una fecha :D")
        # Generamos la info
        print("Comienzo del programa .....")
        df = tools._generar_info(fecha_ini)

        # Insertamos
        print("Comienzo de las inserciones ....")
        tools._inserciones_mult(df)

        # Pantallazo de tiempo de cuando termino el proceso
        fin = t.time()

        print(f'Fecha: {fecha_ini} || Tiempo de ejecución: {round((fin - inicio) / 60, 2)} minutos')

      else:
        # bueno, en este caso (en el caso en que pusiste fecha fin) quieres insertar info
        # de mas de un dia, es más, quieres insertar info de todo un rango de fecha
        print("Estás insertando info de un rango fechas :D")
        rango_fechas = tools.rangos(fecha_ini, fecha_fin)

        for fecha in rango_fechas:
          # Generamos la info
          print("Comienzo del programa .....")
          df = tools._generar_info(fecha)

          # Insertamos
          print("Comienzo de las inserciones ....")
          tools._inserciones_mult(df)

  def consultar(query):
        """
        Realiza una consulta SQL sobre la base de datos 'ventas.db' y devuelve el resultado como DataFrame.

        Parámetros:
        -----------
        query : str
            Consulta SQL a ejecutar sobre la base de datos.

        Retorna:
        --------
        consulta : pandas.DataFrame
            DataFrame con el resultado de la consulta.

        Ejemplo de uso:
        ---------------
        resultado = consultar_bbdd("SELECT * FROM ventas")
        """
        import sqlite3 as sql
        import pandas as pd

        # Creamos la conexion o la base de datos
        conn = sql.connect('Ventas.db')
        # Hacemos una consulta a la tabla ventas
        consulta = pd.read_sql_query(query, conn)
        # Cerramos la conexion
        conn.close()

        return consulta

  def comprobar_fechas():
      """Método para ver cuales fechas tenemos cargadas en la tabla
      con base en una consulta SQL"""
      import sqlite3 as sql
      import pandas as pd

      conn = sql.connect("Ventas.db")

      query = """
      SELECT
        DISTINCT FECHA
      FROM
        Ventas
      """

      df = pd.read_sql_query(query, conn)
      conn.close()

      return df
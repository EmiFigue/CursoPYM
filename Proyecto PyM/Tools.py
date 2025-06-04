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

  def _generar_info(fecha_reporte):
    """

    Args:
        fecha_reporte (fecha): Fecha del reporte que se va a generar

    Returns:
        dataframe: dataframe con la información de las ventas simuladas
    """

    import random as r
    import pandas as pd

    fechas = []
    sucursales = []
    productos = []
    claves_producto = []
    precios = []
    cantidades_vendidas = []
    totales_ticket = []

    for i in range(1, 1001):
      sucursal = r.choice(tools.papelerias)
      producto = r.choice(tools.lineas)
      clave_producto = r.choice(tools.abcdario) + r.choice(tools.abcdario) + r.choice(tools.abcdario) + "-" + str(r.randint(1,9)) + str(r.randint(1,9)) + str(r.randint(1,9))
      precio = round(r.random() * r.randint(100,10000), 2)
      cantidad_vendida = r.randint(1, 1000)
      total_ticket = precio * cantidad_vendida

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

    print(f"Generación exitosa al {fecha_reporte}")
    return df_ventas

  def definiciones():
    """Método para definir la base de datos y la tabla de ventas"""
    import sqlite3 as sql

    conn = sql.connect("Ventas.db")
    cursor = conn.cursor()

    query_create = """
      CREATE TABLE VENTAS_2025(
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
      """"""

      import random as r
      import pandas as pd
      import sqlite3 as sql

      conn = sql.connect("Ventas.db")
      cursor = conn.cursor()

      for i in range(0, 1000):
          query_insert = f"""
          INSERT INTO
            VENTAS_2025
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

  ######################## Metodo proceso final ################################

  def rangos(f_init, f_fin):
    """ Métod público para generar un rango de fechas consecutivo
    comprendido entre f_init y f_fin
    """
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
  def proceso(fecha_ini, fecha_fin=None):
      """Metodo en el cual simulamos la informacion de las ventas y adicionalmente realizamos
      las inserciones. Se podra generar informacion de un solo dia o informacion de
      todo un rango de fechas."""

      import pandas as pd
      import time as t

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

  ######################## Metodo proceso final ################################

  def query(sentencia):
      """Método para realizar consultas SQL en nuestra tabla de ventas"""
      import sqlite3 as sql
      import pandas as pd

      conn = sql.connect("Ventas.db")
      df = pd.read_sql_query(sentencia, conn)
      conn.close()

      return df

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
        VENTAS_2025
      """

      df = pd.read_sql_query(query, conn)
      conn.close()

      return df
from sqlalchemy import create_engine
import pandas as pd
#funcion para crear el motor de MySQL (apenas es como la conexion, pero mas dinamico)
def engine_de_mysql(host,user,password,database):
    try:
        url= f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
        engine=create_engine(url)
        print("Engine de Mysql creado")
        return engine
    except Exception as e:
        print(f"No se pudo :{e}")
        return None

#fucion para crear el motor de postresql
def engine_de_postresql(host,user,password,database):
    try:
        url=f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
        engine=create_engine(url)
        print("engine de postgresql creado")
        return engine
    except Exception as e:
        print(f"No se pudo:{e}")
        return None

#funcion para la extraccion
def extraccion(engine,nombre_tabla):
    try:
        df=pd.read_sql(f"SELECT * FROM {nombre_tabla}",engine)
        print(f"Tabla {nombre_tabla} extraido correctamente")
        return df
    except Exception as e:
        print(f"Fallo la extraccion: {e}")
        
#funcion para las transformaciones necesaias
def transformacion(df_ventas,df_clientes):
    try:
        #borraremos lo datos duplicados de cada base de datos antes de hacer el merge
        #para la tabla de ventas usaremos el id de venta como parametro para que se borren duplicados por el id
        df_ventas=df_ventas.drop_duplicates(subset=['ID_Venta'])
        #mismo caso para la tabla de clientes
        df_clientes=df_clientes.drop_duplicates(subset=['id'])
        #renombrando el nombre id de la tabla de clientes a ID_Cliente por que en la base de datos se nombro el campo como id en vez de ID_Cliente(paso necesario para hacer el merge)
        df_clientes=df_clientes.rename(columns={'id': 'ID_Cliente'})
        df_final = pd.merge(df_ventas, df_clientes, on='ID_Cliente', how='inner')
        #Renombrando el los demas campos a la primera en mayuscula
        df_final=df_final.rename(columns={'monto':'Monto','fecha':'Fecha','nombre':'Nombre','ciudad':'Ciudad'})
        #Estandarizando las fechas
        #se usa la funcion to datetime para se entienda que se se esta tratando con fechas y no solo texto comun,el parametro format es para que analize cada una por separado sin mandar advertencias
        # y el parametro errors es para que si no se entiende se deje un dato nulo
        df_final['Fecha'] = pd.to_datetime(df_final['Fecha'],format='mixed', errors='coerce')
        # .dt.strftime lo vuelve a convertir a texto pero ya todo parejo
        df_final['Fecha'] = df_final['Fecha'].dt.strftime('%d/%m/%Y')
        #ahora se agrega la fila de el total gastado por cliente
        #en este caso se suma el total de los montos de cada cliente y se agregue a la fila el total de cada uno de estos, y se redondea a 2 decimales
        df_final['Total_Gastado_Cliente'] = df_final.groupby('ID_Cliente')['Monto'].transform('sum').round(2)
        #agregamos el conteo de transacciones por ciudad
        #aqui calcula el volumen de ventas por ciudad y lo asigna a cada fila mediante una transformación.
        df_final['Conteo_Transacciones_Ciudad']=df_final.groupby('Ciudad')['ID_Venta'].transform('count')
        #elimina los datos nulos
        df_final=df_final.dropna()
        return df_final
    except Exception as e:
        print(f"No se completo la transformacion: {e}")
        
#funcion para la carga
def carga(df_final,ruta_archivo):
    df_final.to_csv(ruta_archivo,index=False)
    print(f"Carga hecha correctamente en {ruta_archivo}")
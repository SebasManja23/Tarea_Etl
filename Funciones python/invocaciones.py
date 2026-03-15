import funciones
if __name__=="__main__":
    #creamos los engine para cada base de datos para poder luego hacer la extraccion, este engine es como la conexion pero muy dinamica, a diferencia de una simple conexion
    engine_mysql=funciones.engine_de_mysql("192.168.1.91","root","admin","proyecto_etl")
    engine_postrgesql=funciones.engine_de_postresql("localhost","postgres","admin123","tarea_etl")
    #extraemos los datos de cada una de las bases de datos y los ponemos a cada uno en un data frame
    df_mysql=funciones.extraccion(engine_mysql,"ventas")
    df_postgresql=funciones.extraccion(engine_postrgesql,"clientes")
    #creamos un data frame final con una combinacion de las dos bases de datos a travez del merge
    df_final=funciones.transformacion(df_mysql,df_postgresql)
    #finalmente se carga a un nuevo archivo final con todos los cambios correspondientes
    funciones.carga(df_final,'destino_final.csv')
#------------- Inician Imports. -------------#
#import urllib
#import utllib.request
import re
import sys
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import time
import random
import datetime
import os
#----
import pymssql
import _mssql

#------------- Finalizan Imports. -------------#

#------------- Inicia Declaración de Variables Globales. -------------#
#------------- Inicia Configuración de BD. -------------#
#--- Variables de conexión a la base de datos.
connection = pymssql.connect(server='66.232.22.196',
                            user='FOXCLEA_TAREAS',
                            password='JACINTO2014',
                            database='FOXCLEA_TAREAS'
                            #charset='utf8mb4',
                            #cursorclass=pymssql.cursors.DictCursor
                           )
#---
#------------- Finaliza Configuración de BD. -------------#
#--- Navegador.
file_path = os.path.join(sys.path[0]) + "\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe"
print(file_path)
driver = webdriver.PhantomJS(file_path) #<--- Se especifica el path del exe.
# driver.set_page_load_timeout(240)
#--- BD
PORTAL = []
SITE = "AirBnB"
ANUNCIOS = [] #<--- Lista donde se almacenan los resultados de una anuncios.
COMPETENCIA = [] #<--- Lista donde se almacenan los resultados de los anuncios de la competencia.
CONSULTA = [] #<--- Lista para manejar las consultas.
#--- atos de los anuncios propios.
ID = [] #<--- Lista para almacenar los IDs de los hospedajes.
NAME = [] #<--- Lista para almacenar los NOMBRES de los hospedajes.
ADULTS = [] #<--- Lista para almacenar el numero de adulto que permite una habitación
CHILDS = [] #<--- Lista para almacenar el numero de niños que permite una habitación
INFANT = [] #<--- Lista para almacenar el numero de bebes que permite una habitación
KIND = [] #<--- Lista para almacenar los tipos de  habitaciones.
CAPACITY = [] #<----- Lista para almacenar la CAPACIDAD de los hospedajes.
BED = [] #<----- Lista para almacenar el numero de CAMAS en los hospedajes.
A_LINK = [] #<----- Lista para almacenar las URLs de los hospedajes.
#COST = [] #<----- Lista para almacenar los PRECIOS de los hospedajes.
#--- Datos de los anuncios de la Competencia.
C_ID = [] #<--- Lista para almacenar los IDs de los hospedajes.
C_NAME = [] #<--- Lista para almacenar los NOMBRES de los hospedajes.
C_KIND = [] #<--- Lista para almacenar los tipos de  habitaciones.
C_LINK = []
#--- Busquedade de palabras.
bed_name = "cama" #<-- Variable usada para buscar la palabra "cama" en un string.
string = "Precio" #<-- Variable usada para buscar la palabra "precio" en un string
string2 = "Totalmente reembolsable" #<-- Variable usada para buscar la palabra "Totalmente reembolsable" en un string
string3 = "€"
string4 = 'NUEVO'
string5 = '1 evaluación'
string6 = '2 evaluaciones'
string7 = 'Superhost'
string8 = '1 Superhost'
string9 = '2 Superhost'
#---
position = 1 #<--- Posición de los resultados (1,2,3,4,5) ---TODO--- considerar el uso de un arreglo para almacenar todos los resultados.
#---
max_attempt = 16
today = (datetime.datetime.now()) #<--- Fecha del dia de hoy.
URL = ""
S_PROGRAM = False #<--- Estado de la fecha programada.
#--- Almacenar
#---
L_POS = []
L_NAME = []
L_PRICE = []
L_KIND = []
L_BED = []
L_LINK = []
L_RATE = []
#---
#------------- Finaliza Declaración de Variables Globales. -------------#

#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Consulta especifica
        sql = ("SELECT * FROM SCR_PORTALES WHERE NOMBRE = '" + SITE + "'")
        cursor.execute(sql)
        PORTAL = cursor.fetchone()
        #---
        #print(PORTAL)
        print('Correcto #1 -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error #1 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

#------------- Inicia Consulta a BD para Obtener los Hospedajes Almacenados. -------------#
#--- Se extraen las consultas que el script ejecutara.
def load_data_consulta():
    #---
    del CONSULTA[:]
    temp_consulta = []
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql= "SELECT ID_CONSULTA, ID_ANUNCIO, PAIS, CIUDAD, ZONA, ADULTOS, NIÑOS, BEBES, ESTADO, RANGO_DIAS, FECHA_FIN, FECHA_PROGRA, N_MAX_PAG FROM SCR_CONSULTAS WHERE ESTADO = 1 AND ID_PORTAL = " + str(PORTAL[0])
            cursor.execute(sql)
            temp_consulta = cursor.fetchall()
            #---
            #return (temp_consulta)
            print('Correcto #2-> Extracción de los datos de la "consultas" a realizar.')
    #---Excepción
    except _mssql.MssqlDatabaseException as e:
        print('Error #2 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    print(len(temp_consulta))
    for i in range(len(temp_consulta)):
        #---
        if (temp_consulta[i][1] != None):
            try:
            #---
                with connection.cursor() as cursor:
                    #--- Consulta especifica
                    sql= "SELECT C.ID_CONSULTA, C.ID_ANUNCIO, C.PAIS, C.CIUDAD, C.ZONA, C.ADULTOS, C.NIÑOS, C.BEBES, C.ESTADO, C.RANGO_DIAS, C.FECHA_FIN, C.FECHA_PROGRA, C.N_MAX_PAG, A.TITULO, A.ID_PORTAL, A.ESTADO FROM SCR_CONSULTAS C INNER JOIN SCR_ANUNCIOS A ON C.ID_ANUNCIO = A.ID_ANUNCIO WHERE A.ESTADO = 'activo' AND C.ESTADO = 1 AND A.ID_PORTAL = " + str(PORTAL[0]) + " AND C.ID_CONSULTA = " + str(temp_consulta[i][0])
                    cursor.execute(sql)
                    CONSULTA.append(cursor.fetchone())
                    #---
            #---Excepción
            except _mssql.MssqlDatabaseException as e:
                print('Error #2 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
         #---
        else:
            CONSULTA.append(temp_consulta[i])
        #---
    return CONSULTA

    #--- Se Obtiene el Portal
#----
CONSULTA = load_data_consulta() #<--- Se cargan los datos de la consulta
#---
#--- Se cargan las consultas, y verifica su fecha de fin.
for i in range(len(CONSULTA)):
#---
    if (CONSULTA[i][10] == None): #<--- Si no hay una Fecha definida la consulta seguira activa.
        print('INFO: la consulta con ID = ',CONSULTA[i][0],' Sigue activa.')
    #---
    elif (CONSULTA[i][10] >= today): #<--- Si la fecha de es mayor o igual a la de hoy, la consulta sigue activa.
        print('INFO: la consulta con ID = ',CONSULTA[i][0],' Sigue activa.')
    #---
    else: #<--- En caso contrario la fecha de fin de la consulta se habra sobrepasado, por ende se inhabilita la consulta
        print('INFO: la consulta con ID = ',CONSULTA[i][0],' Ha sido desactivada, porque ha pasado su fecha de Fin')
        #---
        try:
        #---
            with connection.cursor() as cursor:
            #---
                sql = "UPDATE SCR_CONSULTAS SET ESTADO = %s WHERE ID_CONSULTA = %s"
                cursor.execute(sql, (False,CONSULTA[i][0]))
            #---
            connection.commit()
        #---
        except _mssql.MssqlDatabaseException as e:
        #---
            print('Error: -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
        #---
#--- Se vuelven a cargar los datos de la consulta, en caso que se hay inhabilitado una con anterioridad.
CONSULTA = load_data_consulta() #<--- Se cargan los datos de la consulta
#---
#--- Se Obtienen los anuncios
try:
    #---
    with connection.cursor() as cursor:
        #--- Consulta especifica
        sql = "SELECT * FROM SCR_ANUNCIOS WHERE ID_PORTAL = " + str(PORTAL[0]) + "AND ESTADO = 'activo' "
        cursor.execute(sql)
        ANUNCIOS = cursor.fetchall()
        #---
        #print(ANUNCIOS)
        print('Correcto #3 -> Extracción de los datos de los "anuncios" de la empresa activos.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error #3 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

#--- Ciclo para Ingresar los datos de hospedajes obtenidos de la tabla anuncios
for i in range(len(ANUNCIOS)):
    ID.append(ANUNCIOS[i][0])
    NAME.append(ANUNCIOS[i][1])
    ADULTS.append(ANUNCIOS[i][6])
    CHILDS.append(ANUNCIOS[i][7])
    INFANT.append(ANUNCIOS[i][8])
    t_cap = (ANUNCIOS[i][6] + ANUNCIOS[i][7] + ANUNCIOS[i][8])
    CAPACITY.append(t_cap)
    KIND.append(ANUNCIOS[i][9])
    BED.append(ANUNCIOS[i][10])
    A_LINK.append(str(ANUNCIOS[i][13]))
    #COST.append(ANUNCIOS[i]['PRECIO_PROM'])
    print('id: ',ID[i],', name: ',NAME[i],', capacity: ',CAPACITY[i],', bed: ',BED[i],', price: ',KIND[i],'--','link: ',A_LINK[i])
    print('Correcto #4 -> Se asignaron los valores de "anuncios" extraidos en las listas a usar.')
#---
#------------- Finaliza Consulta a BD para Obtener Datos Almacenados. -------------#
#---
def insert_log(id_consulta,mensaje,detalle,linea_cod,l_url,tipo):
    #--- Inicia la conección
    try:
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "INSERT INTO SCR_LOG (ID_CONSULTA,MENSAJE,LINEA_COD,URL,DETALLE,TIPO) VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, (id_consulta, mensaje, linea_cod, l_url,detalle,tipo))

            #---
            print('Correcto #5 -> Registro Correcto del Log.')
        connection.commit()
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #5 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

#---
print("------------------------------------------------")
#---
for c in range(len(CONSULTA)):
    print("----- Inicia consulta ID: ",(CONSULTA[c][0]))
    #---
    td = (datetime.date.today()) #<--- Variable donde se almacena la fecha del día de hoy.
    del L_POS[:]
    del L_NAME[:]
    del L_PRICE[:]
    del L_KIND[:]
    del L_BED[:]
    del L_LINK[:]
    del L_RATE[:]
    #---
    position = 1 #<--- Posición de los resultados (1,2,3,4,5) ---TODO--- considerar el uso de un arreglo para almacenar todos los resultados.
    #---
    ALLOW_RESET = False
    S_PROGRAM = False
    #---
    is_date = False #<---- Variable para verificar que la consulta tiene una fecha programada.
    if(CONSULTA[c][11] != None):

        if ((CONSULTA[c][11]).date() < (CONSULTA[c][10]).date()):
            is_date = True
        elif((CONSULTA[c][11]).date() >= (CONSULTA[c][10]).date()):
            is_date = True
            ALLOW_RESET = True
        else:
            is_date = False
    #---
    if (CONSULTA[c][11] == None or is_date == True):
        #---
        #--- Iteración de páginas.
        page = 0 #<--- Indice Inicial.
        #------------- Inicia Asignación de Valores a las Variables de la URL. -------------#
        #--- URL + PARAMETROS DE LA URL    s
        BASE = PORTAL[2]
        PAIS = CONSULTA[c][2]
        CIUDAD = CONSULTA[c][3]
        ZONA = CONSULTA[c][4]
        BODY = '/homes?logo=1&' #<--- ADD & TO ADULTS.
        ADULT = CONSULTA[c][5]
        BODY2 = '&allow_override[]='
        if (is_date == True):
            CHECKIN = (CONSULTA[c][11]).date()
            CHECKOUT = CHECKIN + datetime.timedelta(days=CONSULTA[c][9])
        else:
            CHECKIN = datetime.datetime.now()
            CHECKOUT = CHECKIN + datetime.timedelta(days=CONSULTA[c][9])
        #---
        CHILDREN = CONSULTA[c][6] #<--- MAX 12.
        INFANTS = CONSULTA[c][7] #<--- MAX TWO.
        GUESTS = ADULT + CHILDREN
        #-- falta determinar si sera de un archivo de texto o de la base de datos.
        AREA = ""
        #------------- Finaliza Asignación de Valores a las Variables de la URL. -------------#
        if (ZONA == None): #<--- Si no hay ningun dato asignado a Zona, se trabajara sen la URL con la ciudad y el País.
            AREA = '/s/' + CIUDAD + '--' + PAIS
        else: #<--- Si se especifica una zona, esta sera incorporada a la URL.
            AREA = '/s/' + ZONA + '--' + CIUDAD + '--' + PAIS
        #------------- Inicia estructuración de la URL basado en los parametros que se manejarán. -------------#
        #--- URL estandar
        URL = (BASE + AREA +  BODY + 'adults=' + str(ADULT) + BODY2 + '&checkin=' + CHECKIN.strftime('%Y-%m-%d') + '&checkout=' + CHECKOUT.strftime('%Y-%m-%d') + '&guests=' + str(GUESTS) + '&search_by_map=false')
        #--- URL para cuando se especifican un numero de niños no maximo a 12.
        if CHILDREN > 0:
            URL = BASE + AREA +  BODY + 'adults=' + str(ADULT) + BODY2 + '&checkin=' + CHECKIN.strftime('%Y-%m-%d') + '&checkout=' + CHECKOUT.strftime('%Y-%m-%d') + '&children=' + str(CHILDREN) + '&guests=' + str(GUESTS) + '&search_by_map=false'
        #--- URL para cuando se especifican un numero de bebes no maximo a dos.
        elif INFANTS > 0:
            URL = BASE + AREA +  BODY + 'adults=' + str(ADULT) + BODY2 + '&checkin=' + CHECKIN.strftime('%Y-%m-%d') + '&checkout=' + CHECKOUT.strftime('%Y-%m-%d') + '&guests='+ str(GUESTS) + '2&infants=' + str(INFANTS) + '&search_by_map=false'
        #--- URL para cuando se especifican un numero de niños y bebes.
        elif (CHILDREN > 0) and (INFANTS > 0):
            URL = BASE + AREA +  BODY + 'adults=' + str(ADULT) + BODY2 + '&checkin=' + CHECKIN.strftime('%Y-%m-%d') + '&checkout=' + CHECKOUT.strftime('%Y-%m-%d') + '&children=' + str(CHILDREN) + '&guests='+ str(GUESTS) + '2&infants=' + str(INFANTS) + '&search_by_map=false'
        #---
        print('Correcto #6 -> Creación correcta de la URL = "', URL,'".')
        insert_log((CONSULTA[c][0]),'Se genero la URL global, ver detalle.',URL,'166-176',URL,1) #tipo 0= error, 1= bien, 2= advertencia
        #---
        #------------- Finaliza estructuración de la URL basado en los parametros que se manejaran. -------------#

        #------------- Inicia la Creación de Funciones generales -------------#
         #--- Función para modificar el estado de la consulta.
        def change_State(state):
            #---
            td = (datetime.datetime.now()) #<--- Fecha del dia de hoy.
            #---
            try:
            #---
                with connection.cursor() as cursor:
                #---
                    sql = "UPDATE SCR_ESTADO SET ESTADO = %s, FECHA = %s WHERE ID_PORTAL = %s"
                    cursor.execute(sql, (state,td,PORTAL[0]))
                connection.commit()
                print("------ Se ha realizado el cambio de estado de la consulta.")
            #---
            except _mssql.MssqlDatabaseException as e:
            #---
                print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
            #---
            if (state == True):
                return "El Script se ejecuto correctamente. Se Ha modificado el estado de ejecución del script."
            else:
                return "El Script no se termino de ejecutar. Se Ha modificado el estado de ejecución del script."
        #---
        #---- Funcion para Procesar la URL y Obtener el HTML Renderizado por JS.
        #---- Funcion para Procesar la URL y Obtener el HTML Renderizado por JS.
        def get_innerHTML(P_URL):
            #---
            try:
                #---
                driver.set_page_load_timeout(240)
                #
                time.sleep(1)
                driver.get(P_URL) #<--- Navigate to the page.
                time.sleep(1)
                print(change_State(True))
            #---
            except TimeoutException:
                print(change_State(False))
            #---
            #----
            innerHTML = driver.execute_script("return document.body.innerHTML") #<--- Devuelve el inner HTML Como una cadena de texto.
            #---
            print('Correcto #7 -> Se ha extraido el contenido de la página con la URL = "', P_URL,'".')
            #insert_log((CONSULTA[c][0]),'Se Obtuvo el innerHTML de la URL.',URL,'184-196',URL,1) #tipo 0= error, 1= bien, 2= advertencia
            #---
            #print(BeautifulSoup(innerHTML, "html.parser"))
            #soup = BeautifulSoup(innerHTML, "html.parser")
            #with open("data.html", "w") as file:
            #    file.write(str(soup.encode("utf-8")))
            return (BeautifulSoup(innerHTML, "html.parser"))

        #---- Funcion General para filtrar contenido.
        def get_content(selector, filters):
            #---
            insert_log((CONSULTA[c][0]),'Se filtran los datos extraidos del innerHTML.',selector,'199-204','',1) #tipo 0= error, 1= bien, 2= advertencia
            print('Correcto #8 -> Se filtraron los datos.')
            #---
            return [t.get_text() for t in filters.select(selector)] #<--- Se Devuelven los datos Filtrados.

        #---- Función para reformar el arreglo obtenido de descripción (No todas las descripciones incluyen número de camas).
        def reformat_desc(a_desc):
            temp_desc = [] #<-- Lista temporal donde se almacenarán los datos de la descripción re-ordenadas.
            i = 0 #<-- Iteraciones basado en el tamaño de la lista de descripción.
            indice = 0 #<-- Esta variable es para manejar las iteraciones en caso q exista una irregularidad en la lista con desc (por lo general viene ['tipo','1 cama','tipo','2 camas',...] donde el numero de cama esta en un indice impar).
        #NOTA: Los tipos de cama generalmente tienen como indice un valor par, por ende el indice el par tienen un indice con valor impar.
        #NOTA: Esto es alterado cuando algun hospedaje no especifica el numero de camas, haciendo que el tipo de cama tome un valor impar.
        #--- CICLO
            while (i < (len(a_desc))):
                if (bed_name in a_desc[i] and ((indice % 2) != 0)): #<--- Si el indice es impar (1,3,5,7,...) y lleva la palabra asignada a bed_name("cama").
                    temp_desc.append(a_desc[i]) #<--- Se agrega el valor a la lista temporal.
                elif (bed_name not in a_desc[i] and ((indice % 2) != 0)): #<--- Si el indice es impar (1,3,5,7,...) y no lleva la palabra asignada a la variable bed_name("cama").
                    temp_desc.append("0 cama")  #<--- Como no se especifico un número de camas, se asigna 0 por defecto.
                    temp_desc.append(a_desc[i]) #<--- Se agrega el valor en el indice "i"
                    indice += 1
                else:
                    temp_desc.append(a_desc[i])
                i += 1
                indice += 1
            #---
            insert_log((CONSULTA[c][0]),"Se formateo la descripción.",'','207-231','',1) #tipo 0= error, 1= bien, 2= advertencia
            print('Correcto #9 -> Se formateo la descripción para su uso adecuado. Lista Ordenada = ',temp_desc)
            #---
            return temp_desc #<--- Devuelve la lista temporal con los item organizados.
        #---
        #------------- Finaliza la Creación de Funciones generales. -------------#

        #------------- Se determina el número de paginas de resultados encontrados. -------------#
        #--- Obtener numero de paginas y link
        p_i = 0
        p_attempts = 10
        iterations = 0
        while p_i <= p_attempts:
            #---
            pagination = get_innerHTML(URL) #<--- Se procesa la URL y obtiene el innerHTML de la URL
            pag = get_content(".search-results div ._12to336 div nav span div ._11hau3k li a div", pagination) #<--- Se Filtra el contenido de la pagina.
            print('test: ')
             #--- Se convierte el resultado obtenido a numeros.
            print(pag)
            for i in range(len(pag)):
                if pag[i] == '': #<--- En ocaciones devuelve un array ['1','2','','8'], donde el tercer valor no tiene nada, y se intenta a convertir a número da error.
                    pag[i] = 0 #<---- Se le da un valor númerico de 0 por defecto.
                else:
                    pag[i] = int(pag[i]) #<--- Se convierte de String a Int el valor en la lista.
            #---
            if (pag == []):
                #---
                iterations = 0
            #---
            else:
                #NOTA: Esto se hace por que al convertir los string en numeros, se puede sacarel número más alto que es el que determina el número total de paginas de resultado.
                #---
                #---Se determina el numero de iteraciones.
                print('Pages: ',pag)
                iterations = max(pag) #<--- Considerar caso donde este valor es 0
                insert_log((CONSULTA[c][0]),"Se determino el numero de Pagina",('Numero de Paginas: ' + str(iterations)),'235-248',URL,1) #tipo 0= error, 1= bien, 2= advertencia
                print('Correcto #10 -> Se determino el número de pagina = ', iterations,'.')
                p_i += 10
            #---
            p_i += 1

        #---
        #------------- Fin. -------------#

        #------------- Inicia la Extracción de datos de la pagina. -------------#
        #--- CICLO
        while page < iterations: #<--- Iteración de las páginas.
            #--- Estados
            PRICE_STATE = False
            NAME_STATE = False
            DESCRIPTION_STATE = False
            RATE_STATE = False
            #--- Variables
            allow = False #<--- Variables para hacer iteraciones en caso que algo falle.
            numstr = page #<--- Convierte en string el numero de pagina para su uso en la URL (evita un error al concatenar un numero con una cadena de texto).
            pagURL = URL + '&section_offset=' + str(numstr) #<--- Se estructura la URL con el número de página donde hara la busqueda.
            #--- Listas
            name = []
            link = []
            kind = []
            bed = []
            rates = []
            #---
            insert_log((CONSULTA[c][0]),"Se genero la URL de una página especifica del sitio.",pagURL,'263-264',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
            print('Correcto #11 -> Se genero la URL de la pagina = ', pagURL,'.')
            #---
            #NOTA:--TODO-- Crear variables de los selects.

            #--- Funcion para Obeterner el InnerHTML de la pagURL y devolver los datos ya filtrados.

            def load_HTML():
                container = get_innerHTML(pagURL) #<--- Carga toda la pagina.
                rooms = container.select("div .container_eodux1 .listingCardWrapper_9kg52c .infoContainer_v72lrv .linkContainer_15ns6vh") #<--- Filtra los datos.
                #---
                insert_log((CONSULTA[c][0]),"Se obtuvo el innerHTML de la pagina especificada.",pagURL,'276-283',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                print('Correcto #12 -> Se obtuvo el innerHTML de la pagina = ', pagURL,'.')
                return (BeautifulSoup(str(rooms), "html.parser")) #<--- Devuelve los datos filtrados.
            #---
            filters = get_innerHTML(pagURL) #<--- Variable donde se manejaran los datos filtrados
            #---
            #--- Obtener datos especificos.
            price = get_content(".search-results ._15ns6vh div ._1iurgbx ._g86r3e ._up0n8v6", filters) #<--- Filtra mas los datos para sacar los precios de los hospedajes.
            name = get_content(".search-results ._5ruk8 ._1xf3sln ._surdeb span", filters) #<--- Filtra mas los datos para obtener los nombres de los hospedajes.
            description = get_content(".search-results ._15ns6vh div div ._hylizj6 span", filters) #<--- Filtra más los datos para sacar la descripción de los hospedajes.
            r_rating = get_content(".search-results ._15ns6vh div div span ._36rlri", filters)
            r_link = get_content(".search-results ._5ruk8 ._1xf3sln ._surdeb", filters)
            #---
            #--- Se obtienen los precios
            attempts = 1 #<--- Variable con el numero de intentos.
            #---
            insert_log((CONSULTA[c][0]),"Inicia la extracción de precio.",pagURL,'298-325',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
            print('Correcto #13 -> Inicia la Extracción de los precios')
            #---CICLO
            while (allow == False):
                #---
                if len(price) == 0: #<--- Si el tamaño de la lista es 0, quiere decir que hay ningun precio, por ende hay un error y se debe iterar.
                    filters = get_innerHTML(pagURL)
                    price = get_content(".search-results ._15ns6vh div ._1iurgbx ._g86r3e ._up0n8v6", filters)
                    #---
                    insert_log((CONSULTA[c][0]),('Intento No.' + str(attempts) + ' - No hay ningun precio en los datos extraidos.'),'','300',pagURL,2) #tipo 0= error, 1= bien, 2= advertencia
                    print('Advertencia #14 -> No hay ningun precio en los datos extraidos. Precios = ',price)
                    #---
                    allow = False
                    PRICE_STATE = False
                #---
                elif (max_attempt == attempts): #<--- Cuando se alcanza el número maximo de intentos permitidos, se cancela.
                    PRICE_STATE = False
                    allow = True
                    #---
                    insert_log((CONSULTA[c][0]),('Se agotaron el número maximo de intentos (' + str(max_attempt) + ') sin ningun resultado'),'','310',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                    print('Error #14 -> Se agotaron el número maximo de intentos (',max_attempt,'). Precios = ',price)
                #---
                else:
                    PRICE_STATE = True
                    allow = True
                    #---
                    insert_log((CONSULTA[c][0]), ('Intento No.' + str(attempts) + ' - Se obtuvo correctamente los precios de los alojamientos'), '', '322 - 330', pagURL, 1) #tipo 0= error, 1= bien, 2= advertencia
                    print('Correcto #14 -> Se extrageron los precios. Precios = ',price)

                #---
                attempts +=1

            #---
            print('Descripci: ',description)
            #--- Extracción de la descripción de los Hospedajes
            if (PRICE_STATE == True): #<-- Condición, en caso que PRICE_STATE sea TRUE, como resultado de la extracción de precios, se realiza la siguiente tarea.
                allow = False #<--- Variables para hacer iteraciones en caso que algo falle.
                attempts = 1 #<--- Variable con el numero de intentos.
                price_ws = ([price_ws.replace(string3,"") for price_ws in price]) #<--- Se quita el simbolo de EURO en los precios.
                prices_1 = ([(re.findall(r"[-+]?\d*\.\d+|\d+", prices_1)) for prices_1 in price_ws]) #<--- Se quita la palabra "precio" (variable string) de los item de la lista, y se almacena el resultado en prices. Ex: (INIT= "Precio $300" RESULT = "$300").
                #prices = ([float(prices.replace(string,"")) for prices in price_ws])
                prices = []
                for temp_p in prices_1:
                    if temp_p:
                        #print(temp_p)
                        prices.append(float(temp_p[0]))
                #---
                print('Los precios')
                print(prices)
                new_desc = None
                #---
                insert_log((CONSULTA[c][0]),"Inicia la extracción de la descripciones.",pagURL,'329-389',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                print('Correcto #15 -> Inicia la Extracción de las descripciones')
                #---  CICLO
                while (allow == False):
                    #--- Variables temporales
                    i  = 0 #<--- Número de iteraciones.
                    filt_des = [] #<---Lista para filtrar la descripción.
                    #--- CICLO
                    print('desciptcion: ')
                    print(description)
                    while i < (len(description)):
                        filt_des.append(description[i]) #<--- Se extraen el tipo de hospedaje de la descripción.
                        i += 2
                        #---
                        num = (re.findall('\\d+', description[i])) #<--- Se extrae el numero de camas de la descripción.
                        filt_des.append(int(num[0]))
                        i += 2
                    print('new desciption: ',filt_des)
                    #--- Verificación del tamaño de la descripción Generalmente es el doble de la lista de precios (precios = 18 (items), descripción = 36(items)) salvo casos especiales.
                    if len(prices) == (len(filt_des)/2): #<--- Si el numero de item en la lista de precios es igual a la mitad de numeros de item en filt_des.
                        new_desc = filt_des #<--- se pasan los valores de la lista temporal filt_des a new_des para
                        allow = True
                        DESCRIPTION_STATE = True
                        #---
                        insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - Se obtuvo correctamente las descripciones de los alojamientos"),'','353',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                        print('Correcto #16 -> Se extrageron las descipciones. Descripción = ',filt_des)
                    #---
                    elif (len(prices) > (len(filt_des)/2)): #<--- Si el número de items en la lista de precios es mayor a la mitad de numeros de items en filt_des. Ex: (precios = 18 (items), descripción = 35(items))
                        new_desc = reformat_desc(filt_des) #<-- Se llama a la funcion reformat_des para q reorganize los item y los devuelva completos.
                        #---
                        insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - Se re-ordeno correctamente las descripciones de los alojamientos"),'','361',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                        print('Correcto #16 -> Se extrageron las descipciones. Descripción = ',new_desc)
                        #---
                        DESCRIPTION_STATE = True
                        allow = True
                    #---
                    elif (max_attempt == attempts): #<--- Cuando se alcanza el número maximo de intentos permitidos, se cancela.
                        DESCRIPTION_STATE = False
                        allow = True
                        #---
                        insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado"),'','370',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                        print('Error #16 -> Se agotaron el número maximo de intentos (',max_attempt,'). Descripción = ',filt_des)
                    #---
                    else: #<--- Se itera en busca del funcionamiento de los algoritmos.
                        filters = get_innerHTML(pagURL)
                        description = get_content(".search-results ._15ns6vh div div ._hylizj6 span", filters)
                        allow = False
                        DESCRIPTION_STATE = False
                        #---
                        insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - El tamaño de la lista no es el requerido."),'','377',pagURL,2) #tipo 0= error, 1= bien, 2= advertencia
                        print('Advertencia #16 -> El tamaño de la lista de descripciones no es valido . Descripción = ',filt_des)
                    #---
                    attempts += 1


                #--- Extracción de los precios de los hospedajes
                if (DESCRIPTION_STATE == True): #<-- Condición, en caso que DESCRIPTION_STATE sea TRUE, como resultado de la extracción de las descripciones, se realiza la siguiente tarea.
                    #---
                    insert_log((CONSULTA[c][0]),"Inicia la extracción de los nombres.",pagURL,'389-460',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                    print('Correcto #17 -> Inicia la Extracción de los nombres')
                    #--- Varaibles temporales
                    i = 0 #<--- Número de iteraciones.
                    allow = False
                    #---
                    #--- Se obtienen el tipo y el numero de cama
                    #--- CICLO
                    print(new_desc)
                    while i < (len(new_desc)):
                        kind.append(new_desc[i]) #<--- Se extraen el tipo de hospedaje de la descripción.
                        i += 1
                        #---
                        #num = (re.findall('\\d+', new_desc[i])) #<--- Se extrae el numero de camas de la descripción.
                        bed.append(new_desc[i])
                        i += 1
                    #---
                    insert_log((CONSULTA[c][0]),"Se ha separado correctamente el tipo de hospedaje y el número de camas.",pagURL,'400-407',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                    print('Correcto #18 -> Se ha separado correctamente el tipo de hospedaje y el número de camas')
                    #--- Extracción del nombre
                    i = 0 #<--- Número de iteraciones.
                    while (allow == False):
                        #---
                        if len(name) == 0: #<--- Si el tamaño de la lista de nombres obtenidos es 0.
                            filters = get_innerHTML(pagURL) #<--- Variable donde se manejaran los datos filtrados
                            name = get_content(".search-results ._15ns6vh div ._1iurgbx ._up0n8v6 span", filters) #<--- se filtra nuevamente para obtener el nombre.
                            r_rating = get_content(".search-results ._15ns6vh div div span ._36rlri ", filters)
                            NAME_STATE = False
                            allow = False
                            #---
                            insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - El tamaño de la lista no es el requerido."),'','414',pagURL,2) #tipo 0= error, 1= bien, 2= advertencia
                            print('Advertencia #19 -> El tamaño de la lista de nombres no es valido . Nombres = ',name)
                        #---
                        elif (max_attempt == attempts): #<--- Cuando se alcanza el número maximo de intentos permitidos, se cancela.
                            NAME_STATE = False
                            allow = True
                            #---
                            insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado"),'','423',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                            print('Error #19 -> Se agotaron el número maximo de intentos (',max_attempt,'). Nombres = ',name)
                        #---
                        else: #<--- si se cargan los nombres se termina la iteración.
                            NAME_STATE = True
                            allow = True
                            for g_link in filters.select(".search-results ._5ruk8 ._1xf3sln ._surdeb"):
                                temp_link = g_link.get('href')
                                f_link = temp_link.split('?')
                                last_link = str(PORTAL[2]) + str(f_link[0])
                                print(last_link)
                                link.append(str(last_link))
                            #---
                            insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - Se obtuvo correctamente los nombres de los alojamientos"),'','430',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                            print('Correcto #19 -> Se extrageron los Nombres. Descripción = ',name)
                        #--
                        attempts += 1 #<--- Intentos

                    #---
                    #--- Almacenamiento en la base de datos
                    if (NAME_STATE == True): #<-- Condición, en caso que NAME_STATE sea TRUE, como resultado de la extracción de nombres, se realiza la siguiente tarea.
                         #---
                        insert_log((CONSULTA[c][0]),"Inicia la extracción de los valoraciones.",pagURL,'389-460',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                        print('Correcto #17 -> Inicia la Extracción de los nombres')
                        #--- Varaibles temporales
                        i = 0 #<--- Número de iteraciones.
                        allow = False
                        #---
                        S_PROGRAM = True
                        #---
                        while (allow == False):
                            #---
                            print(r_rating)
                            if len(r_rating) == 0: #<--- Si el tamaño de la lista de nombres obtenidos es 0.
                                filters = get_innerHTML(pagURL) #<--- Variable donde se manejaran los datos filtrados
                                r_rating = get_content(".search-results ._15ns6vh div div span ._36rlri", filters) #<--- se filtra nuevamente para obtener el nombre.
                                RATE_STATE = False
                                allow = False
                                #---
                                insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - El tamaño de la lista no es el requerido."),'','414',pagURL,2) #tipo 0= error, 1= bien, 2= advertencia
                                print('Advertencia #19 -> El tamaño de la lista de nombres no es valido . Nombres = ',r_rating)
                            #---
                            elif (max_attempt == attempts): #<--- Cuando se alcanza el número maximo de intentos permitidos, se cancela.
                                RATE_STATE = False
                                allow = True
                                #---
                                insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado"),'','423',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                                print('Error #19 -> Se agotaron el número maximo de intentos (',max_attempt,'). Nombres = ',r_rating)
                            #---
                            else: #<--- si se cargan los nombres se termina la iteración.
                                RATE_STATE = True
                                allow = True
                                temporal_rate = []
                                a = 0
                                for rate in filters.select(".search-results ._15ns6vh div div span ._36rlri ._hzkfa span"):
                                    if (rate.get('aria-label') != None):
                                        temp_rate = rate.get('aria-label')
                                        temp_filt = (re.findall(r"[-+]?\d*\.\d+|\d+", temp_rate))
                                        end = float(temp_filt[0])
                                        temporal_rate.append(end)
                                #---
                                print(temporal_rate)
                                last_rate = []
                                for filt in r_rating:
                                    print("filter: ",filt)
                                    if filt:
                                        last_rate.append(filt)
                                #---
                                print('last_rate: ')
                                print(last_rate)
                                for i in range(len(last_rate)):
                                    if (string4 in last_rate[i] or string5 in last_rate[i] or string6 in last_rate[i] or last_rate[i] == string7 or last_rate[i] == string8 or last_rate[i] == string9 or last_rate[i] == ''):
                                        rates.append(0)
                                    else:
                                        rates.append(temporal_rate[a])
                                        a += 1
                                #---
                                print('total rate')
                                print(rates)
                                insert_log((CONSULTA[c][0]),("Intento No." + str(attempts) + " - Se obtuvo correctamente los nombres de los alojamientos"),'','430',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                                print('Correcto #19 -> Se extrageron los Nombres. Descripción = ',r_rating)
                            #--
                            attempts += 1 #<--- Intentos
                        #---
                        if(RATE_STATE == True):
                            #---
                            insert_log((CONSULTA[c][0]),"Inicia el listado en variables de los datos extraidos para su inserción en la BD.",pagURL,'461 - 492',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                            print('Correcto #21 -> Inicia el listado en variables de los datos extraidos para su inserción en la BD.')
                            #---
                            if (len(prices) == len(name) and len(name) == len(kind) and len(kind) == len(bed) and len(bed) == len(rates) and len(rates) == len(link)):
                                #---Variable
                                i = 0 #<-- Iteraciones
                                #--- Almacenamiento de datos
                                #--- CICLO
                                while i < (len(prices)):
                                    #--- Variables Generales
                                    L_POS.append(position)
                                    L_NAME.append(name[i])
                                    L_LINK.append(link[i])
                                    L_PRICE.append(prices[i])
                                    L_KIND.append(kind[i])
                                    L_BED.append(bed[i])
                                    L_RATE.append(rates[i])
                                    #---
                                    insert_log((CONSULTA[c][0]),"Se agregaron los datos de los hospedajes extaidos a la nuevas lista que sera usada para insertar en la BD.",("Precios = " + str(len(prices)) +', Nombres = ' + str(len(name)) + ', Tipos = ' + str(len(kind)) + ' & Camas = ' + str(len(bed)) + ', Valoración = ' + str(len(rates)) + ' y Links = ' + str(len(link)) ),'471 - 482',pagURL,1) #tipo 0= error, 1= bien, 2= advertencia
                                    print('Correcto #22 -> Se agregaron los datos de los hospedajes extaidos a la nuevas lista que sera usada para insertar en la BD.')
                                    position += 1 #<--- Se aumenta en uno la posición.
                                    i += 1 #<--- Se aumenta la iteración
                            #---
                            else:
                                #---
                                page -= 1
                                S_PROGRAM = False
                                #---
                                insert_log((CONSULTA[c][0]),("El tamaño de las listas no concuerda."),("Precios = " + str(len(prices)) +', Nombres = '+ str(len(name)) + ', Tipos = '+ str(len(kind)) + ' & Camas = ' + str(len(bed))),'466',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                                print('Error -> El tamaño de las listas no concuerda., se iterara toda la página.')
                                #---TODO--- LOG here.

                        #---
                        else:
                            #---
                            page -= 1
                            S_PROGRAM = False
                            #---
                            insert_log((CONSULTA[c][0]),("El tamaño de las listas no concuerda."),("Precios = " + str(len(prices)) +', Nombres = '+ str(len(name)) + ', Tipos = '+ str(len(kind)) + ' & Camas = ' + str(len(bed))),'466',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                            print('Error -> El tamaño de las listas no concuerda., se iterara toda la página.')
                            #---TODO--- LOG here.
                            #
                    #---
                    else:
                        #---
                        page -= 1
                        S_PROGRAM = False
                        #---
                        insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado, se iterara toda la página."),'','423',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                        print('Error -> Se agotaron el número maximo de intentos (',max_attempt,') sin ningun resultado, se iterara toda la página.')
                        #---TODO--- LOG here.
                #---
                else:
                    #---
                    page -= 1
                    S_PROGRAM = False
                    #---
                    insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado, se iterara toda la página."),'','370',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                    print('Error -> Se agotaron el número maximo de intentos (',max_attempt,') sin ningun resultado, se iterara toda la página.')
                    #---TODO--- LOG here.

            #---
            else:
                #---
                page -= 1
                S_PROGRAM = False
                #---
                insert_log((CONSULTA[c][0]),("Se agotaron el número maximo de intentos (" + str(max_attempt) + ") sin ningun resultado, se iterara toda la página."),'','310',pagURL,0) #tipo 0= error, 1= bien, 2= advertencia
                print('Error -> Se agotaron el número maximo de intentos (',max_attempt,') sin ningun resultado, se iterara toda la página.')
                #---TODO--- LOG here.

            #---
            if (CONSULTA[c][12] != None):
                if (page == CONSULTA[c][12]):
                    page = page + iterations
                    print('----END = ',page)
            page += 1 #<--- Se pasa a la siguiente pagina.
            #---
    #---
    #--- Se Obtiene los datos de la competencia.
    try:
        del C_ID[:]
        del C_NAME[:]
        del C_KIND[:]
        del C_LINK[:]
        del COMPETENCIA[:]
        #---
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "SELECT * FROM SCR_COMPETENCIA WHERE ID_PORTAL = " + str(PORTAL[0])
            cursor.execute(sql)
            COMPETENCIA = cursor.fetchall()
            #---
            #print(COMPETENCIA)
            print('Correcto #23 -> Extracción de los datos de los "anuncios" de la competencia.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error #23 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

    #--- Ciclo para Ingresar los datos de hospedajes obtenidos de la tabla anuncios
    for i in range(len(COMPETENCIA)):
        #--- Limpiamos las listas para ingresar nuevamente todos los datos de la competencia.
        C_ID.append(COMPETENCIA[i][0])
        C_NAME.append(COMPETENCIA[i][1])
        C_KIND.append(COMPETENCIA[i][5])
        C_LINK.append(COMPETENCIA[i][6])
        #---
        #print('id: ',C_ID[i],', name: ',C_NAME[i],', tipo: ',C_KIND[i],'--')
    print('Correcto #24 -> Se asignaron los valores de "anuncios" de la competencia extraido extraidos en las listas a usar.')
    #---
    #---
    insert_log((CONSULTA[c][0]),"Inicia la inserción de los datos en la Base de Datos.",URL,'425 - 560',URL,1) #tipo 0= error, 1= bien, 2= advertencia
    print('Correcto #25 -> Inicia la inserción de los datos en la Base de Datos.')
    #---
    if (len(L_POS) == len(L_NAME) and len(L_NAME) == len(L_PRICE) and len(L_PRICE) == len(L_KIND) and len(L_KIND) == len(L_BED) and len(L_BED) == len(L_RATE) and len(L_RATE) == len(L_LINK) ):
        #--- CICLO
        for i in range(len(L_NAME)):
            #--- Variables
            competition_id = None #<--- Almacena el Id de la competencia.
            own_id = None #<--- Almacena el Id de los anuncios propia.
            #--- COMPARACIÓN DE ANUNCIO
            for itera1 in range(len(A_LINK)):
                #---
                if (L_LINK[i] in A_LINK[itera1]):
                    own_id = ID[itera1] #<--- ID.
                    time.sleep(60)
                print('Empresa link = ', A_LINK[itera1],' ----- Anuncio link = ',(L_LINK[i]))
                #---
            #---
            for itera2 in range(len(C_NAME)):
                #---
                if (str.lower(C_NAME[itera2]) == str.lower(L_NAME[i]) and (C_LINK[itera2]).replace(" ", "") == (L_LINK[i]).replace(" ", "")):
                    competition_id = C_ID[itera2] #<--- C_ID.
                #---
            #---
            try:
                with connection.cursor() as cursor:
                    # Create a new record.
                    if (own_id != None):
                        sql = "INSERT INTO SCR_ANUNCIANTES (ID_ANUNCIO,FECHAI,FECHAF,ORDEN,ID_PORTAL,PRECIO,ID_CONSULTA,TIPO,N_CAMA,RATIO) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (own_id,CHECKIN.strftime('%Y-%m-%d'),CHECKOUT.strftime('%Y-%m-%d'),L_POS[i],PORTAL[0],L_PRICE[i],CONSULTA[c][0],L_KIND[i],L_BED[i],L_RATE[i]))
                        connection.commit()
                    #---
                    elif (competition_id != None):
                        sql = "INSERT INTO SCR_ANUNCIANTES (ID_COMPETENCIA,FECHAI,FECHAF,ORDEN,ID_PORTAL,PRECIO,ID_CONSULTA,TIPO,N_CAMA,RATIO) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (competition_id,CHECKIN.strftime('%Y-%m-%d'),CHECKOUT.strftime('%Y-%m-%d'),L_POS[i],PORTAL[0],L_PRICE[i],CONSULTA[c][0],L_KIND[i],L_BED[i],L_RATE[i]))
                        connection.commit()
                    #----
                    else:
                        #---
                        sql = u"INSERT INTO SCR_COMPETENCIA (TITULO,ID_PORTAL,TIPO,URL) VALUES (%s, %s, %s, %s)"
                        cursor.execute(sql, (L_NAME[i], PORTAL[0], L_KIND[i], L_LINK[i]))
                        print('----INSERT COMP NAME= ',L_NAME[i],', PORTAL = ', PORTAL[0],', KIND = ',L_KIND[i],'')
                        connection.commit()
                        time.sleep(1)
                        #--- Consulta especifica
                        sql = "SELECT * FROM SCR_COMPETENCIA WHERE ID_PORTAL = %s AND TITULO = %s AND TIPO = %s AND URL = %s"
                        cursor.execute(sql, (str(PORTAL[0]), L_NAME[i], L_KIND[i], L_LINK[i]))
                        temp_comp = cursor.fetchone()
                        print('---------ID=',temp_comp)
                        if (temp_comp == None):
                            cursor.execute(sql, (str(PORTAL[0]), L_NAME[i], L_KIND[i]))
                            temp_comp = cursor.fetchone()
                            print('------second try---ID=',temp_comp)
                        #---
                        C_ID.append(temp_comp[0])
                        C_NAME.append(L_NAME[i])
                        C_KIND.append(L_KIND[i])
                        C_LINK.append(L_LINK[i])
                        #---
                        sql = "INSERT INTO SCR_ANUNCIANTES (ID_COMPETENCIA,FECHAI,FECHAF,ORDEN,ID_PORTAL,PRECIO,ID_CONSULTA,TIPO,N_CAMA,RATIO) VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(sql, (temp_comp[0],CHECKIN.strftime('%Y-%m-%d'),CHECKOUT.strftime('%Y-%m-%d'),L_POS[i],PORTAL[0],L_PRICE[i],CONSULTA[c][0],L_KIND[i],L_BED[i],L_RATE[i]))
                        #---
                        connection.commit()
                        insert_log((CONSULTA[c][0]),"Inicia la inserción de los datos en la Base de Datos.",URL,'572-599',URL,1) #tipo 0= error, 1= bien, 2= advertencia
                # connection is not autocommit by default. So you must commit to save
                # your changes.


            finally:
                print('Correcto #26 -> Finaliza la inserción de los datos en la Base de Datos.')
    else:
        #---
        insert_log((CONSULTA[c][0]),"El Tamaño de la lista de elementos a ingresar difieren entre ellos.",URL,'602',URL,0) #tipo 0= error, 1= bien, 2= advertencia
        #----
    #---
        print('Error -> El tamaño de la lista de los elementos a ingresar a la BD, No Son iguales.')
    print("------------------------------------------------")
    if(S_PROGRAM == True and ALLOW_RESET  == True):
        #---
        t_day = datetime.datetime.now()
        f_schedule = t_day + datetime.timedelta(days=(CONSULTA[c][9])) #<--- se aumenta en el rango de días que asigna la consulta la proxima fecha en la que se ejecutara la consulta.
        #---
        try:
        #---
            with connection.cursor() as cursor:
                sql = "UPDATE SCR_CONSULTAS SET FECHA_PROGRA = %s WHERE ID_CONSULTA = %s"
                cursor.execute(sql,(f_schedule.date(),CONSULTA[c][0]))
                insert_log((CONSULTA[c][0]),'Se ha modificado la fecha en que se ejecutara nuevamente el script.',('proxima fecha: ' + str(f_schedule)),'196-210','',1) #tipo 0= error, 1= bien, 2= advertencia
                print('Correcto -> Se ha modificado la fecha en que se ejecutara nuevamente el script.')
            #---
                connection.commit()
            #---
        except _mssql.MssqlDatabaseException as e:
            print('Error# -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    elif (S_PROGRAM == True and ALLOW_RESET  == False):
        #---
        t_day = None
        if(CONSULTA[c][11] != None):
            t_day = CONSULTA[c][11]
        else:
            t_day = datetime.datetime.now()
        #---
        f_schedule = t_day + datetime.timedelta(days=(CONSULTA[c][9])) #<--- se aumenta en el rango de días que asigna la consulta la proxima fecha en la que se ejecutara la consulta.
        #---
        try:
        #---
            with connection.cursor() as cursor:
                sql = "UPDATE SCR_CONSULTAS SET FECHA_PROGRA = %s WHERE ID_CONSULTA = %s"
                cursor.execute(sql,(f_schedule.date(),CONSULTA[c][0]))
                insert_log((CONSULTA[c][0]),'Se ha modificado la fecha en que se ejecutara nuevamente el script.',('proxima fecha: ' + str(f_schedule)),'196-210','',1) #tipo 0= error, 1= bien, 2= advertencia
                print('Correcto -> Se ha modificado la fecha en que se ejecutara nuevamente el script.')
            #---
                connection.commit()
            #---
        except _mssql.MssqlDatabaseException as e:
            print('Error# -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    print("----- Finaliza consulta ID: ",(CONSULTA[c][0]))
#-----
print("------------------------------------------------")
print("Termino :)")
print("------------------------------------------------")
driver.quit() #<--- Se cierra y borra de memoria al naavegador.
connection.close() #<--- Se finaliza la conexión con la base de datos.

import time
import re
import ssl
import urllib.parse
import urllib.request
import sqlite3
import requests
from datetime import datetime
from bs4 import BeautifulSoup


start_time = time.time()

conn = sqlite3.connect('Osce.sqlite')
cur = conn.cursor()

# Do some setup
cur.executescript('''
CREATE TABLE IF NOT EXISTS Datos_generales (
    id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    nomenclatura TEXT,
    num_convocatoria   TEXT,
    tipo_compra   TEXT,
    normativa_aplicable   TEXT,
    version_seace   INTEGER,
    entidad_convocante   TEXT,
    direccion_legal   TEXT,
    pagina_web   TEXT,
    telefono   TEXT,
    objeto_contratacion   TEXT,
    descripcion_objeto   TEXT,
    valor_estimado   TEXT,
    montoparticipacion   TEXT,
    fecha_publicacion   TEXT
);

CREATE TABLE IF NOT EXISTS CRONOGRAMA (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    codConvocatoria  INTEGER,
    etapa TEXT,
    fechaInicio TEXT,
    fechaFin TEXT
);

CREATE TABLE IF NOT EXISTS ListaDocumentos (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    codConvocatoria INTEGER,
    Numero   TEXT,
    etapa   TEXT,
    documento   TEXT,
    linkArchivo TEXT,
    nombreDocumento TEXT,
    fechaHora   TEXT,
    Acciones    TEXT
);

CREATE TABLE IF NOT EXISTS DOCHTML (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    codConvocatoria INTEGER,
    DodHTML BLOB
);

''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

num1 = input('Enter intial number: ')
num2 = input('Enter final number: ')


count = 0
errores = 0
url_base="https://prodapp2.seace.gob.pe/seacebus-uiwd-pub/fichaSeleccion/fichaSeleccion.xhtml?idSistema=3&ongei=1&idConvocatoria="
for id in range(int(num1),int(num2)):

    cur.execute("SELECT id FROM Datos_generales WHERE id= ? ",
    (id,))
    try:
        data = cur.fetchone()[0]
        #print("Found in database ", line)
        continue
    except:
        pass

    url = url_base+str(id)
    #print(url)
    try:
        html = requests.get(url)

    except:
        errores = errores + 1
        print('Error ', errores, 'has occurred, pausing 6sec')
        time.sleep(6)
        html = requests.get(url)


    soup = BeautifulSoup(html.content, 'html.parser')
    #TABLA DE Información General
    infGeneral=soup.find('table', id='tbFicha:j_idt842')
    
    intento = 1
    while infGeneral == None:
        # time.sleep(.1)
        html = requests.get(url)
        soup = BeautifulSoup(html.content, 'html.parser')

        infGeneral=soup.find('table', id='tbFicha:j_idt842')
        intento=intento+1
        print("intento")

    html=html.content
    cur.execute('''INSERT INTO DOCHTML (codConvocatoria, DodHTML)
            VALUES ( ?, ?)''',
            (id, html) )
    conn.commit()

    if infGeneral != None:
        td=infGeneral.find_all('td')
        nomenclatura=td[1].text.strip()
        num_convocatoria=td[3].text.strip()
        tipo_compra=td[5].text.strip()
        normativa_aplicable=td[7].text.strip()
        version_seace=td[9].text.strip()
    else:
        nomenclatura=None
        num_convocatoria=None
        tipo_compra=None
        normativa_aplicable=None
        version_seace=None

    #TABLA DE Información General de la Entidad
    infGeneral=soup.find('table', id='tbFicha:j_idt890')

    if infGeneral != None:

        td=infGeneral.find_all('td')
        entidad_convocante=td[1].text.strip()
        direccion_legal=td[3].text.strip()
        pagina_web=td[5].text.strip()
        telefono=td[7].text.strip()
    else:
        entidad_convocante=None
        direccion_legal=None
        pagina_web=None
        telefono=None
    #TABLA Información general del procedimiento
    infGeneral=soup.find('table', id='tbFicha:j_idt914')

    if infGeneral != None:

        td=infGeneral.find_all('td')
        objeto_contratacion=td[1].text.strip()
        descripcion_objeto=td[3].text.strip()
        valor_estimado=td[5].text.strip()
        montoparticipacion=td[7].text.strip()
        fecha_publicacion=td[9].text.strip()
    else:
        objeto_contratacion=None
        descripcion_objeto=None
        valor_estimado=None
        montoparticipacion=None
        fecha_publicacion=None


    descripcion_objeto=soup.find('div', id='tbFicha:dialogDescObj')
    descripcion_objeto=descripcion_objeto.find_all('div')
    descripcion_objeto=descripcion_objeto[1].text.strip()
    #print(descripcion_objeto)


    cur.execute('''INSERT INTO Datos_generales (id, nomenclatura, num_convocatoria, tipo_compra, normativa_aplicable,
     version_seace, entidad_convocante, direccion_legal, pagina_web, telefono, objeto_contratacion,
      descripcion_objeto, valor_estimado, montoparticipacion, fecha_publicacion)
            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (id, nomenclatura, num_convocatoria, tipo_compra, normativa_aplicable, version_seace,
            entidad_convocante, direccion_legal, pagina_web, telefono, objeto_contratacion, descripcion_objeto,
             valor_estimado, montoparticipacion, fecha_publicacion) )
    conn.commit()
    print(version_seace)

    #LISTA ListaDocumentos

    ListaDoc=soup.find('div', id='tbFicha:dtDocumentos')


    if ListaDoc != None:
        filas=ListaDoc.find_all('tr')
    #    print(filas)
        i=1
        while True:
            try:
                avance=filas[i].find_all('td')

                Numero=avance[0].text.strip()
                etapa=avance[1].text.strip()
                documento=avance[2].text.strip()

                linkArchivo="Pronto"


                nombreDocumento=avance[4].text.strip()
                fechaHora=avance[5].text.strip()
                Acciones=avance[6].text.strip()
                i=i+1
                cur.execute('''INSERT INTO ListaDocumentos (codConvocatoria, Numero, etapa, documento,
                linkArchivo, nombreDocumento, fechaHora, Acciones)
                        VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (id, Numero, etapa, documento, linkArchivo, nombreDocumento, fechaHora, Acciones))
                conn.commit()
            except:
                break

    #LISTA Cronogramma

    ListaDoc=soup.find('div', id='tbFicha:dtCronograma')


    if ListaDoc != None:
        filas=ListaDoc.find_all('tr')
    #    print(filas)
        i=1
        while True:
            try:
                avance=filas[i].find_all('td')

                etapa=avance[0].text.strip()
                fechaInicio=avance[1].text.strip()
                fechaFin=avance[2].text.strip()
                i=i+1

                cur.execute('''INSERT INTO ListaDocumentos (codConvocatoria, etapa, fechaInicio, fechaFin)
                        VALUES ( ?, ?, ?, ?)''',
                        (id, etapa, fechaInicio, fechaFin))
                conn.commit()
            except:
                break

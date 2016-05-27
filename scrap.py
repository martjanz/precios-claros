# -*- coding: utf-8 -*-
from __future__ import division # force floating point in integer division (Python 2)

import sys
import math
import os.path
import ujson
import requests
import grequests
import sqlite


def main():
    # global variables (wrapper class, maybe?)
    stemUrl = 'https://8kdx6rx8h4.execute-api.us-east-1.amazonaws.com/prod/'
    datadir = 'data'
    configFile = '_progress.json' # Configuración, info de avance
    posFile = datadir + '/comercios.json' # Comercios, sucursales, puntos de venta
    categoriesFile = datadir + '/categorias.json' # Categorías de artículos
    articlesFile = datadir + '/productos.json' # Artículos

    dbFilename = "data.sqlite" # SQLite DB

    dbLayer = sqlite.DBLayer(dbFilename)

    # progress info (for resume support)
    progress = readConfig(configFile)

    # No hay comercios descargados
    if (not progress['comercios']):
        # Obtiene comercios
        comercios = getSucursales(stemUrl)

        # Graba en db
        insertData = []
        i = 0
        for comercio in comercios:
            insertData.append((
                i,
                comercio['id'],
                comercio['comercioId'],
                comercio['banderaId'],
                comercio['banderaDescripcion'],
                comercio['provincia'],
                comercio['localidad'],
                comercio['direccion'],
                comercio['lat'],
                comercio['lng'],
            ))
            i += 1

        dbLayer.insertMany('comercios',
            '(_id, id, comercioId, banderaId, banderaDescripcion, provincia, localidad, \
            direccion, lat, lng)',
            '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            insertData
        )

        progress['comercios'] = True
        saveConfig(progress, configFile)

    # No hay categorías descargadas
    if (not progress['categorias']):
        # Obtiene categorías
        categorias = getCategorias(stemUrl)

        # Graba en db
        insertData = []
        i = 0
        for categoria in categorias:
            insertData.append((
                i,
                categoria['id'],
                categoria['nivel'],
                categoria['nombre'],
                categoria['productos'],
            ))
            i += 1

        dbLayer.insertMany('categorias',
            '(_id, id, nivel, nombre, cant_productos)',
            '(?, ?, ?, ?, ?)',
            insertData
        )

        progress['categorias'] = True
        saveConfig(progress, configFile)


    # Obtiene los comercios que todavía no se levantaron precios
    comCursor = dbLayer.conn.cursor()
    comercios = comCursor.execute("SELECT * FROM comercios WHERE pendiente = 1").fetchall()

    if (os.path.isfile('_cantarticulos.json')):
        with open('_cantarticulos.json', mode='r') as cantArtFile:
            cantArticulos = ujson.load(cantArtFile)
    else:
        cantArticulos = getCantArticulos(stemUrl, comercios)
        with open('_cantarticulos.json', 'w') as outfile:
            ujson.dump(cantArticulos, outfile)

    comercios = comCursor.execute("SELECT * FROM comercios WHERE pendiente = 1").fetchall()

    if len(comercios) == 0:
        progress['productos'] = True

        with open(configFile, 'w') as outfile:
            ujson.dump(progress, outfile)

        return

    for comercio in comercios:
        for item in cantArticulos:
            if comercio['id'] == item['id']:
                cantProductos = item['total']
                maxPermitido = item['maxLimitPermitido']

        articulos = getArticulos(stemUrl, comercio['id'], cantProductos, maxPermitido)

        # Graba artículos en db
        insertData = []
        for articulo in articulos:
            insertData.append((
                articulo['id'],
                articulo['marca'],
                articulo['nombre'],
                articulo['presentacion']
            ))

        dbLayer.insertMany('articulos',
            '(id, marca, nombre, presentacion)',
            '(?, ?, ?, ?)',
            insertData
        )

        # Graba precios en db
        insertData = []
        for articulo in articulos:
            insertData.append((
                comercio['id'],
                articulo['id'],
                articulo['precio'],
            ))

        dbLayer.insertMany('precios',
            '(id_comercio, id_articulo, precio)',
            '(?, ?, ?)',
            insertData
        )

        # save progress in database
        updateCursor = dbLayer.conn.cursor()
        comercios = updateCursor.execute("UPDATE comercios SET pendiente = 0 \
            WHERE id = '" + comercio['id'] + "'")
        dbLayer.conn.commit()

    # save progress
    progress['comercios'] = True
    with open(configFile, 'w') as outfile:
        ujson.dump(progress, outfile)

    raise Exception()


# Obtiene comercios ("sucursales")
def getSucursales(stemUrl):
    sucursales = []
    mainUrl = stemUrl + 'sucursales'

    print 'Recolectando información sobre comercios...'
    data = getJsonData(mainUrl)

    cantSucursales = data['total']
    maxLimit = data['maxLimitPermitido']
    cantPages = int(math.ceil(cantSucursales / maxLimit))

    urls = []
    print ('Descargando comercios...')
    for x in xrange(1, cantPages + 1):
        urls.append(mainUrl + '?offset=' + str((x - 1) * maxLimit) + '&limit=' + str(maxLimit))

    rs = (grequests.get(u) for u in urls)
    responses = grequests.map(rs)
    for response in responses:
        data = ujson.loads(response.content)
        sucursales = sucursales + data['sucursales']
        response.close()

    return sucursales


# Obtiene categorías
def getCategorias(stemUrl):
    mainUrl = stemUrl + 'categorias'

    print ('Descargando categorías...')
    data = getJsonData(mainUrl)
    categorias = data['categorias']

    return categorias


# Obtiene productos de un comercio
def getArticulos(stemUrl, idComercio, totalProductos, maxPermitido):
    # default seconds to launch timeout exception
    timeoutSecs = 20
    concurrents = 20
    articulos = []
    urls = []

    mainUrl = stemUrl + 'productos' + '?id_sucursal=' + idComercio
    cantPages = int(math.ceil(totalProductos / maxPermitido))

    print ('Descargando ' + str(totalProductos) + ' productos de comercio ' + str(idComercio) + '...')

    for x in xrange(1, cantPages + 1):
        urls.append(mainUrl + \
            '&offset=' + str((x - 1) * maxPermitido) + \
            '&limit=' + str(maxPermitido))

    rs = (grequests.get(u, stream = False, timeout = timeoutSecs) for u in urls)
    responses = grequests.imap(rs, exception_handler = timeoutExceptionHandler, size = concurrents)

    for response in responses:
        if hasattr(response, 'content'):
            data = ujson.loads(response.content)
            response.close()
        elif 'content' in response:
            data = {}
            data['productos'] = response['content']
        else:
            try:
                data = ujson.loads(response)
                # print "--- try ---"
                # print data
                # data = data['content']
            except:
                # print response
                raise Exception()

        if 'errorMessage' in data:
            # Algo falló en el servidor.
            raise Exception('Server Error in productos: ' + data['errorMessage'])

        if 'productos' not in data:
            # print '--- dir(data) ---'
            # print dir(data)
            # print '--- data ---'
            # print data
            raise Exception()

        articulos = articulos + data['productos']

    return articulos

def getCantArticulos(stemUrl, comercios):
    timeoutSecs = 30
    mainUrl = stemUrl + 'productos' + '?id_sucursal='
    concurrents = 20
    urls = []
    reqCounter = 0
    responses = []

    print "Obteniendo cantidad de artículos por comercio..."

    session = requests.Session()
    for comercio in comercios:
        urls.append(mainUrl + comercio['id'])

    rs = (grequests.get(u,
            stream = False,
            session = session,
            timeout = timeoutSecs) for u in urls)

    for r in grequests.imap(rs, size = concurrents):
        response = ujson.loads(r.text)
        idComercio = r.url[r.url.rfind('=', 0, len(r.url)) + 1:]

        responses.append({
            "id": idComercio,
            "total": response['total'],
            "maxLimitPermitido": response['maxLimitPermitido'],
        })

        r.close() # Close open connections

    return responses


def timeoutExceptionHandler(request, exception):
    # Retry request timeout (in seconds)
    retryTimeout = 20
    print "----- Timeout ------"
    print "Retrying " + request.url + "..."

    productos = []

    rs = [grequests.get(request.url, timeout = retryTimeout)]
    responses = grequests.map(rs, exception_handler = timeoutExceptionHandler)

    for response in responses:
        if hasattr(response, 'content'):
            data = ujson.loads(response.content)
            response.close()
        elif 'content' in response:
            data = {}
            data['productos'] = response['content']
        else:
            try:
                data = ujson.loads(response)
                # print "--- try ---"
                # print data
                data = data['content']
            except:
                # print response
                raise Exception()

        if 'errorMessage' in data:
            # Algo falló en el servidor.
            raise Exception('Server Error in productos: ' + data['errorMessage'])

        if 'productos' not in data:
            # print '--- dir(data) ---'
            # print dir(data)
            # print '--- data ---'
            # print data
            raise Exception()

        productos = productos + data['productos']

    return {
        'content': productos
    }

# Get json data from url.
#   Returns dict
def getJsonData(url):
    req = requests.get(url, timeout = 10)
    data = req.json()

    # chequear estado = 200, si no reintentar x veces.
    # Si después de x veces sigue sin funcionar devolver error.

    return data

# Read progress and config. Create file if not exists.
def readConfig(configFile):
    if (os.path.isfile(configFile)):
        with open(configFile, mode='r') as progressFile:
            progress = ujson.load(progressFile)
    else:
        progress = {
            'last_commerce': -1,
            'comercios': False,
            'categorias': False,
            'productos': False
        }
    return progress


# Read progress and config. Create file if not exists.
def saveConfig(progress, configFile):
    with open(configFile, 'w') as outfile:
        ujson.dump(progress, outfile)
    pass

if __name__ == "__main__":
    main()

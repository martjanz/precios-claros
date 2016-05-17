# -*- coding: utf-8 -*-
import sys
import math
import os.path
import ujson
import requests
import grequests


def main():
    # global variables (wrapper class, maybe?)
    stemUrl = 'https://8kdx6rx8h4.execute-api.us-east-1.amazonaws.com/prod/'
    datadir = 'data'
    configFile = datadir + '/_progress.json' # Configuración, info de avance
    posFile = datadir + '/comercios.json' # Comercios, sucursales, puntos de venta
    categoriesFile = datadir + '/categorias.json' # Categorías de artículos
    articlesFile = datadir + '/productos.json' # Artículos

    # progress info (for resume support)
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

    # No hay comercios descargados
    if (not progress['comercios']):
        # Obtiene comercios
        comercios = getSucursales(stemUrl)
        # Graba en .json
        with open(POSFile, 'w') as outfile:
            ujson.dump(comercios, outfile)

        progress['comercios'] = True
        with open(configFile, 'w') as outfile:
            ujson.dump(progress, outfile)

    # No hay categorías descargadas
    if (not progress['categorias']):
        # Obtiene categorías
        categorias = getCategorias(stemUrl)
        # Guarda en .json
        with open(categoriesFile, 'w') as outfile:
            ujson.dump(categorias, outfile)

        progress['categorias'] = True
        with open(configFile, 'w') as outfile:
            ujson.dump(progress, outfile)

    # Lee comercios
    comercios = ujson.loads(open(posFile).read())

    if (progress['last_commerce'] > -1):
        # Hay comercios descargados previamente
        with open(articlesFile, mode='r') as inputFile:
            articles = ujson.load(inputFile)
    else:
        articles = []

    # Loop por cada comercio, descargando todos sus artículos
    for x in xrange(progress['last_commerce'] + 1, len(comercios)):
        sucursal = comercios[x]

        articles.append(getProductos(stemUrl, sucursal['id']))

        # save commerce articles in json
        with open(articlesFile, 'w') as outfile:
            ujson.dump(articles, outfile)

        # save progress
        progress['last_commerce'] = x
        with open(configFile, 'w') as outfile:
            ujson.dump(progress, outfile)

    # save progress
    progress['comercios'] = True
    with open(configFile, 'w') as outfile:
        ujson.dump(progress, outfile)

    print 'Data dump succesfully ended.'
    pass


# Obtiene comercios ("sucursales")
def getSucursales(stemUrl):
    sucursales = []
    mainUrl = stemUrl + 'sucursales'

    print 'Recolectando información sobre sucursales...'
    data = getJsonData(mainUrl)

    cantSucursales = data['total']
    maxLimit = data['maxLimitPermitido']
    cantPages = int(math.ceil(cantSucursales / maxLimit))

    urls = []
    for x in xrange(1, cantPages + 1):
        print ('Descargando sucursales (pág ' + str(x) + ' de ' + str(cantPages) + ')...')
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
def getProductos(stemUrl, idComercio):
    # default seconds to launch timeout exception
    timeoutSecs = 20

    productos = []
    mainUrl = stemUrl + 'productos' + '?id_sucursal=' + idComercio

    print 'Recolectando información sobre productos del comercio ' + str(idComercio) + '...'
    data = getJsonData(mainUrl)

    if 'errorMessage' in data:
        # Algo falló en el servidor.
        raise Exception('Server Error in productos: ' + data['errorMessage'])

    cantProductos = data['total']
    maxLimit = data['maxLimitPermitido']
    cantPages = int(math.ceil(cantProductos / maxLimit))

    print ('Descargando ' + str(cantProductos) + ' productos de comercio ' + str(idComercio) + '...')
    urls = []
    for x in xrange(1, cantPages + 1):
        urls.append(mainUrl + \
            '&offset=' + str((x - 1) * maxLimit) + \
            '&limit=' + str(maxLimit))

    rs = (grequests.get(u, stream = False, timeout = timeoutSecs) for u in urls)
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
        'id_sucursal': idComercio,
        'productos': productos
    }


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

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
import urllib2
import json
import math
import os.path

def main():
    # global variables (wrapper class, maybe?)
    stemUrl = 'https://8kdx6rx8h4.execute-api.us-east-1.amazonaws.com/prod/'
    datadir = 'data'

    # progress info dict (for resume support)
    filename = datadir + '/_progress.json'
    if (os.path.isfile(filename)):
        with open(filename, mode='r') as progressFile:
            progress = json.load(progressFile)
    else:
        progress = {
            'last_commerce': -1,
            'comercios': False,
            'categorias': False,
            'productos': False
        }

    # Feature: si los archivos existen, levantarlos. Si no, descargarlos.
    if (progress['comercios']):
        comercios = getSucursales(stemUrl)
        with open(datadir + '/comercios.json', 'w') as outfile:
            json.dump(comercios, outfile)

        progress['comercios'] = True
        with open(datadir + '/_progress.json', 'w') as outfile:
            json.dump(progress, outfile)

    if (not progress['categorias']):
        categorias = getCategorias(stemUrl)
        with open(datadir + '/categorias.json', 'w') as outfile:
            json.dump(categorias, outfile)

        progress['categorias'] = True
        with open(datadir + '/_progress.json', 'w') as outfile:
            json.dump(progress, outfile)

    comercios = json.loads(open(datadir + '/comercios.json').read())

    filename = datadir + '/productos.json'

    if (progress['last_commerce'] > -1):
        with open(filename, mode='r') as articlesFile:
            articles = json.load(articlesFile)
    else:
        articles = []

    for x in xrange(progress['last_commerce'] + 1, len(comercios)):
        sucursal = comercios[x]

        articles.append(getProductos(stemUrl, sucursal['id']))

        # save commerce articles
        with open(filename, 'w') as outfile:
            json.dump(articles, outfile)

        # save progress
        progress['last_commerce'] = x
        with open(datadir + '/_progress.json', 'w') as outfile:
            json.dump(progress, outfile)

    progress['comercios'] = True
    with open(datadir + '/_progress.json', 'w') as outfile:
        json.dump(progress, outfile)

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

    for x in xrange(1, cantPages + 1):
        print ('Descargando sucursales (pág ' + str(x) + ' de ' + str(cantPages) + ')...')
        url = mainUrl + '?offset=' + str((x - 1) * maxLimit) + '&limit=' + str(maxLimit)
        data = getJsonData(url)
        sucursales = sucursales + data['sucursales']
        pass

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
    productos = []
    mainUrl = stemUrl + 'productos' + '?id_sucursal=' + idComercio

    print 'Recolectando información sobre productos del comercio ' + str(idComercio) + '...'
    data = getJsonData(mainUrl)

    cantProductos = data['total']
    maxLimit = data['maxLimitPermitido']
    cantPages = int(math.ceil(cantProductos / maxLimit))

    for x in xrange(1, cantPages + 1):
        print ('Descargando productos de comercio ' + str(idComercio) + ' (pág ' + str(x) + ' de ' + str(cantPages) + ')...')
        url = mainUrl + \
            '&offset=' + str((x - 1) * maxLimit) + \
            '&limit=' + str(maxLimit)

        data = getJsonData(url)
        productos = productos + data['productos']

    return {
        'id_sucursal': idComercio,
        'productos': productos
    }


# Get json data from url.
#   Returns dict
def getJsonData(url):
    req = urllib2.Request(url)
    opener = urllib2.build_opener()
    f = opener.open(req)
    data = json.loads(f.read())

    # chequear estado = 200, si no reintentar x veces.
    # Si después de x veces sigue sin funcionar devolver error.

    return data

if __name__ == "__main__":
    main()

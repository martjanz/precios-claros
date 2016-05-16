# -*- coding: utf-8 -*-
import json
import numpy as np

# Cantidad de sucursales (2266) dividido cantidad de elementos por página (50): 46
maxPages = 46
sucursales = []

for i in xrange(0, maxPages):
	page = json.loads(open("sucursales/suc-" + str(i) + ".json").read())
	sucursales = np.concatenate((sucursales, page['sucursales']))

sucursales = np.array(sucursales).tolist()

with open('sucursales.json', 'w') as outfile:
	json.dump(sucursales, outfile)

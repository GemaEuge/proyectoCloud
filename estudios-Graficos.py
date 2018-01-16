#!/usr/bin/python
# -*- coding: utf-8 -*-

#Universidad Complutense de Madrid
#Cloud & Big Data

#Proyecto grupo G
#Integrantes:
# -Julia Fernández Reyes
# -Elliani Agüero Selva
# -Juan Mas Aguilar
# -Gema Eugercios Suárez
# -Lorenzo José de la Paz Suárez

import sys
import re


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Estadistica
import pandas as pd
import matplotlib.pyplot as plt

sc = SparkContext("local", "EstudioTendencias")

spark = SparkSession.builder \
          .appName("Estudio tendencias terroristas") \
          .config("spark.some.config.option", "some-value") \
          .getOrCreate()

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("globalterrorismdb_0617dist.csv") 

def estudioNumeroDeAtaquesPorYear():
    #Agrupamos por año y mostramos el número de ataques
    resultadosDF = df.groupBy("iyear").count().orderBy(['iyear']).withColumnRenamed('iyear','year').withColumnRenamed('count','numero de ataques')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()

    grafica = resultadosPanda.plot(x='year',y='numero de ataques', title="Evolucion del numero de ataques terroristas a escala global", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/numeroDeAtaquesPorAño.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioNumeroDeAtaquesPorAño.csv')

def estudioPaisesMasAtacados():
    #Agrupamos por país y mostramos el número de ataques. Los 20 más atacados
    resultadosDF = df.groupBy("country_txt").count().orderBy(['count'],ascending=False).limit(20).withColumnRenamed('country_txt', 'country').withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()

    grafica = resultadosPanda.plot.bar(x='country', y = 'numero de ataques', title="Los 20 paises mas atacados", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/20paisesMasAtacados.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioPaisesMasAtacados.csv')

def estudioPaisesMenosAtacados():
    #Agrupamos por país y mostramos el número de ataques. Los 20 menos atacados
    resultadosDF = df.groupBy("country_txt").count().orderBy(['count'],ascending=True).limit(20).withColumnRenamed('country_txt', 'country').withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()

    grafica = resultadosPanda.plot.bar(x='country', y = 'numero de ataques', title="Los 20 paises menos atacados", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/20paisesMenosAtacados.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioPaisesMenosAtacados.csv')

def estudioCiudadesMasAtacadas():
    #Agrupamos por ciudad y mostramos el número de ataques. Los 20 más atacados
    resultadosDF = df.groupBy("city").count().orderBy(['count'],ascending=False).limit(20).withColumnRenamed('city', 'icity').withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='icity', y = 'numero de ataques', title="Las 20 ciudades mas atacadas", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/20ciudadesMasAtacadas.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioCiudadesMasAtacadas.csv')

def estudioCiudadesMenosAtacadas():
    #Agrupamos por ciudad y mostramos el número de ataques. Los 20 menos atacados
    resultadosDF = df.groupBy("city").count().orderBy(['count'],ascending=True).limit(20).withColumnRenamed('city', 'icity').withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='icity', y = 'numero de ataques', title="Las 20 ciudades menos atacadas", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/20ciudadesMenosAtacadas.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioCiudadesMenosAtacadas.csv')

def estudioNumeroAtaquesPais(pais):
    #Seleccionamos el país y vemos su número de ataques
    resultadosDF = df.where("country_txt = '"+pais+"'").groupBy("iyear", "country_txt").count().withColumnRenamed('country_txt', 'country').withColumnRenamed('count', 'numero de ataques').sort('iyear')
    resultadosDF.show()
    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot(x='iyear', y='numero de ataques', title="Evolucion del numero de ataques en " + pais + " (country)", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioPaisesAtacados.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioNumeroAtaquesPais.csv')

def estudioNumeroAtaquesCiudad(ciudad):
    
    #Seleccionamos la ciudad y vemos la evolucion por año de su número de ataques
    resultadosDF = df.where("city = '"+ciudad+"'").groupBy("iyear").count().withColumnRenamed('iyear','year').withColumnRenamed('count', 'numero de ataques').sort('year')
    resultadosDF.show()

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot(x='year', y='numero de ataques', title="Evolucion del numero de ataques de " + ciudad + " (icity)", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioCiudadesAtacadas.png', bbox_inches='tight')
    #resultadosDF.write.csv('EstudioNumeroAtaquesCiudad.csv')


def estudioCiudadesMasAtacadasPorDecada(num_ciudades, decada):
    #Dada una decada, seleccionamos los x países más atacados
    last_year = decada + 9
    df_with_decade = df.withColumn("decade", lit(decada))
    resultadosDF = df_with_decade.where("iyear between '" + str(decada) + "' AND '" + str(last_year) + "'"     ).groupBy("decade", "city").count().orderBy(['count'],ascending=False).limit(num_ciudades).withColumnRenamed('count', "numero de ataques").withColumnRenamed('city', 'icity')
    return resultadosDF
#resultadosDF.write.csv('ciudadesMasAtacadasPorDecada.csv')

def estudioCiudadesMasAtacadasPorDecadas():
    #Representamos las 5 ciudades más atacadas por décadas
    decadas = [1970,1980,1990,2000,2010]
    fig = plt.figure(figsize=(20,20))
    axes = []

    i = 0
    for j in range(1, 6):
	axes.append(fig.add_subplot(2,3,j))

    for decada in decadas:
        resultadosDF = estudioCiudadesMasAtacadasPorDecada(5, decada);
        resultadosDF.show()
	panda = resultadosDF.toPandas()
        #resultadosDF.write.csv('EstudioCiudadesMasAtacadasPorDecadas.csv', mode = "append")  
	grafica = panda.plot.bar(ax=axes[i], x='icity', y='numero de ataques', title="Ciudades mas atacadas por decada (" + str(decada) +")")
        i = i + 1 

    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioCiudadesAtacadasPorDecadas.png', bbox_inches='tight')

def estudioPaisesMasAtacadosPorDecada(num_paises, decada):
    #Dada una decada, seleccionamos los x países más atacados
    last_year = decada + 9
    df_with_decade = df.withColumn("decade", lit(decada))
    resultadosDF = df_with_decade.where("iyear between '" + str(decada) + "' AND '" + str(last_year) + "'" ).groupBy("decade", "country_txt").count().orderBy(['count'],ascending=False).limit(num_paises).withColumnRenamed('country_txt', 'country').withColumnRenamed('count', 'numero de ataques')
    #resultadosDF.write.csv('paisesMasAtacadosPorDecada.csv')
    return resultadosDF
    
def estudioPaisesMasAtacadosPorDecadas():
    #Representamos los 5 países más atacados por décadas
    decadas = [1970,1980,1990,2000,2010]
    
    fig = plt.figure(figsize=(20,20))
    axes = []

    i = 0
    for j in range(1, 6):
	axes.append(fig.add_subplot(2,3,j))

    for decada in decadas:
        resultadosDF = estudioPaisesMasAtacadosPorDecada(5, decada)
        resultadosDF.show()
        panda = resultadosDF.toPandas()
        #resultadosDF.write.csv('EstudioCiudadesMasAtacadasPorDecadas.csv', mode = "append")  
	grafica = panda.plot.bar(ax=axes[i], x='country', y='numero de ataques', title="Paises mas atacados por decada (" + str(decada) +")")
        i = i + 1 

    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioPaisesAtacadosPorDecadas.png', bbox_inches='tight')

def estudioTipoDeArmaMasComun():
    resultadosDF = df.groupBy("weaptype1_txt").count().orderBy(['count'],ascending=False).limit(20).withColumnRenamed('weaptype1_txt', 'weapon').withColumnRenamed('count', 'numero de usos')
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioTipoDeArmaMasComun.csv')
    
    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='weapon', y='numero de usos', title="Tipos de arma mas utilizados", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioArmasMasComunes.png', bbox_inches='tight')

def estudioTipoDeArmaMasComunPorGrupo():
    resultadosSinFiltrarDF = df.groupBy("gname","weaptype1_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF = resultadosSinFiltrarDF.dropDuplicates(["gname"]).withColumnRenamed('weaptype1_txt', 'weapon')
    #resultadosDF.write.csv('EstudioTipoDeArmaMasComunPorGrupo.csv')

    resultadosPanda = resultadosDF.toPandas()
    #no se que tipo de grafica debería venir aqui
    grafica = resultadosPanda.plot.bar(x='weapon', y='gname', title="Tipos de arma mas utilizados")
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/EstudioArmasMasComunesPorGrupo.png', bbox_inches='tight')

def estudioGruposTerroristasMasAgresivos():
    resultadosDF = df.groupBy("gname").count().orderBy(['count'],ascending=False).limit(20). withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioGruposTerroristasMasAgresivos.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='gname', y='numero de ataques', title="Grupos Terroristas Mas Agresivos", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/GruposMasAgresivos.png', bbox_inches='tight')

def estudioGruposTerroristasPorPais(pais):
    resultadosDF = df.where("country_txt = '" + pais + "'").groupBy("country_txt","gname").count().orderBy(['count'],ascending=False).limit(20).withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioGruposTerroristasPorPais.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='gname', y='numero de ataques', title="Grupos Terroristas Mas Agresivos en " + pais, figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/GruposMasAgresivosPorPais.png', bbox_inches='tight')

def estudioGruposTerroristasPorCiudad(ciudad):
    resultadosDF = df.where("city = '" + ciudad + "'").groupBy("city","gname").count().orderBy(['count'],ascending=False).limit(50).withColumnRenamed('count', 'numero de ataques')
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioGruposTerroristasPorCiudad.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='gname', y='numero de ataques', title="Grupos Terroristas Mas Agresivos en " + ciudad, figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/GruposMasAgresivosPorCiudad.png', bbox_inches='tight')

def estudioVictimasPorPais(pais):
    resultadosDF = df.where("country_txt = '" + pais + "'").groupBy("iyear","country_txt").agg({'nkill': 'sum'}).sort('iyear')
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioVictimasPorPais.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot(x='iyear', y='sum(nkill)', title="Evolucion del numero de victimas en " + pais, figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/VictimasPorPais.png', bbox_inches='tight')

def estudioVictimas():
    resultadosDFA = df.groupBy("iyear","country_txt").agg({'nkill': 'sum'}).orderBy(['iyear']).withColumnRenamed('iyear', 'year').withColumnRenamed('sum(nkill)', 'numero_de_victimas')
    resultadosDF = resultadosDFA.where(col('numero_de_victimas').isNotNull())
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioVictimasAtaquesTerroristas.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot(x='year', y='numero_de_victimas', title="Evolucion del numero de victimas a escala global", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/VictimasDeAtaquesTerroristas.png', bbox_inches='tight')

def estudioObjetivoMasComun():
    resultadosDF = df.groupBy("targtype1_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    #resultadosDF.write.csv('EstudioObjetivoMasComun.csv')

    resultadosPanda = resultadosDF.toPandas()
    grafica = resultadosPanda.plot.bar(x='targtype1_txt', y='count', title="Objetivos mas comunes", figsize=(10,10))
    grafica = grafica.get_figure()
    grafica.savefig('Graficas/ObjetivosMasComunes.png', bbox_inches='tight')



#Por argumento le pasamos qué estudio quiere obtener
def main():
    if len(sys.argv) < 2:
	print "Debe introducir un parámetro de los siguientes: (NumeroAtaquesAño,paisesMasAtacados,paisesMenosAtacados,ciudadesMasAtacadas,ciudadesMenosAtacadas,numeroAtaquesPais [Spain],numeroAtaquesCiudad [Madrid], paisesMasAtacadosPorDecada [1980, 15], paisesMasAtacadosPorDecadas, tipoDeArmaMasComun, gruposTerroristasMasAgresivos, victimas)"
    else:
	if sys.argv[1] == "numeroAtaquesAño":
		estudioNumeroDeAtaquesPorYear()

	elif sys.argv[1] == "paisesMasAtacados":
		estudioPaisesMasAtacados()

	elif sys.argv[1] == "paisesMenosAtacados":
		estudioPaisesMenosAtacados()

	elif sys.argv[1] == "ciudadesMasAtacadas":
		estudioCiudadesMasAtacadas()

	elif sys.argv[1] == "ciudadesMenosAtacadas":
		estudioCiudadesMenosAtacadas()

	elif sys.argv[1] == "numeroAtaquesPais":
		try:
		     	sys.argv[2]
		except IndexError:
		     	print "No ha introducido el país que quiere analizar"
		else:
			estudioNumeroAtaquesPais(sys.argv[2])

	elif sys.argv[1] == "numeroAtaquesCiudad":
		try:
		     	sys.argv[2]
		except IndexError:
		     	print "No ha introducido la ciudad que quiere analizar"
		else:
			estudioNumeroAtaquesCiudad(sys.argv[2])
	elif sys.argv[1] == "paisesMasAtacadosPorDecadas":
		estudioPaisesMasAtacadosPorDecadas()
        elif sys.argv[1] == "paisesMasAtacadosPorDecada":
		try:
			sys.argv[2]
			sys.argv[3]
		except IndexError:
			print "No ha introducido la década o el número de países"
		else:
			estudioPaisesMasAtacadosPorDecada(int(sys.argv[3]), int(sys.argv[2]))
			
        elif sys.argv[1] == "tipoDeArmaMasComun":
                estudioTipoDeArmaMasComun()
	elif sys.argv[1] == "gruposTerroristasMasAgresivos":
                estudioGruposTerroristasMasAgresivos()
	elif sys.argv[1] == "ciudadesMasAtacadasPorDecada":
		try:
			sys.argv[2]
			sys.argv[3]
		except IndexError:
			print "No ha introducido la década o el número de ciudades"
		else:
			estudioCiudadesMasAtacadasPorDecada(int(sys.argv[3]), int(sys.argv[2]))
			
	elif sys.argv[1] == "ciudadesMasAtacadasPorDecadas":
		estudioCiudadesMasAtacadasPorDecadas()
	elif sys.argv[1] == "tipoDeArmaMasComunPorGrupo":
		estudioTipoDeArmaMasComunPorGrupo()
	elif sys.argv[1] == "gruposTerroristasPorPais":
		try:
		     	sys.argv[2]
		except IndexError:
		     	print "No ha introducido el país que quiere analizar"
		else:
			estudioGruposTerroristasPorPais(sys.argv[2])
	elif sys.argv[1] == "gruposTerroristasPorCiudad":
		try:
		     	sys.argv[2]
		except IndexError:
		     	print "No ha introducido la ciudad que quiere analizar"
		else:
			estudioGruposTerroristasPorCiudad(sys.argv[2])
	elif sys.argv[1] == "victimas":
		estudioVictimas()

	elif sys.argv[1] == "victimasPorPais":
		try:
		     	sys.argv[2]
		except IndexError:
		     	print "No ha introducido el pais que quiere analizar"
		else:
			estudioVictimasPorPais(sys.argv[2])
	elif sys.argv[1] == "objetivoMasComun":
		estudioObjetivoMasComun()
	elif sys.argv[1] == "estudioGeneral":
		estudioNumeroDeAtaquesPorYear()
		estudioPaisesMasAtacados()
		estudioPaisesMenosAtacados()
		estudioCiudadesMasAtacadas()
		estudioCiudadesMenosAtacadas()
		estudioNumeroAtaquesPais("Spain")
		estudioNumeroAtaquesCiudad("Madrid")
		estudioPaisesMasAtacadosPorDecadas()
                estudioTipoDeArmaMasComun()
		estudioGruposTerroristasMasAgresivos()
                estudioCiudadesMasAtacadasPorDecadas()
		estudioGruposTerroristasPorPais("Spain")
		estudioGruposTerroristasPorCiudad("Madrid")
		estudioVictimas()
		estudioVictimasPorPais("Spain")
		#estudioTipoDeArmaMasComunPorGrupo()
		estudioObjetivoMasComun()
		
		
		
		

if __name__ == "__main__":
    main()


#Ejecutar el archivo en una máquina AWS (o en tu ordenador linux) con todo instalado de la siguiente forma:
# 1-Acceder a la máquina y pasar el presente archivo y el archivo CSV
# 2-Ejecutar ./bin/spark-submit <NombreArchivoPresente.py> //En este caso plantilla.py

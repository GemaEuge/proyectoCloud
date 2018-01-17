#!/usr/bin/python
# -*- coding: utf-8 -*-

#Universidad Complutense de Madrid
#Cloud & Big Data

#Proyecto grupo G
#Integrantes:
# -Julia Fernández Reyes
# -Elianni Agüero Selva
# -Juan Mas Aguilar
# -Gema Eugercios Suárez
# -Lorenzo José de la Paz Suárez

import sys
import re


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sc = SparkContext("local", "EstudioTendencias")

spark = SparkSession.builder \
          .appName("Estudio tendencias terroristas") \
          .config("spark.some.config.option", "some-value") \
          .getOrCreate()

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("globalterrorismdb_0617dist.csv") 

def estudioNumeroDeAtaquesPorYear():
    #Agrupamos por año y mostramos el número de ataques
    resultadosDF = df.groupBy("iyear").count().orderBy(['iyear']).show()
    resultadosDF.show()
	#Se guardan los resultados en un archivo CSV
    resultadosDF.write.csv('EstudioNumeroDeAtaquesPorAño.csv')

def estudioPaisesMasAtacados():
    #Agrupamos por país y mostramos el número de ataques. Los 20 más atacados
    resultadosDF = df.groupBy("country_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('EstudioPaisesMasAtacados.csv')

def estudioPaisesMenosAtacados():
    #Agrupamos por país y mostramos el número de ataques. Los 20 menos atacados
    resultadosDF = df.groupBy("country_txt").count().orderBy(['count'],ascending=True).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('EstudioPaisesMenosAtacados.csv')

def estudioCiudadesMasAtacadas():
    #Agrupamos por ciudad y mostramos el número de ataques. Los 20 más atacados
    resultadosDF = df.groupBy("city").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('EstudioCiudadesMasAtacadas.csv')

def estudioCiudadesMenosAtacadas():
    #Agrupamos por ciudad y mostramos el número de ataques. Los 20 menos atacados
    resultadosDF = df.groupBy("city").count().orderBy(['count'],ascending=True).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('EstudioCiudadesMenosAtacadas.csv')

def estudioNumeroAtaquesPais(pais):
    #Seleccionamos el país y vemos su número de ataques
    resultadosDF = df.where("country_txt = '"+pais+"'").groupBy("country_txt").count()
    resultadosDF.show()
    resultadosDF.write.csv('EstudioNumeroAtaquesPais.csv')

def estudioNumeroAtaquesCiudad(ciudad):
    
    #Seleccionamos la ciudad y vemos su número de ataques
    resultadosDF = df.where("city = '"+ciudad+"'").groupBy("city").count()
    resultadosDF.show()
    resultadosDF.write.csv('EstudioNumeroAtaquesCiudad.csv')


def ciudadesMasAtacadasPorDecada(num_ciudades, decada):
    #Dada una decada, seleccionamos los x países más atacados
    last_year = decada + 9
    df_with_decade = df.withColumn("decade", lit(decada))
    resultadosDF = df_with_decade.where("iyear between '" + str(decada) + "' AND '" + str(last_year) + "'" ).groupBy("decade", "city").count().orderBy(['count'],ascending=False).limit(num_ciudades)
    return resultadosDF

def estudioCiudadesMasAtacadasPorDecadas():
    #Representamos las 5 ciudades más atacadas por décadas
    decadas = [1970,1980,1990,2000,2010]
    for decada in decadas:
        resultadosDF = ciudadesMasAtacadasPorDecada(5, decada);
        resultadosDF.show()
        resultadosDF.write.csv('EstudioCiudadesMasAtacadasPorDecadas.csv', mode = "append")

def paisesMasAtacadosPorDecada(num_paises, decada):
    #Dada una decada, seleccionamos los x países más atacados
    last_year = decada + 9
    df_with_decade = df.withColumn("decade", lit(decada))
    resultadosDF = df_with_decade.where("iyear between '" + str(decada) + "' AND '" + str(last_year) + "'" ).groupBy("decade", "country_txt").count().orderBy(['count'],ascending=False).limit(num_paises)
    return resultadosDF

def estudioPaisesMasAtacadosPorDecadas():
    #Representamos los 5 países más atacados por décadas
    decadas = [1970,1980,1990,2000,2010]
    for decada in decadas:
        resultadosDF = paisesMasAtacadosPorDecada(5, decada)
        resultadosDF.show()
        resultadosDF.write.csv('EstudioPaisesMasAtacadosPorDecadas.csv', mode = "append")

def estudioTipoDeArmaMasComun():
    resultadosDF = df.groupBy("weaptype1_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('EstudioTipoDeArmaMasComun.csv')

def estudioTipoDeArmaMasComunPorGrupo():
    resultadosSinFiltrarDF = df.groupBy("gname","weaptype1_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF = resultadosSinFiltrarDF.dropDuplicates(["gname"])
    resultadosDF.write.csv('EstudioTipoDeArmaMasComunPorGrupo.csv')

def estudioGruposTerroristasMasAgresivos():
    resultadosDF = df.groupBy("gname").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('estudioGruposTerroristasMasAgresivos.csv')

def estudioGruposTerroristasPorPais(pais):
    resultadosDF = df.where("country_txt = '" + pais + "'").groupBy("country_txt","gname").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('estudioGruposTerroristasPorPais.csv')

def estudioGruposTerroristasPorCiudad(ciudad):
    resultadosDF = df.where("city = '" + ciudad + "'").groupBy("city","gname").count().orderBy(['count'],ascending=False).limit(50)
    resultadosDF.show()
    resultadosDF.write.csv('estudioGruposTerroristasPorCiudad.csv')

def estudioVictimas():
    resultadosDF = df.groupBy("iyear","country_txt").agg({'nkill': 'sum'}).orderBy(['iyear']).withColumnRenamed('iyear', 'year').withColumnRenamed('sum(nkill)', 'numero de victimas')
    resultadosDF.show()
    resultadosDF.write.csv('EstudioVictimasAtaquesTerroristas.csv')

def estudioVictimasPorPais(pais):
    #Con esta funcion tenemos un problema. EL csv no está bien formado, hay campos en nkills que son strings cuando no deberían, por eos da error
    resultadosDF = df.where("country_txt = '" + pais + "'").groupBy("country_txt").sum("nkill")
    resultadosDF.show()
    resultadosDF.write.csv('estudioGruposTerroristasPorCiudad.csv')

def estudioObjetivoMasComun():
    resultadosDF = df.groupBy("targettype1_txt").count().orderBy(['count'],ascending=False).limit(20)
    resultadosDF.show()
    resultadosDF.write.csv('estudioObjetivoMasComun.csv')


#Por argumento le pasamos qué estudio quiere obtener
def main():
    if len(sys.argv) < 2:
	print "Debe introducir un parámetro de los siguientes: (NumeroAtaquesAño,paisesMasAtacados,paisesMenosAtacados,ciudadesMasAtacadas,ciudadesMenosAtacadas,numeroAtaquesPais [Spain],numeroAtaquesCiudad [Madrid], paisesMasAtacadosPorDecada [1980, 15], paisesMasAtacadosPorDecadas, tipoDeArmaMasComun, gruposTerroristasMasAgresivos, victimas)"
    else:
	if sys.argv[1] == "NumeroAtaquesAño":
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
		except NameError:
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
			resultadosDF = paisesMasAtacadosPorDecada(int(sys.argv[3]), int(sys.argv[2]))
			resultadosDF.write.csv('paisesMasAtacadosPorDecada.csv')
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
			resultadosDF = ciudadesMasAtacadasPorDecada(int(sys.argv[3]), int(sys.argv[2]))
			resultadosDF.write.csv('ciudadesMasAtacadasPorDecada.csv')
	elif sys.argv[1] == "ciudadesMasAtacadasPorDecadas":
		estudioCiudadesMasAtacadasPorDecadas()
	elif sys.argv[1] == "tipoDeArmaMasComunPorGrupo":
		estudioTipoDeArmaMasComunPorGrupo()
	elif sys.argv[1] == "gruposTerroristasPorPais":
		try:
		     	sys.argv[2]
		except NameError:
		     	print "No ha introducido el país que quiere analizar"
		else:
			estudioGruposTerroristasPorPais(sys.argv[2])
	elif sys.argv[1] == "gruposTerroristasPorCiudad":
		try:
		     	sys.argv[2]
		except NameError:
		     	print "No ha introducido la ciudad que quiere analizar"
		else:
			estudioGruposTerroristasPorCiudad(sys.argv[2])
	elif sys.argv[1] == "victimas":
		estudioVictimas()
	elif sys.argv[1] == "victimasPorPais":
		try:
		     	sys.argv[2]
		except NameError:
		     	print "No ha introducido el pais que quiere analizar"
		else:
			estudioVictimasPorPais(sys.argv[2])
	elif sys.argv[1] == "tipoDeArmaMasComunPorGrupo":
		estudioTipoDeArmaMasComunPorGrupo()
		

if __name__ == "__main__":
    main()


#Ejecutar el archivo en una máquina AWS con todo instalado de la siguiente forma:
# 1-Acceder a la máquina y pasar el presente archivo y el archivo CSV
# 2-Ejecutar ./bin/spark-submit <NombreArchivoPresente.py> //En este caso plantilla.py

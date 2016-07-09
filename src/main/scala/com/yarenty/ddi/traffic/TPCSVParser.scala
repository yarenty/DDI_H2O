package com.yarenty.ddi.traffic

import water.parser.{DefaultParserProviders, ParseSetup}


object TPCSVParser {

    def get: ParseSetup = {

      val parseTraffic: ParseSetup = new ParseSetup()
      val trafficNames: Array[String] = Array(
        "day", "district", "timeslice", "t1","t2", "t3", "t4", "temp","pollution",
        "1", "2", "3", "4", "5", "6", "7", "8", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
        "21", "22", "23", "24", "25"
      )
      val trafficTypes = ParseSetup.strToColumnTypes(Array(
        "int", "int", "int", "int", "int", "int", "int", "double", "double"
        "int", "int", "int", "int", "int", "int", "int", "int", "int",
        "int", "int", "int", "int", "int", "int", "int", "int","int", "int",
      "int", "int", "int", "int", "int"
      ))
      parseTraffic.setColumnNames(trafficNames)
      parseTraffic.setColumnTypes(trafficTypes)
      parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
      parseTraffic.setSeparator(',')
      parseTraffic.setNumberColumns(33)
      parseTraffic.setSingleQuotes(false)
      parseTraffic.setCheckHeader(1)
    }

}

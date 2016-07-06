package com.yarenty.ddi.weather

import com.yarenty.ddi.DataMunging
import water.parser.{DefaultParserProviders, ParseSetup}



object WPCSVParser {

  def get: ParseSetup = {
    //    parseFiles
    //      paths: ["/opt/data/season_1/training_data/weather_data/weather_data_2016-01-01"]
    //      destination_frame: "weather_data_2016_01_01.hex"
    //      parse_type: "CSV"
    //      separator: 9
    //      number_columns: 4
    //      single_quotes: false
    //      column_names: ["time","weather","temperature","pollution"]
    //      column_types: ["String","Numeric","Numeric","Numeric"]
    //      delete_on_done: true
    //      check_header: -1
    //      chunk_size: 4194304
    val parseWeather: ParseSetup = new ParseSetup()
    val weatherNames: Array[String] = Array("Day","Timeslice", "Weather", "Temp", "Pollution")
    val weatherTypes = ParseSetup.strToColumnTypes(Array("int","int", "int", "double", "double"))
    parseWeather.setColumnNames(weatherNames)
    parseWeather.setColumnTypes(weatherTypes)
    parseWeather.setParseType(DefaultParserProviders.CSV_INFO)
    parseWeather.setSeparator(',')
    parseWeather.setNumberColumns(5)
    parseWeather.setSingleQuotes(false)
    parseWeather.setCheckHeader(1)
    return parseWeather
  }

}

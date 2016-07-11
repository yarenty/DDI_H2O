package com.yarenty.ddi.traffic

import water.parser.{DefaultParserProviders, ParseSetup}


object TrafficCSVParser {

  def get: ParseSetup = {
    //    parseFiles
    //      paths: ["/opt/data/season_1/training_data/traffic_data/traffic_data_2016-01-01"]
    //      destination_frame: "traffic_data_2016_01_01.hex"
    //      parse_type: "CSV"
    //      separator: 9
    //      number_columns: 6
    //      single_quotes: false
    //      column_names: ["DistrictHash","Traffic1","Traffic2","Traffic3","Traffic4","Time"]
    //      column_types: ["String","String","String","String","String","String"]
    //      delete_on_done: true
    //      check_header: -1
    //      chunk_size: 7220
    val parseTraffic: ParseSetup = new ParseSetup()
    val trafficNames: Array[String] = Array(
      "DistrictHash", "Traffic1", "Traffic2", "Traffic3", "Traffic4", "Time")
    val trafficTypes = ParseSetup.strToColumnTypes(Array(
      "string", "string", "string", "string", "string", "string"))
    parseTraffic.setColumnNames(trafficNames)
    parseTraffic.setColumnTypes(trafficTypes)
    parseTraffic.setParseType(DefaultParserProviders.CSV_INFO)
    parseTraffic.setSeparator('\t')
    parseTraffic.setNumberColumns(6)
    parseTraffic.setSingleQuotes(false)
    parseTraffic.setCheckHeader(-1)
  }

}

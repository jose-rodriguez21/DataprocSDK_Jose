package com.bbva.datioamproduct.fdevdatio.fields

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait Field {

  val name: String
  lazy val comlum: Column = col(name)

}

package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.fields.FoodList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, forall, lit, when}

package object transformations {
  implicit class DFTransformations(df: DataFrame) {

    @throws[Exception]
    def addColumn(newColumn: Column): DataFrame = {
      try {
        val columns: Array[Column] = df.columns.map(col) :+ newColumn
        df.select(columns: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }

    def spicyfilter(): DataFrame = {
      val levelsNotList: List[Int] = List(1,2,3,5,6,7,8,9)
      df.filter(!col("SPICY_LEVEL").isin(levelsNotList:_*))
    }

    def listOfFood(): String = {
      val rddFoodDFSpicy: RDD[(String, String, String)] = df
        .rdd
        .map(record => (record.getString(0), record.getString(1), record.getString(4)))

      val secuenciaRDD: RDD[String] = rddFoodDFSpicy
        .map(record => {
          if (record._3 == "N")
            record._1 + ":" + record._2 + ","
          else
            "(*),"
        })

      val secuenciaString: String = secuenciaRDD.reduce((a,b) => a + "" + b)

      secuenciaString
    }

    def justWomen(): DataFrame = {
      val rowNumberList: List[Int] = List(1,3,4,5)
      val spicyToleranceList: List[String] = List("4","5","7","8","10")
      df
        .filter(col("ROW_NUMBER").isin(rowNumberList:_*))
        .filter(col("SPICY_TOLERANCE").isin(spicyToleranceList:_*))
    }

    def concatFood(cadena: String): DataFrame = {
      df.select(
        df.columns.map {
          case name: String if name == "FOOD_LIST" => lit(cadena).alias(name)
          case _@name => col(name)
        }: _*
      )
    }

  }

}

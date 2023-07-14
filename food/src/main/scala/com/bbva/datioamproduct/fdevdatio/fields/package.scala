package com.bbva.datioamproduct.fdevdatio

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{dense_rank, lit, max, min, rank, row_number}

package object fields {

  case object Nationality extends Field {
    override val name: String = "NATIONALITY"
  }

  case object SpicyTolerance extends Field {
    override val name: String = "SPICY_TOLERANCE"
  }

  case object RowNumber extends Field {
    override val name: String = "ROW_NUMBER"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(Nationality.comlum).orderBy(SpicyTolerance.comlum)
      row_number().over(w).alias(name)
    }
  }

  case object FoodList extends Field {
    override val name: String = "FOOD_LIST"

    def apply(): Column = lit("nuevo valor").alias(name)
  }

}

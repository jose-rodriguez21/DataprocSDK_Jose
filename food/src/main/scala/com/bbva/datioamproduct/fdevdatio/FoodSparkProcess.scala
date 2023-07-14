package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.ConfigConstantsFood.{KditFoodTag, KditFoodieTag}
import com.bbva.datioamproduct.fdevdatio.fields.{FoodList, RowNumber}
import com.bbva.datioamproduct.fdevdatio.utils.{FoodSuperConfig, IOUtils}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import com.bbva.datioamproduct.fdevdatio.transformations.DFTransformations
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class FoodSparkProcess extends SparkProcess with IOUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      val config: Config = runtimeContext.getConfig
      val dataFrameReader: Map[String, DataFrame] = config.readInputsMap

      val foodDF: DataFrame = dataFrameReader(KditFoodTag)
      val foodieDF: DataFrame = dataFrameReader(KditFoodieTag)

      val foodDFSpicy: DataFrame = foodDF.spicyfilter()
      foodDFSpicy.show()

      val cadena: String = foodDFSpicy.listOfFood()
      println(cadena)

      val foodieDFWomen: DataFrame = foodieDF
        .filter(col("GENRE").equalTo("FEMALE"))
        .addColumn(RowNumber())
        .justWomen()

      foodieDFWomen.show()

      val foodieDFAddFoodList: DataFrame = foodieDFWomen
        .addColumn(FoodList())
        .concatFood(cadena)

      foodieDFAddFoodList.show()


    } match {

      case Success(_) => 0
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        -1
    }
  }

  override def getProcessId: String = "FoodSparkProcess"
}

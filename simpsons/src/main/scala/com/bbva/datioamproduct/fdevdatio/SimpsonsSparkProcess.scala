package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.ConfigConstantsFood.FdevSimpsons
import com.bbva.datioamproduct.fdevdatio.utils.{FoodSuperConfig, IOUtils}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

class SimpsonsSparkProcess extends SparkProcess with IOUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      val config: Config = runtimeContext.getConfig
      val dataFrameReader: Map[String, DataFrame] = config.readInputsMap

      val simpsonsDF: DataFrame = dataFrameReader(FdevSimpsons)
      simpsonsDF.show()

    } match {
      case Success(_) => 0
      case Failure(exception: Exception) =>
        exception.printStackTrace()
        -1
    }
  }
  override def getProcessId: String = "SimpsonsSparkProcess"
}

package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.ConfigConstantsFood.InputTag
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {

  implicit class FoodSuperConfig(config: Config) extends IOUtils {

    def readInputsMap: Map[String, DataFrame] = {
      config
        .getObject(InputTag)
        .keySet()
        .map(key => {
          val inputConfig: Config = config.getConfig(s"$InputTag.$key")
          (key, read(inputConfig))
        }).toMap
    }

  }

}

package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.datio.dataproc.sdk.launcher.SparkLauncher

class FoodLauncherTest extends ContextProvider {

  "FoodLauncher execute" should "return 0 in success execution" in {

    val args: Array[String] = Array(
      "src/test/resources/config/FoodJobTest.conf", "FoodSparkProcess"
    )
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 0
  }

}

package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class FoodSparkProcessTest extends ContextProvider {

  "when I execute runProcess method with a correct RuntimeContext" should "return 0" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(config)
    val courseSparkProcess: FoodSparkProcess = new FoodSparkProcess

    val exitCode: Int = courseSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe 0
  }

}

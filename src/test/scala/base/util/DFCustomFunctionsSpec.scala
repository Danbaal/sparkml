package base.util

import base.SharedSparkTestBase

class DFCustomFunctionsSpec extends SharedSparkTestBase {

  test("test") {
    val df = sqlc.createDataFrame(Seq(("A","1"),("B","2"))).toDF("col1", "col2")
    df.show()
  }

}

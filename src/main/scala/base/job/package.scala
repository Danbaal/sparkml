package base

package object job {

  trait AppLogic {
    def name: String
    def run(): Unit

    def args = _args
    private var _args: Array[String] = _
    def setArgs(args: Array[String]) = _args = args
  }

}

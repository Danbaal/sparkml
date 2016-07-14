package base.job

import scala.reflect.ClassTag

/**
  * Alternative approach for implementing jobs
 */
class Job[T<: AppLogic](implicit ct: ClassTag[T]) {

  val job: T = ct.runtimeClass.newInstance().asInstanceOf[T]

  final def main(args: Array[String]): Unit = {
    job.setArgs(args)

    run(job)

  }

  def run(job: T): Unit = {

  }

}

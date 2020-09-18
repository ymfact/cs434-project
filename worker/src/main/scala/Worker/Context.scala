package Worker

import java.io.File

class Context(val dir: File, val workerIndex: Int, val partitionCount: Int, val partitionSize: Int, val isBinary: Boolean) {

  private def util = new Util(this)

  val workerDir = new File(dir, s"$workerIndex")

  def clean(): Unit = util.clean()

  def gensort(): Unit = util.gensort()
}

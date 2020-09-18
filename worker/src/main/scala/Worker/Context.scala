package Worker

import java.io.File

import Worker.Types.WorkerIndexType

class Context(dir: File, val workerIndex: WorkerIndexType, partitionCount: Int, partitionSize: Int, isBinary: Boolean) {

  private val util = new Util(dir, workerIndex, partitionCount, partitionSize, isBinary)

  def workerDir: File = util.workerDir

  def clean(): Unit = util.clean()

  def gensort(): Unit = util.gensort()
}

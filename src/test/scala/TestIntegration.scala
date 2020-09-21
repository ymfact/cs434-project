import java.io.File

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.parallel.CollectionConverters.seqIsParallelizable

@RunWith(classOf[JUnitRunner])
class TestIntegration extends AnyFunSuite {

  trait Launch {
    val FILE_DIR = new File("D:\\works\\csed434\\project_temp\\")
    val WORKER_COUNT = 4
    val PARTITION_COUNT = 2
    val PARTITION_SIZE = 0x10000
    val SAMPLE_COUNT = 1000
    val IS_BINARY = false

    val workerContexts =
      (0 until WORKER_COUNT)
        .map( workerIndex =>
          new Worker.Context(
            rootDir = FILE_DIR,
            workerCount = WORKER_COUNT,
            workerIndex = workerIndex,
            partitionCount = PARTITION_COUNT,
            partitionSize = PARTITION_SIZE,
            sampleCount = SAMPLE_COUNT,
            isBinary = IS_BINARY)
        )
    val masterContext =
      new Master.Context(
        rootDir = FILE_DIR,
        workerCount = WORKER_COUNT,
        partitionCount = PARTITION_COUNT,
        partitionSize = PARTITION_SIZE
      )
  }

  test("test") {
    new Launch{
      val workers = workerContexts.map(new Worker(_))
      val master = new Master(masterContext)
    }
  }
}

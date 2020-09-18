import java.io.File

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestIntegration extends AnyFunSuite {

  trait Launch {
    val FILE_DIR = new File("D:\\works\\csed434\\project_temp\\")
    val WORKER_COUNT = 4
    val PARTITION_COUNT = 2
    val PARTITION_SIZE = 0x10000
    val IS_BINARY = false

    val workerContexts = (0 until WORKER_COUNT).map(new Worker.Context(FILE_DIR, _, PARTITION_COUNT, PARTITION_SIZE, IS_BINARY))
    val masterContext = new Master.Context(FILE_DIR, WORKER_COUNT, PARTITION_COUNT, PARTITION_SIZE)
  }

  test("test") {
    new Launch{
      val workers = workerContexts.map(new Worker(_))
      val master = new Master(masterContext)
      workers.foreach{_.close()}
    }
  }
}

import java.io.File

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Test extends AnyFunSuite {

  trait Launch {
    val FILE_DIR = new File("D:\\works\\csed434\\project_temp\\")
    val WORKER_COUNT = 4
    val workerContexts = (0 until WORKER_COUNT).map(new Worker.Context(FILE_DIR, _))
    val workers = workerContexts.map(new Worker(_))
    val masterContext = new Master.Context(FILE_DIR, WORKER_COUNT)
    val master = new Master(masterContext)
  }

  test("test") {
    new Launch{
      Thread.sleep(1 * 1000)
      workers.foreach{_.close()}
      //assert(Hello.hello("test") === "Hello, test!")
    }
  }
}

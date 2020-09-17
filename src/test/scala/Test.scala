import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Test extends AnyFunSuite {

  trait Launch {
    val WORKER_COUNT = 4
    val workers = (0 until WORKER_COUNT).map(new Worker(_))
    val master = new Master(WORKER_COUNT)
  }

  test("test") {
    new Launch{
      Thread.sleep(1 * 1000)
      workers.foreach{_.close()}
      //assert(Hello.hello("test") === "Hello, test!")
    }
  }
}

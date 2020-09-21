import java.io.File

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.io.Source
import scala.util.Using

@RunWith(classOf[JUnitRunner])
class TestGensort extends AnyFunSuite {

  trait Env {
    val FILE_DIR = new File("D:\\works\\csed434\\project_temp\\")
    val WORKER_INDEX = 1
    val WORKER_COUNT = 4
    val PARTITION_COUNT = 3
    val PARTITION_SIZE = 0x10000
    val IS_BINARY = false

    val ctx =
      new Worker.Context(
        rootDir = FILE_DIR,
        workerCount = WORKER_COUNT,
        workerIndex = WORKER_INDEX,
        partitionCount = PARTITION_COUNT,
        partitionSize = PARTITION_SIZE,
        isBinary = IS_BINARY
      )
  }

  test("clean") {
    new Env {
      ctx.workerDir.mkdirs()
      ctx.clean
      assert(!ctx.workerDir.exists())
    }
  }

  test("gensort") {
    new Env {
      ctx.clean
      ctx.gensort
      assert(new File(ctx.workerDir, "0").exists())

      val file = new File(ctx.workerDir, s"${PARTITION_COUNT - 1}")
      assert(file.exists())

      Using(Source.fromFile(file)){ source =>
        val line = source.getLines().next()
        assert(line.substring(12, 44).toInt === 50000)
      }
    }
  }
}

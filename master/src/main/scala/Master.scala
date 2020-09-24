import Common.Util.unitToEmpty
import Master.Context
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging
import protocall.DataForClassify

class Master(ctx: Context) extends Logging {

  def thisDest(): String = ctx.thisDest()

  ctx.awaitWorkers {

    val samples: Seq[ByteString] = ctx.broadcast {
      logger.info(s"Sending sample request...")
      _.sample()
    }.map(result => {
      logger.info(s"Received sample result.")
      result.bytes
    })

    samples.foreach(sample => logger.info(s"sample received: ${sample.size}"))

    val sampleRanges: Seq[ByteString] = ctx.processSample(samples)

    logger.info(s"sample ranges: ${sampleRanges.size}")

    ctx.broadcast {
      _.classify(DataForClassify(sampleRanges, ctx.workerDests))
    }

    logger.info(s"start final sort...")

    ctx.broadcast {
      _.finalSort()
    }

    logger.info(s"finished.")
  }
}
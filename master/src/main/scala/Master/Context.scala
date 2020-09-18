package Master

import java.io.File

import org.apache.logging.log4j.scala.Logging

class Context(dir:File, val workerCount: Int, partitionCount: Int, partitionSize: Int) extends Logging {

}

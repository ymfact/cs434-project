package Worker

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "worker") {
  var dir            = opt[File](abbrev = "d", description = "directory. default:.", default=new File("."))
  var workerCount    = opt[Int] (abbrev = "w", description = "worker count. default:1", default=1)
  var workerIndex    = opt[Int] (abbrev = "i", description = "worker index. default:0", default=0)
  var partitionCount = opt[Int] (abbrev = "pc", description = "partition count. default:1", default=1)
  var partitionSize  = opt[Int] (abbrev = "ps", description = "partition size. default:100000", default=100000)
  var sampleCount    = opt[Int] (abbrev = "s", description = "sample count per worker. default:1000", default=1000)
  var isBinary       = opt[Boolean](abbrev = "b", description = "binary records. default:false", default=false)
}
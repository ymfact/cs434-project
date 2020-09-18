package Worker

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "worker") {
  var dir            = opt[File](abbrev = "d", description = "directory. default:.", default=new File("."))
  var workerIndex    = opt[Int] (abbrev = "i", description = "worker index. default:0", default=0)
  var partitionCount = opt[Int] (abbrev = "p", description = "partition count. default:1", default=1)
  var partitionSize  = opt[Int] (abbrev = "s", description = "partition size. default:100000", default=100000)
  var isBinary       = opt[Boolean](abbrev = "b", description = "binary records. default:false", default=false)
}
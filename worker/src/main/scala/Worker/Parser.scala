package Worker

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "worker") {
  var dir            = opt[File](name = "dir", description = "directory. default:.", default=new File("."))
  var workerCount    = opt[Int] (name = "wc", description = "worker count. default:1", default=1)
  var workerIndex    = opt[Int] (name = "wi", description = "worker index. default:0", default=0)
  var partitionCount = opt[Int] (name = "pc", description = "partition count. default:1", default=1)
  var partitionSize  = opt[Int] (name = "ps", description = "partition size. default:100000", default=100000)
  var sampleCount    = opt[Int] (name = "sc", description = "sample count per worker. default:1000", default=1000)
  var isBinary       = opt[Boolean](abbrevOnly = "b", description = "binary records. default:false", default=false)
}
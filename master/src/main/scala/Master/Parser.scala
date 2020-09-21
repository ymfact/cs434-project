package Master

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "master") {
  var dir            = opt[File](name = "dir", description = "directory. default:.", default=new File("."))
  var workerCount    = opt[Int] (name = "wc", description = "worker count. default:1", default=1)
  var partitionCount = opt[Int] (name = "pc", description = "partition count. default:1", default=1)
  var partitionSize  = opt[Int] (name = "ps", description = "partition size. default:100000", default=100000)
}
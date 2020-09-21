package Master

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "master") {
  var dir            = opt[File](abbrev = "d", description = "directory. default:.", default=new File("."))
  var workerCount    = opt[Int] (abbrev = "w", description = "worker count. default:1", default=1)
  var partitionCount = opt[Int] (abbrev = "p", description = "partition count. default:1", default=1)
  var partitionSize  = opt[Int] (abbrev = "s", description = "partition size. default:100000", default=100000)
}
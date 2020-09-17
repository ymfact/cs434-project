package Master

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "master") {
  var dir            = opt[File](abbrev = "d", description = "directory", default=new File("."))
  var workerCount    = opt[Int] (abbrev = "i", description = "worker index", default=1)
}
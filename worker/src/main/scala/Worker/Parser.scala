package Worker

import java.io.File

import org.backuity.clist.{Command, opt}

class Parser extends Command(description = "worker") {
  var dir            = opt[File](abbrev = "d", description = "directory", default=new File("."))
  var workerIndex    = opt[Int] (abbrev = "i", description = "worker index", default=0)
}
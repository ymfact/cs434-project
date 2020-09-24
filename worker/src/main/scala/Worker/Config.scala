package Worker

import java.io.File

case class Config
(
  masterDest: String = "",
  in: Seq[File] = Seq(),
  out: File = null,
)
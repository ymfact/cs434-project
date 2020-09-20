package Common

import java.io.{DataInputStream, EOFException}

object RecordStream {
  type RecordStream = LazyList[RecordFromByteArray]
  def from(stream: DataInputStream): RecordStream = {
    LazyList.continually({
      try {
        Some(Record.from(stream))
      } catch {
        case _: EOFException => None
      }
    }).takeWhile(_.isDefined).map(_.get)
  }
}

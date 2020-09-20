package Common

import java.io.InputStream

object RecordStream {
  type RecordStream = LazyList[RecordFromByteArray]
  def from(stream: InputStream): RecordStream = {
    LazyList.continually(Record.from(stream)).takeWhile(_ != null)
  }
}

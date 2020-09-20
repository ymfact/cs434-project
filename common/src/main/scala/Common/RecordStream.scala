package Common

import java.io.{DataInputStream, EOFException}

import com.google.protobuf.ByteString

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

  def recordsToByteString(records: Iterable[RecordFromByteArray]): ByteString =
    records.map(_.toByteArray).map(ByteString.copyFrom).fold(ByteString.EMPTY)(_ concat _)
}

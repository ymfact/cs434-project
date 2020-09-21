package Common

import java.io.{DataInputStream, EOFException}

import com.google.protobuf.ByteString

object RecordStream {
  type RecordStream = LazyList[RecordFromStream]

  def from(stream: DataInputStream): RecordStream = {
    LazyList.continually({
      try Some(Record.from(stream)) catch {
        case _: EOFException => None
      }
    }).takeWhile(_.isDefined).map(_.get)
  }

  def recordsToByteString(records: Iterable[RecordFromStream]): ByteString =
    records.map(_.raw).map(ByteString.copyFrom).fold(ByteString.EMPTY)(_ concat _)
}

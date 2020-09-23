package Common

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import protocall.ProtoCallGrpc
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.mutable

object RPCClient {
  val channels: mutable.Map[Int, ManagedChannel] = mutable.Map()
  def apply(workerIndex: Int): ProtoCallStub = {
    if(!channels.contains(workerIndex)) {
      val port = 65400 + workerIndex
      val builder = ManagedChannelBuilder.forAddress("localhost", port)
      builder.usePlaintext()
      channels.addOne(workerIndex -> builder.build)
    }

    ProtoCallGrpc.stub(channels(workerIndex)).withWaitForReady()
  }
}

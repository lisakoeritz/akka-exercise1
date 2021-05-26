package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.function.Creator;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	private byte[] entireMessage = new byte[0];  //Entire message to be sent

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	private ActorRef receiverWorkerReference;
	private ActorRef masterReference;
	private List<Byte> incomingByteData = new ArrayList<Byte>();

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	private static class MasterOnInitializeMessage implements Serializable { //Master initializes contact with worker
		private ActorRef master;
		private ActorRef masterLargeMessageProxyReference;
		private ActorRef receiverWorkerReference;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	private  static class StreamSyncMessage implements Serializable {
		private ActorRef senderReference;
	}

	@Data
	@NoArgsConstructor
	private static class StreamCompletedMessage implements Serializable{}

	@Data
	@NoArgsConstructor
	private static class StreamInitializedMessage implements Serializable {}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	private static class StreamFailureMessage implements Serializable {
		private Throwable causeOfFailure;
	}


	private class BatchIterator implements Iterator<Byte[]> {

		private int counter = 0;
		private final int batchSize = 121000;

		@Override
		public boolean hasNext() {
			if(entireMessage.length > counter * batchSize){
				return true;
			}
			return false;
		}

		@Override
		public Byte[] next() {
			int batchStart = counter * batchSize;
			int batchEnd = batchStart + batchSize;
			if (batchEnd > entireMessage.length) {
				batchEnd = entireMessage.length;
			}
			counter++;
			Byte[] result = IntStream.range(batchStart, batchEnd).mapToObj(k -> Byte.valueOf(entireMessage[k])).toArray(Byte[]::new);
			return result;
		}
	}
	
	/////////////////
	// Actor State //
	/////////////////

	enum AckMessage {
		INSTANCE
	}
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(MasterOnInitializeMessage.class, this::handle)
				.match(StreamSyncMessage.class, this::handle)
				.match(StreamInitializedMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(Byte[].class, this::handle)
				.match(StreamCompletedMessage.class, this::handle)
				.match(StreamFailureMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}



	private void handle(LargeMessage<?> largeMessage) { // 7 - handle LargeMessage message from master
		Object message = largeMessage.getMessage();
		ActorRef sender = this.sender();
		ActorRef receiver = largeMessage.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// TODO: Implement a protocol that transmits the potentially very large message object.
		// The following code sends the entire message wrapped in a BytesMessage, which will definitely fail in a distributed setting if the message is large!
		// Solution options:
		// a) Split the message into smaller batches of fixed size and send the batches via ...
		//    a.a) self-build send-and-ack protocol (see Master/Worker pull propagation), or
		//    a.b) Akka streaming using the streams build-in backpressure mechanisms.
		// b) Send the entire message via Akka's http client-server component.
		// c) Other ideas ...
		// Hints for splitting:
		// - To split an object, serialize it into a byte array and then send the byte array range-by-range (tip: try "KryoPoolSingleton.get()").
		// - If you serialize a message manually and send it, it will, of course, be serialized again by Akka's message passing subsystem.
		// - But: Good, language-dependent serializers (such as kryo) are aware of byte arrays so that their serialization is very effective w.r.t. serialization time and size of serialized data.

		//Kryo Serialization
		//Official Streaming documentation: https://doc.akka.io/docs/akka/2.6.14/stream/stream-quickstart.html

		this.entireMessage = KryoPoolSingleton.get().toBytesWithClass(message); //Serialization: converting data into bytes and saving to array
		receiverProxy.tell(new BytesMessage<>(this.entireMessage, sender, receiver), this.self());
	}

	private void handle(BytesMessage<byte[]> message) {
		// TODO: With option a): Store the message, ask for the next chunk and, if all chunks are present, reassemble the message's content, deserialize it and pass it to the receiver.
		// The following code assumes that the transmitted bytes are the original message, which they shouldn't be in your proper implementation ;-)
		//message.getReceiver().tell(message.getBytes(), message.getSender());
		message.getReceiver().tell(KryoPoolSingleton.get().fromBytes(message.getBytes()), message.getSender()); //Deserialization: de-converting bytes
	}


	private void handle(MasterOnInitializeMessage masterOnInitializeMessage) {
		this.receiverWorkerReference = masterOnInitializeMessage.getReceiverWorkerReference();
		this.masterReference = masterOnInitializeMessage.getMaster();
		this.log().info("LargeMessageProxy - Initialization from Master" + this.self());
		masterOnInitializeMessage.getMasterLargeMessageProxyReference().tell(new StreamSyncMessage(this.self()), this.self());
	}

	private class BatchCreator implements Creator {
		BatchIterator iterator = new BatchIterator();
		@Override
		public Object create()  {
			return iterator;
		}
	}

	//https://doc.akka.io/docs/akka/current/stream/operators/ActorSource/actorRefWithBackpressure.html
	private void handle(StreamSyncMessage streamSyncMessage) {
		BatchCreator batchCreator = new BatchCreator();
		Sink sink = Sink.actorRefWithBackpressure(
				streamSyncMessage.getSenderReference(),
				new StreamInitializedMessage(),
				AckMessage.INSTANCE,
				new StreamCompletedMessage(),
				error -> new StreamFailureMessage(error)
		);
		Source.fromIterator(batchCreator).runWith(sink, Materializer.createMaterializer(this.context()));
	}

	private void handle(StreamInitializedMessage streamInitializedMessage) {
		sender().tell(AckMessage.INSTANCE, self());
	}

	private void handle(Byte[] bytes) {
		// Rebuilding byte data
		for(Byte b : bytes){
			this.incomingByteData.add(b);
		}
		sender().tell(AckMessage.INSTANCE, self());
	}

	private void handle(StreamCompletedMessage streamCompletedMessage) {

		byte[] bytes = new byte[this.incomingByteData.size()];
		int counter = 0;
		while (counter < this.incomingByteData.size()) {
			bytes[counter] = this.incomingByteData.get(counter);
			counter++;
		}
		this.receiverWorkerReference.tell(KryoPoolSingleton.get().fromBytes(bytes), this.masterReference); //Deserialization (unpacking of bytes)
	}

	private void handle(StreamFailureMessage streamFailureMessage) {
		this.log().error(streamFailureMessage.toString());
	}

}

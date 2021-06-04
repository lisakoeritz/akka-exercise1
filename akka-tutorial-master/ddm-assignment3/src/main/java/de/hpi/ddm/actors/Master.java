package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import lombok.*;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	//public static Props props(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
	public static Props props(final ActorRef reader, final ActorRef collector) {
		//return Props.create(Master.class, () -> new Master(reader, collector, welcomeData));
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	//public Master(final ActorRef reader, final ActorRef collector, final BloomFilter welcomeData) {
	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.occupiedWorkers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		//this.welcomeData = welcomeData;
		this.pwdHashmap = new HashMap<Integer, Password>();
		this.pwdDecryptionQueue = new LinkedList<SolvePasswordMessage>();
		this.hintDecryptionQueue = new LinkedList<SolveHintMessage>();
		this.pwdLength = 0;
		this.possiblePermutationsForHintsList = new ArrayList<char[]>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Getter @Setter @ToString @AllArgsConstructor @NoArgsConstructor
	public static class SolveHintMessage implements Serializable {
		private int ID;
		private String hint;
		private char[] hintCharCombination;
	}

	@Getter @Setter @ToString @AllArgsConstructor @NoArgsConstructor
	public static class SolvePasswordMessage implements Serializable {
		Password password;
	}

	@Getter @Setter @ToString @AllArgsConstructor @NoArgsConstructor
	public static class PasswordDecryptedMessage implements Serializable {
		private int ID;
		private String decryptedPassword;
		private String encryptedPassword;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;
	//private final BloomFilter welcomeData;

	private List<Boolean> occupiedWorkers;
	private HashMap<Integer, Password> pwdHashmap; //to keep overview of data associated with each ID/password
	//private HashMap<Character, ArrayList<String>> hashedHintUniverse; //for each missing char (from hint) all hashed permutations
	private Queue<SolveHintMessage> hintDecryptionQueue;
	private Queue<SolvePasswordMessage> pwdDecryptionQueue;


	private int pwdLength;
	private char[] charUniverse;

	private final ArrayList<char[]> possiblePermutationsForHintsList;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle) //TODO: who sends this?
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(Worker.HintSolvedMessage.class, this::handle)
				.match(Worker.PasswordDecryptedMessage.class, this::handle)
				.match(Worker.AvailabilityMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		this.reader.tell(new Reader.ReadMessage(), this.self());
		this.log().info("DEBUG: StartMessage");
	}
	
	protected void handle(BatchMessage message) {
		
		// TODO: This is where the task begins:
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		//Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.log().info("DEBUG: Empty lines");
			this.terminate();
			return;
		} else{
			this.log().info("DEBUG: Batch Message lines: " + Arrays.toString(message.getLines().get(0)));
		}


		if(this.pwdLength == 0){
			this.pwdLength = Integer.parseInt(message.getLines().get(0)[3]);
			this.charUniverse = message.getLines().get(0)[2].toCharArray();
			getCharPermutations();
		}


		String[] hints;
		for (String[] line : message.getLines()) {
			//System.out.println(Arrays.toString(messageLine)); //Print message
			//System.out.println(messageLine[4]);
			int ID = Integer.parseInt(line[0]);
			hints = new String[line.length-5];
			for (int i = 5; i < line.length; i++) {
				hints[i-5] = line[i];
			}
			Password password = new Password(ID, line[1], line[4], hints, this.charUniverse, this.pwdLength);
			//this.log().info("DEBUG: Password: " + password);
			//System.out.println(password);
			this.pwdHashmap.put(ID, password); //adding password to hashmap
			for (int i = 0; i < password.getEncrHints().length; i++) {
				for (char[] chars : this.possiblePermutationsForHintsList) {
					//TODO: lisa
					this.hintDecryptionQueue.add(new SolveHintMessage(ID, password.getEncrHints()[i], chars));
				}
			}
		}

		sendSolveHintMessage();

		
		// TODO: Process the lines with the help of the worker actors
		//for (String[] line : message.getLines())
		//	this.log().error("Need help processing: {}", Arrays.toString(line));
		
		// TODO: Send (partial) results to the Collector
		//this.collector.tell(new Collector.CollectMessage("If I had results, this would be one."), this.self());
		
		// TODO: Fetch further lines from the Reader
		this.log().info("DEBUG: Fetching data");

		this.reader.tell(new Reader.ReadMessage(), this.self());
		
	}

	protected void sendSolveHintMessage() {
		for (int i = 0; i < this.occupiedWorkers.size(); i++) {
			if (!this.occupiedWorkers.get(i)){
				try {
					this.workers.get(i).tell(this.hintDecryptionQueue.remove(), this.self());
					this.occupiedWorkers.set(i, true); //occupied
				}catch (NoSuchElementException ignored){};
			}
		}
	}

	protected void sendSolvePasswordMessage(){
		for (int i = 0; i < occupiedWorkers.size(); i++) {
			if (!this.occupiedWorkers.get(i)){
				try {
					SolvePasswordMessage messageToSend = this.pwdDecryptionQueue.remove(); //.poll para ver si tiene elemento primero
					this.workers.get(i).tell(messageToSend, this.self());
					this.occupiedWorkers.set(i, true); //Set occupied
					this.log().info("PasswordCracking message sent to worker");
				}catch (NoSuchElementException e){};

			}
		}
	}

	private void handle(Worker.PasswordDecryptedMessage passwordDecryptedMessage) {
		int id = passwordDecryptedMessage.getID();
		ActorRef messageSender = this.sender();
		String decryptedPassword = passwordDecryptedMessage.getDecryptedPassword();
		for (int i = 0; i < workers.size(); i++) {
			if(messageSender.equals(workers.get(i))){
				if(this.pwdHashmap.containsKey(id)){
					if(!decryptedPassword.equals("")){
						pwdHashmap.get(id).setDecrPwd(decryptedPassword);
						this.log().info("Decrypted Password from " + pwdHashmap.get(id).getName() + " with ID " + pwdHashmap.get(id).getID() + ": " + decryptedPassword);
						//Send solution to the collector
						this.collector.tell(new Collector.CollectMessage("Decrypted Password from " + pwdHashmap.get(id).getName() + " with ID " + pwdHashmap.get(id).getID() + ": " + decryptedPassword), this.self());
						this.collector.tell(new Collector.PrintMessage(), this.self());
						break;
					}
				}
				//System.out.println("Worker is available");
				this.occupiedWorkers.set(i, false); //Set available
			}
		}

		sendSolvePasswordMessage();
		sendSolveHintMessage();

		if(pwdDecryptionQueue.isEmpty() && hintDecryptionQueue.isEmpty()){ //Check to see if there are more tasks in queues
			this.reader.tell(new Reader.ReadMessage(), this.self()); //tell reader to send more batches of passwords
		}
	}
	
	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		// TODO: add poison pill if master ended?

		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.occupiedWorkers.add(false);
		this.log().info("Registered {}", this.sender());
		
		//this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(this.welcomeData, this.sender()), this.self()); // replaced new Worker.WelcomeMessage(this.welcomeData)
		
		// TODO: Assign some work to registering workers. Note that the processing of the global task might have already started.
	}

	private void handle(Worker.AvailabilityMessage availableWorkerMessage) {
		ActorRef sender = this.sender();
		for (int i = 0; i < workers.size(); i++) {
			if(sender.equals(workers.get(i))){
				this.occupiedWorkers.set(i, false); //set to false: available
				sendSolvePasswordMessage();
				sendSolveHintMessage();
				break;
			}
		}
	}

	//get hashed hint permutations for each one combination back
	private void handle(Worker.HintSolvedMessage hintMessage) {
		int ID = hintMessage.getID();
		ActorRef messageSender = this.sender();
		this.log().info("Password hint decrypted from ID: " + ID + " | decrypted hint: " + hintMessage.getDecryptedHint());
		for (int i = 0; i < workers.size(); i++) {
			if(messageSender.equals(workers.get(i))){
				if(this.pwdHashmap.containsKey(ID)){
					this.log().info("Added hint to hashmap with key " + ID);
					this.pwdHashmap.get(ID).addDecrHint(hintMessage.getEncryptedHint(), hintMessage.getDecryptedHint());
					this.log().info("Password object: " + this.pwdHashmap.get(ID).toString());
					//this.log().info("Saved hint for " + this.ID_PasswordHashMap.get(ID).getName() + " with ID: " + this.ID_PasswordHashMap.get(ID).getID() + "\n" + "		Hints Array: " + this.ID_PasswordHashMap.get(ID).getDecryptedPassword().toString());
					break;
				}
				this.occupiedWorkers.set(i, false); //Set available
			}
		}

		//check if all hints from ID are cracked
		if(this.pwdHashmap.get(ID).checkAllHintsDecrypted()){
			//send decrypt password message to worker!
			Password password = (Password) pwdHashmap.get(ID).clone(); //clone the password from hashmap to send to the worker
			this.pwdDecryptionQueue.add(new SolvePasswordMessage(password));
			this.log().info("Added Password Cracking work for ID" + ID + " with Password object: " + this.pwdHashmap.get(ID).toString());
			this.log().info("PasswordCrackingQueue size: " + this.pwdDecryptionQueue.size());
			sendSolvePasswordMessage(); //12. send password cracking
		}
		sendSolveHintMessage();

		//check if workers are free and there is no more tasks in both queues -> (this means we need to terminate the program)
		if(pwdDecryptionQueue.isEmpty() && hintDecryptionQueue.isEmpty()){ //Check to see if there are more tasks in queues
			this.reader.tell(new Reader.ReadMessage(), this.self()); //tell reader to send more batches of passwords
		}
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
		this.log().info("DEBUG: Terminated");
	}

	@Getter @Setter @ToString @NoArgsConstructor
	//static & @NoArgsConstructor for serialization with kryo
	protected static class Password implements Serializable, Cloneable{
		private int ID;
		private String name;
		private int pwdLength;
		private char[] charUniverse;
		private String encrPwd;
		private String decrPwd;
		private String[] encrHints;
		private String[] decrHints;

		public Password(int ID, String name, String encryptedPassword, String[] encryptedHints, char[] charUniverse, int pwdLength){
			this.ID = ID;
			this.name = name;
			this.encrPwd = encryptedPassword;
			this.decrPwd = "";
			this.encrHints = encryptedHints.clone();
			this.decrHints = new String[this.encrHints.length];
			Arrays.fill(this.decrHints, "");
			this.charUniverse = charUniverse;
			this.pwdLength = pwdLength;
		}

		public Object clone(){
			try {
				return super.clone();
			} catch (CloneNotSupportedException e){
				return this;
			}
		}

		public void setDecrHintsOnIndex(int index, String stringValue){
			decrHints[index] = stringValue;
		}

		public String setDecrHintsOnIndex(int index){
			return decrHints[index];
		}

		public String getEncrHintsOnIndex(int index){
			return encrHints[index];
		}

		public int getIndexFromEncrHintsElem(String stringElement){
			for (int i = 0; i < encrHints.length; i++) {
				if(stringElement.equals(encrHints[i])){
					return i;
				}
			}
			return -1;
		}

		public void addDecrHint(String encrypted, String decrypted){
			int index = getIndexFromEncrHintsElem(encrypted);
			setDecrHintsOnIndex(index, decrypted);
		}

		//check if all hints are not empty
		public boolean checkAllHintsDecrypted(){
			for (int i = 0; i < decrHints.length; i++) {
				if(decrHints[i].equals("")){
					return false;
				}
			}
			return true;
		}
	}

		//Character permutations for hints
		//https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
		//https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
		private void getCharPermutations() {
			char[] combination = new char[this.charUniverse.length - 1];
			for (int i = 0; i < this.charUniverse.length; i++) {
				int combination_index = 0;
				for (int j = 0; j < this.charUniverse.length; j++) {
					if (j != i) {
						combination[combination_index++] = this.charUniverse[j];
					}
				}
				if (combination.length == this.pwdLength) {
					this.possiblePermutationsForHintsList.add(combination.clone());
				} else {
					getCharPermutations();
				}
			}
		}


	}

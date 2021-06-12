package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.structures.BloomFilter;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";
	private String decryptedPwd;

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
		this.decryptedPwd = "";
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}

	@Data
	@AllArgsConstructor @NoArgsConstructor
	public static class HintSolvedMessage implements Serializable {
		private int ID;
		private String encryptedHint;
		private String decryptedHint;
	}

	@Data
	@AllArgsConstructor @NoArgsConstructor
	public static class PasswordDecryptedMessage implements Serializable {
		private int ID;
		private String encryptedPassword;
		private String decryptedPassword;
	}

	@AllArgsConstructor
	public static class AvailabilityMessage implements Serializable{}
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;
	private long registrationTime;
	private ActorRef master;
	private String hint;
	private int ID;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class); // 2 - worker subscribes to cluster
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(WelcomeMessage.class, this::handle)
				.match(Master.SolveHintMessage.class, this::handle)
				.match(Master.SolvePasswordMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) { // 3 - first message via CurrentClusterState
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up())) //if status up
				this.register(member); // register function evoked
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	} // 4 - handle registration via MemberUp if  current master dead

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self()); // 5 - first communication between worker and master
			
			this.registrationTime = System.currentTimeMillis();
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private void handle(WelcomeMessage message) {
		final long transmissionTime = System.currentTimeMillis() - this.registrationTime;
		this.log().info("WelcomeMessage with " + message.getWelcomeData().getSizeInMB() + " MB data received in " + transmissionTime + " ms.");
	}

	private void handle(Master.SolveHintMessage message){
		this.master = this.sender();
		this.ID = message.getID();
		this.hint = message.getHint();

		//this.log().info("Started decrypting hint");

		//System.out.println(this.hint);

		//List<String> allPermutations = new ArrayList<>(); //Not needed unless we want to see permutations checked
		heapPermutation(message.getHintCharCombination(), message.getHintCharCombination().length);
		//this.log().info("Size of permutations tried: " + allPermutations.size());

		this.master.tell(new AvailabilityMessage(), this.self()); //tell master it is free
	}

	private void handle(Master.SolvePasswordMessage message) {
		this.ID = message.getPassword().getID();
		int pwdLength = message.getPassword().getPwdLength();

		String encrypted = message.getPassword().getEncrPwd();


		String[] hints = message.getPassword().getDecrHints().clone();
		char[] alphabet = message.getPassword().getCharUniverse().clone();


		char[] set = getHintChars(hints, alphabet);
		int n = set.length;

		getAllHintPermutations(set, "", n,pwdLength,encrypted);
		if(!this.decryptedPwd.equals("")) {
			//this.log().info("Password found");
			this.master.tell(new PasswordDecryptedMessage(this.ID, encrypted, this.decryptedPwd), this.self());
			this.master.tell(new AvailabilityMessage(), this.self()); //tell master it is free
			return;
		}
		//this.log().info("No password found");
		this.master.tell(new PasswordDecryptedMessage(this.ID, encrypted, this.decryptedPwd), this.self());
	}
	
	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size) {
		// If size is 1, store the obtained permutation
		if (size == 1) {
			String permutationHash = hash(new String(a));
			if(this.hint.equals(permutationHash)){
				this.log().info("		Hint for ID " + this.ID + " decrypted");
				this.master.tell(new HintSolvedMessage(this.ID, this.hint, new String(a)), this.self());
				return;
			}
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	private char[] getHintChars (String[] passwordHints, char[] alphabetArray){
		List<Character> alphabet = new ArrayList<Character>();
		for (char c : alphabetArray) {
			alphabet.add(c);
		}
		for (String hint : passwordHints) {
			List<Character> tempAlphabet =  new ArrayList<Character>();
			for (int i = 0; i < hint.length(); i++) {
				char hintChar = hint.charAt(i);
				for (int k = 0; k < alphabet.size(); k++) {
					char alphabetChar = alphabet.get(k);
					if(alphabetChar == hintChar) {
						if(alphabet.contains(hintChar)) {
							tempAlphabet.add(hintChar);
						}
					}
				}
			}
			alphabet = tempAlphabet;
		}
		StringBuilder returnString = new StringBuilder();
		for (Character character : alphabet) {
			returnString.append(character);
		}
		return returnString.toString().toCharArray();
	}




	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	void getAllHintPermutations(char[] set, String prefix, int n, int k, String encrypted)
	{
		// Base case: k is 0,
		// print prefix
		if (k == 0)
		{
			String curr_hashed = hash(prefix);
			if (curr_hashed.equals(encrypted)){
				this.decryptedPwd = prefix;
				this.log().info("Found password for ID  " + this.ID + ": " + this.decryptedPwd);
			}
			return;
		}
		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{
			// Next character of input added
			String newPrefix = prefix + set[i];
			// k is decreased, because
			// we have added a new character
			getAllHintPermutations(set, newPrefix, n, k - 1, encrypted);
		}
	}


}
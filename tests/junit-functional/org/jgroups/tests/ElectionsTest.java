 package org.jgroups.tests;

 import org.jgroups.Address;
 import org.jgroups.Global;
 import org.jgroups.JChannel;
 import org.jgroups.protocols.raft.*;
 import org.jgroups.util.Util;
 import org.testng.annotations.AfterMethod;
 import org.testng.annotations.BeforeMethod;
 import org.testng.annotations.Test;

 import java.lang.reflect.Method;
 import java.util.ArrayList;
 import java.util.Arrays;
 import java.util.List;
 import java.util.function.Supplier;

 import static org.testng.Assert.fail;

 /**
  * Tests elections
  * @author Bela Ban
  * @since  0.2
  */
 @Test(groups=Global.FUNCTIONAL,singleThreaded=true)
 public class ElectionsTest {
     protected JChannel                  a,b,c;
     protected static final String       CLUSTER="ElectionsTest";
     protected static final List<String> members=Arrays.asList("A", "B", "C");
     protected static final Method       startElectionTimer;
     protected static final byte[]       BUF={};

     static {
         try {
             startElectionTimer=ELECTION.class.getDeclaredMethod("startElectionTimer");
             startElectionTimer.setAccessible(true);
         }
         catch(NoSuchMethodException ex) {
             throw new RuntimeException(ex);
         }
     }

     @BeforeMethod protected void init() throws Exception {
         a=create("A"); a.connect(CLUSTER);
         b=create("B"); b.connect(CLUSTER);
         c=create("C"); c.connect(CLUSTER);
         Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
     }

     @AfterMethod protected void destroy() {
         close(true, true, c,b,a);
     }


     /** All members have the same (initial) logs, so any member can be elected as leader */
     public void testSimpleElection() throws Exception {
         startElections(a, b, c);
         assertLeader(20, 500, null, a,b,c);
     }

     /** Test whether having min-interval higher than max-interval throws an exception */
     public void testMinIntervalAboveMaxInterval() {
    	 ELECTION election=new ELECTION().noElections(true).electionMaxInterval(500L).electionMinInterval(1000L).heartbeatInterval(100L);
    	 try {
			election.init();
			fail("should have thrown an exception due to misconfiguration");
		} catch (Exception ex) {
			assert String.format("election_min_interval (%d) needs to be smaller than "
                + "election_max_interval (%d)", election.electionMinInterval(), election.electionMaxInterval()).equals(ex.getMessage());
		}
     }

     /** Test whether having heartbeat-interval higher than min-interval throws an exception */
     public void testHeartbeatIntervalAboveMinInterval() {
    	 ELECTION election=new ELECTION().noElections(true).electionMaxInterval(500L).electionMinInterval(250L).heartbeatInterval(300L);
    	 try {
			election.init();
			fail("should have thrown an exception due to misconfiguration");
		} catch (Exception ex) {
			assert String.format("heartbeat_interval (%d) needs to be smaller than "
                + "election_min_interval (%d)", election.heartbeatInterval(), election.electionMinInterval()).equals(ex.getMessage());
		}
     }

     /** Test whether heartbeat-interval below one throws an Exception */
     public void testNegativeHeartbeat() {
    	 ELECTION election=new ELECTION().noElections(true).electionMaxInterval(500L).electionMinInterval(250L).heartbeatInterval(-300L);
    	 try {
			election.init();
			fail("should have thrown an exception due to misconfiguration");
		} catch (Exception ex) {
			assert String.format("heartbeat_interval (%d) must not be below one", election.heartbeatInterval()).equals(ex.getMessage());
		}
     }


     /**
      * B and C have longer logs than A: one of {B,C} must become coordinator, but *not* A
      */
     public void testElectionWithLongLog() throws Exception {
         setLog(b, 1,1,2);
         setLog(c, 1,1,2);
         startElections(a, b, c);
         Address leader=assertLeader(20, 500, null, a, b, c);
         assert leader.equals(b.getAddress()) || leader.equals(c.getAddress());
         assert !leader.equals(a.getAddress());
     }

     /** ELECTION should look for RAFT or its subclasses */
     public void testRAFTSubclass() throws Exception {
         close(true, true, c);
         c=createWithRAFTSubclass("C");
         c.connect(CLUSTER);
     }

     protected static JChannel createWithRAFTSubclass(String name) throws Exception {
         return create(name, () -> new RAFT(){});
     }

     protected static JChannel create(String name) throws Exception {
         return create(name, RAFT::new);
     }

     protected static JChannel create(String name, Supplier<RAFT> raftSupplier) throws Exception {
         ELECTION election=new ELECTION().noElections(true);
         RAFT raft=raftSupplier.get().members(members).raftId(name)
           .logClass("org.jgroups.protocols.raft.InMemoryLog").logName(name + "-" + CLUSTER);
         REDIRECT client=new REDIRECT();
         return new JChannel(Util.getTestStack(election, raft, client)).name(name);
     }


     protected static void close(boolean remove_log, boolean remove_snapshot, JChannel... channels) {
         for(JChannel ch: channels) {
             if(ch == null)
                 continue;
             close(remove_log, remove_snapshot, ch);
         }
     }

     protected static void close(boolean remove_log, boolean remove_snapshot, JChannel ch) {
         if(ch == null)
             return;
         RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
         if(remove_log)
             raft.log().delete(); // remove log files after the run
         if(remove_snapshot)
             raft.deleteSnapshot();
         Util.close(ch);
     }

     protected static void setLog(JChannel ch, int... terms) {
         RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
         Log log=raft.log();
         int index=log.lastAppended();
         for(int term: terms)
             log.append(++index, true, new LogEntry(term, BUF));
     }

     protected static void startElections(JChannel... channels) throws Exception {
         for(JChannel ch: channels) {
             ELECTION election=ch.getProtocolStack().findProtocol(ELECTION.class);
             election.noElections(false);
             startElectionTimer.invoke(election);
         }
     }

     protected static boolean isLeader(JChannel ch) {
         RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
         return ch.getAddress().equals(raft.leader());
     }

     protected static List<Address> leaders(JChannel... channels) {
         List<Address> leaders=new ArrayList<>(channels.length);
         for(JChannel ch: channels) {
             if(isLeader(ch))
                 leaders.add(ch.getAddress());
         }
         return leaders;
     }

     /** If expected is null, then any member can be a leader */
     protected static Address assertLeader(int times, long sleep, Address expected, JChannel... channels) {
         // wait until there is 1 leader
         for(int i=0; i < times; i++) {
             List<Address> leaders=leaders(channels);
             if(!leaders.isEmpty()) {
                 int size=leaders.size();
                 assert size <= 1;
                 Address leader=leaders.get(0);
                 System.out.println("leader: " + leader);
                 assert expected == null || expected.equals(leader);
                 break;
             }

             Util.sleep(sleep);
         }
         List<Address> leaders=leaders(channels);
         assert leaders.size() == 1 : "leaders=" + leaders;
         Address leader=leaders.get(0);
         System.out.println("leader = " + leader);
         return leader;
     }


 }

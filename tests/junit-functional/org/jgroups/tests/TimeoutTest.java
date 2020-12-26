package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.testng.annotations.Test;

import java.util.Random;

@Test
public class TimeoutTest {

    protected final org.jgroups.logging.Log log= LogFactory.getLog(this.getClass());

    public void testTimeout() throws Exception {
        /* Create state machine */
        JChannel ch = new JChannel("timeout-raft.xml");
        final ReplicatedStateMachine<Object, Object> sm = new ReplicatedStateMachine<>(ch);
        sm.channel().connect("cluster");
        /* 2s timeout writing to state machine */
        sm.timeout(2000);

        /* Wait for leader */
        final RAFT raft = ch.getProtocolStack().findProtocol(RAFT.class);
        while (!Thread.currentThread().isInterrupted() && !raft.isLeader()) {
            Thread.sleep(50);
        }

        /* Write 1k values every ~50ms */
        Random rnd = new Random();
        byte[] b = new byte[1024];
        long index = 0L;
        while (!Thread.currentThread().isInterrupted()) {
            rnd.nextBytes(b);
            sm.put(++index, b);
            System.out.println(index);
            Thread.sleep(50);
        }
    }
}

package edu.gmu.cs475.test;

import edu.gmu.cs475.internal.TestingClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class P2ReplicationTests extends Base475Test {

	@Rule
	public Timeout globalTimeout = new Timeout(30000);
	boolean operationsCompletedConcurrently = true;
	boolean ok = true;

	@Test
	public void testWriteReadOneKVServer() throws Exception {
		TestingClient c1 = newClient("Leader");
		blockUntilLeader(c1);
		String k1 = getNewKey();

		String k2 = getNewKey();

		Assert.assertNull(c1.getValue(k1));
		Assert.assertNull(c1.getValue(k1));

		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		c1.setValue(k1, v1);
		Assert.assertEquals(v1, c1.getValue(k1));
		Assert.assertEquals(v1, c1.getValue(k1));

		c1.setValue(k1, v2);
		Assert.assertEquals(v2, c1.getValue(k1));
		Assert.assertEquals(v2, c1.getValue(k1));

		c1.setValue(k2, v3);
		Assert.assertEquals(v3, c1.getValue(k2));
		Assert.assertEquals(v3, c1.getValue(k2));

	}

	@Test
	public void testWriteOneClientReadAnother() throws Exception {

		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);

		String k1 = getNewKey();

		String k2 = getNewKey();

		Assert.assertNull(c1.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));

		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		c2.setValue(k1, v1);

		Assert.assertEquals(v1, c2.getValue(k1));
		Assert.assertEquals(v1, c3.getValue(k1));
		Assert.assertEquals(v1, c1.getValue(k1));

		c3.setValue(k2, v2);

		Assert.assertEquals(v2, c2.getValue(k2));
		Assert.assertEquals(v2, c3.getValue(k2));
		Assert.assertEquals(v2, c1.getValue(k2));
	}

	@Test
	public void testReadAndWriteBothCache() throws Exception {
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		TestingClient c4 = newClient("Follower (C4)");
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);
		blockUntilMemberJoins(c4);

		String k1 = getNewKey();

		String k2 = getNewKey();

		Assert.assertNull(c1.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));

		assertGetValueCalled(c1, 6, 7); //C2,C3,C4 must not cache


		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		//Try setting a key, doing the write on c2, checking on others
		setKeyAndRead(false, k1, v1, c1, c2, c3, c4);

		//Try setting a different key, doing the write on c3, checking on others
		setKeyAndRead(false, k2, v2, c1, c3, c2, c4);
		setKeyAndRead(false, getNewKey(), v3, c1, c4, c3, c2);
	}

	@Test
	public void testWriteInvalidatesCache() throws Exception {

		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		TestingClient c4 = newClient("Follower (C4)");
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);
		blockUntilMemberJoins(c4);


		String k1 = getNewKey();
		String k2 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		Assert.assertNull(c1.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));

		assertGetValueCalled(c1, 6, 7); //C2,C3,C4 must not cache

		setKeyAndRead(false, k1, v1, c1, c2, c3, c4);
		setKeyAndRead(false, k2, v2, c1, c2, c3, c4);
		setKeyAndRead(true, k1, v2, c1, c4, c3, c2);
		setKeyAndRead(true, k2, v2, c1, c3, c2, c4);

	}

	@Test
	public void testInvalidateIsOnlySentToRelevantClients() throws Exception {

		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		TestingClient c4 = newClient("Follower (C4)");
		TestingClient c5 = newClient("Follower (C5)");
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);
		blockUntilMemberJoins(c4);


		String k1 = getNewKey();
		String k2 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		Assert.assertNull(c1.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));
		Assert.assertNull(c2.getValue(k1));
		Assert.assertNull(c3.getValue(k1));
		Assert.assertNull(c4.getValue(k1));

		assertGetValueCalled(c1, 6, 7); //C2,C3,C4 must not cache

		setKeyAndRead(false, k1, v1, c1, c2, c3, c5);
		setKeyAndRead(false, k2, v2, c1, c2, c3, c4);
		setKeyAndRead(true, k1, v2, c1, c4, c3, c5);
		assertInvalidateCalled(c2, k1);
		setKeyAndRead(true, k2, v2, c1, c3, c2, c4);

	}

	@Test
	public void testCanWriteDifferentKeysAtOnce() throws Exception {

		ok = true;
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		CountDownLatch latch = new CountDownLatch(4);
		operationsCompletedConcurrently = true;
		c2.setInvalidateHandler((key) -> {
					latch.countDown();
					try {
						operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
					} catch (InterruptedException ex) {

					}
					return null;
				}
		);
		c1.setInvalidateHandler((key) -> {
					latch.countDown();
					try {
						operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
					} catch (InterruptedException ex) {

					}
					return null;
				}
		);
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);


		String k1 = getNewKey();
		String k2 = getNewKey();
		String k3 = getNewKey();
		String k4 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();
		String v4 = getNewValue();


		setKeyAndRead(false, k1, v1, c1, c2, c3);
		setKeyAndRead(false, k2, v2, c1, c2, c3);
		setKeyAndRead(false, k3, v3, c1, c2, c3);
		setKeyAndRead(false, k4, v4, c1, c2, c3);


		Thread t1 = new Thread(() -> {
			try {
				c1.setValue(k1, v1);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				c1.setValue(k2, v2);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t3 = new Thread(() -> {
			try {
				c1.setValue(k3, v3);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t4 = new Thread(() -> {
			try {
				c1.setValue(k4, v4);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		Assert.assertTrue("Expected to be able to write to two separate keys concurrently", operationsCompletedConcurrently);
		Assert.assertTrue(ok);
	}

	@Test
	public void testMutualExclusionWritingSameKey() throws Exception {
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		CountDownLatch latch = new CountDownLatch(4);
		ok = true;
		operationsCompletedConcurrently = true;
		c2.setInvalidateHandler((key) -> {
					latch.countDown();
					try {
						operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
					} catch (InterruptedException ex) {

					}
					return null;
				}
		);
		c1.setInvalidateHandler((key) -> {
					latch.countDown();
					try {
						operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
					} catch (InterruptedException ex) {

					}
					return null;
				}
		);
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);


		String k1 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();
		String v4 = getNewValue();


		setKeyAndRead(false, k1, v1, c1, c2, c3);

		Thread t1 = new Thread(() -> {
			try {
				c1.setValue(k1, v1);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				c1.setValue(k1, v2);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t3 = new Thread(() -> {
			try {
				c1.setValue(k1, v3);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t4 = new Thread(() -> {
			try {
				c1.setValue(k1, v4);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		t1.join();
		t2.join();
		t3.join();
		t4.join();
		Assert.assertFalse("Expected to not be able to write to the same key concurrently", operationsCompletedConcurrently);
		Assert.assertTrue(ok);
	}

	@Test
	public void testReadWhileWritingWaits() throws Exception {

		ok = true;
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		CountDownLatch latch = new CountDownLatch(2);
		CountDownLatch inInvalidate = new CountDownLatch(1);
		operationsCompletedConcurrently = true;

		c2.setInvalidateHandler((key) -> {
					inInvalidate.countDown();
					if (operationsCompletedConcurrently) {
						latch.countDown();
						try {
							operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
						} catch (InterruptedException ex) {

						}
					}
					return null;
				}
		);
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);


		String k1 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();


		setKeyAndRead(false, k1, v1, c1, c2, c3);

		c1.setGetHandler((key) -> {
					if (operationsCompletedConcurrently) {
						latch.countDown();
						try {
							operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
						} catch (InterruptedException ex) {

						}
					}
					return null;
				}
		);
		Thread t1 = new Thread(() -> {
			try {
				c1.setValue(k1, v2);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				c3.getValue(k1);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		t1.start();
		inInvalidate.await();
		t2.start();
		t1.join();
		t2.join();
		Assert.assertFalse("Expected to not be able to read and write to the same key concurrently", operationsCompletedConcurrently);
		Assert.assertTrue(ok);
	}


	@Test
	public void testReadWhileWritingDifferentKeysIsOK() throws Exception {
		ok = true;
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		CountDownLatch latch = new CountDownLatch(2);
		CountDownLatch inInvalidate = new CountDownLatch(1);
		operationsCompletedConcurrently = true;

		c2.setInvalidateHandler((key) -> {
					inInvalidate.countDown();
					if (operationsCompletedConcurrently) {
						latch.countDown();
						try {
							operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
						} catch (InterruptedException ex) {

						}
					}
					return null;
				}
		);
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);


		String k1 = getNewKey();
		String k2 = getNewKey();
		String v1 = getNewValue();
		String v2 = getNewValue();
		String v3 = getNewValue();

		setKeyAndRead(false, k1, v1, c1, c2, c3);
		c1.setValue(k2, v2);

		c1.setGetHandler((key) -> {
					if (operationsCompletedConcurrently) {
						latch.countDown();
						try {
							operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
						} catch (InterruptedException ex) {

						}
					}
					return null;
				}
		);
		Thread t1 = new Thread(() -> {
			try {
				c2.setValue(k1, v2);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				c3.getValue(k2);
			} catch (Throwable ex) {
				ex.printStackTrace();
				ok = false;
			}
		});
		t1.start();
		inInvalidate.await();
		t2.start();
		t1.join();
		t2.join();
		Assert.assertTrue("Expected to be able to read and write different keys concurrently", operationsCompletedConcurrently);
	}

	@Test
	public void testConcurrentServerReadsOK() throws Exception {

		ok = true;
		TestingClient c1 = newClient("Leader (C1)");
		blockUntilLeader(c1);
		TestingClient c2 = newClient("Follower (C2)");
		TestingClient c3 = newClient("Follower (C3)");
		CountDownLatch latch = new CountDownLatch(2);
		operationsCompletedConcurrently = true;
		blockUntilMemberJoins(c2);
		blockUntilMemberJoins(c3);


		String k1 = getNewKey();
		String v1 = getNewValue();

		setKeyAndRead(false, k1, v1, c1, c2, c3);

		c1.setGetHandler((key) -> {
					if (operationsCompletedConcurrently) {
						latch.countDown();
						try {
							operationsCompletedConcurrently &= latch.await(2, TimeUnit.SECONDS);
						} catch (InterruptedException ex) {

						}
					}
					return null;
				}
		);
		Thread t1 = new Thread(() -> {
			try {
				c3.getValue(k1);
			} catch (Throwable t) {
				t.printStackTrace();
				ok = false;
			}
		});
		Thread t2 = new Thread(() -> {
			try {
				c3.getValue(k1);
			} catch (Throwable t) {
				t.printStackTrace();
				ok = false;
			}
		});
		t1.start();
		t2.start();
		t1.join();
		t2.join();
		Assert.assertTrue("Expected to be able to read and write different keys concurrently", operationsCompletedConcurrently);
		Assert.assertTrue(ok);
	}
}

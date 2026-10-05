/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link LockablePool}.
 */
public class LockablePoolTests extends OpenSearchTestCase {

    /**
     * A simple lockable entry for testing.
     */
    static class LockableEntry implements Lockable {
        final String id;
        private final ReentrantLock delegate = new ReentrantLock();

        LockableEntry(String id) {
            this.id = id;
        }

        @Override
        public void lock() {
            delegate.lock();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        boolean isHeldByCurrentThread() {
            return delegate.isHeldByCurrentThread();
        }
    }

    static class ValueEqualLockableEntry extends LockableEntry {
        ValueEqualLockableEntry(String id) {
            super(id);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ValueEqualLockableEntry other && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private static int idCounter = 0;

    private LockablePool<LockableEntry> createPool() {
        return new LockablePool<>(() -> new LockableEntry("entry-" + idCounter++), LinkedList::new, 1);
    }

    public void testGetAndLockReturnsLockedItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        assertNotNull(item);
        assertTrue(item.isHeldByCurrentThread());
        pool.releaseAndUnlock(item);
    }

    public void testReleaseAndUnlockAllowsReuse() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry first = pool.getAndLock();
        pool.releaseAndUnlock(first);

        LockableEntry second = pool.getAndLock();
        assertSame("Released item should be reused", first, second);
        pool.releaseAndUnlock(second);
    }

    public void testClosedPoolThrowsOnGetAndLock() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        pool.close();

        IllegalStateException ex = expectThrows(IllegalStateException.class, pool::getAndLock);
        assertEquals("LockablePool is already closed", ex.getMessage());
    }

    public void testCheckoutAllReturnsAllItems() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry a = pool.getAndLock();
        pool.releaseAndUnlock(a);
        LockableEntry b = pool.getAndLock();
        pool.releaseAndUnlock(b);

        // a and b may be the same item (reuse), so pool may have 1 or 2 items
        List<LockableEntry> all = pool.checkoutAll();
        assertFalse("checkoutAll should return at least one item", all.isEmpty());
    }

    public void testCheckoutAllOnEmptyPoolReturnsEmptyList() {
        LockablePool<LockableEntry> pool = createPool();
        List<LockableEntry> all = pool.checkoutAll();
        assertTrue("checkoutAll on empty pool should return empty list", all.isEmpty());
    }

    public void testCheckoutAllReturnsUnmodifiableList() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        List<LockableEntry> all = pool.checkoutAll();
        expectThrows(UnsupportedOperationException.class, () -> all.add(new LockableEntry("rogue")));
    }

    public void testIsRegisteredTrueForPoolItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        assertTrue(pool.isRegistered(item));
        pool.releaseAndUnlock(item);
    }

    public void testIsRegisteredFalseForUnknownItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry stranger = new LockableEntry("stranger");
        assertFalse(pool.isRegistered(stranger));
    }

    public void testIteratorReturnsSnapshot() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry a = pool.getAndLock();
        pool.releaseAndUnlock(a);

        int count = 0;
        for (LockableEntry item : pool) {
            assertNotNull(item);
            count++;
        }
        assertEquals(1, count);
    }

    public void testClosedPoolThrowsOnCheckoutAll() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        pool.close();

        IllegalStateException ex = expectThrows(IllegalStateException.class, pool::checkoutAll);
        assertEquals("LockablePool is already closed", ex.getMessage());
    }

    public void testRejectsLockableWithValueEquality() {
        LockablePool<ValueEqualLockableEntry> pool = new LockablePool<>(() -> new ValueEqualLockableEntry("equal"), LinkedList::new, 1);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, pool::getAndLock);
        assertThat(ex.getMessage(), containsString("must use Object identity equality and hash codes"));
    }

    public void testCloseAndCheckoutAllIncludesInFlightCreationAndPreventsFurtherAdmission() throws Exception {
        CountDownLatch supplierEntered = new CountDownLatch(1);
        CountDownLatch allowCreation = new CountDownLatch(1);
        CountDownLatch itemAcquired = new CountDownLatch(1);
        CountDownLatch releaseItem = new CountDownLatch(1);
        AtomicReference<LockableEntry> acquired = new AtomicReference<>();
        AtomicReference<List<LockableEntry>> checkedOut = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        LockablePool<LockableEntry> pool = new LockablePool<>(() -> {
            supplierEntered.countDown();
            await(allowCreation);
            return new LockableEntry("in-flight");
        }, LinkedList::new, 1);

        Thread getter = new Thread(() -> {
            try {
                LockableEntry item = pool.getAndLock();
                acquired.set(item);
                itemAcquired.countDown();
                await(releaseItem);
                item.unlock();
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "pool-getter");
        Thread closer = new Thread(() -> {
            try {
                checkedOut.set(pool.closeAndCheckoutAll());
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "pool-closer");

        try {
            getter.start();
            assertTrue("supplier must begin creating an item", supplierEntered.await(30, TimeUnit.SECONDS));
            closer.start();
            allowCreation.countDown();
            assertTrue("getter must receive the in-flight item", itemAcquired.await(30, TimeUnit.SECONDS));
            assertTrue("close must wait for the checked-out item", closer.isAlive());
            releaseItem.countDown();
            getter.join(TimeUnit.SECONDS.toMillis(30));
            closer.join(TimeUnit.SECONDS.toMillis(30));

            assertFalse("getter must finish", getter.isAlive());
            assertFalse("closer must finish", closer.isAlive());
            assertNull(failure.get());
            assertEquals(List.of(acquired.get()), checkedOut.get());
            assertFalse(pool.isRegistered(acquired.get()));
            IllegalStateException ex = expectThrows(IllegalStateException.class, pool::getAndLock);
            assertEquals("LockablePool is already closed", ex.getMessage());
        } finally {
            allowCreation.countDown();
            releaseItem.countDown();
            getter.join(TimeUnit.SECONDS.toMillis(30));
            closer.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    // --- Tests for filtered getAndLock with rejection ---

    public void testFilteredGetAndLockReturnsCompatibleItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        LockableEntry result = pool.getAndLock(e -> true);
        assertSame(item, result);
        pool.releaseAndUnlock(result);
    }

    public void testFilteredGetAndLockRejectsIncompatibleItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        // Reject the existing item — predicate fails, pool creates new via supplier
        LockableEntry result = pool.getAndLock(e -> !e.id.equals(item.id));
        assertNotSame(item, result);
        pool.releaseAndUnlock(result);
    }

    public void testCheckoutAllIncludesRejectedItems() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        // Reject the existing item
        LockableEntry fresh = pool.getAndLock(e -> !e.id.equals(item.id));
        pool.releaseAndUnlock(fresh);

        List<LockableEntry> all = pool.checkoutAll();
        assertEquals(2, all.size());
        assertTrue(all.contains(item));
        assertTrue(all.contains(fresh));
    }

    public void testRejectedItemNotReturnedBySubsequentPoll() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        // Reject it
        LockableEntry fresh = pool.getAndLock(e -> !e.id.equals(item.id));
        pool.releaseAndUnlock(fresh);

        // Next poll should return fresh, not the rejected item
        LockableEntry next = pool.getAndLock();
        assertSame(fresh, next);
        pool.releaseAndUnlock(next);
    }

    // --- Tests for evaluateAllAndLock ---

    public void testEvaluateAllAndLockReturnsCloseableForMatchingItem() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        Closeable releasable = pool.evaluateAllAndLock(e -> e.id.equals(item.id));
        assertNotNull(releasable);
        assertTrue(item.isHeldByCurrentThread());
        releasable.close();
        assertFalse(item.isHeldByCurrentThread());
    }

    public void testEvaluateAllAndLockReturnsNullWhenNoMatch() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        Closeable releasable = pool.evaluateAllAndLock(e -> false);
        assertNull(releasable);
        assertFalse(item.isHeldByCurrentThread());
    }

    public void testEvaluateAllAndLockReturnsNullOnEmptyPool() {
        LockablePool<LockableEntry> pool = createPool();
        Closeable releasable = pool.evaluateAllAndLock(e -> true);
        assertNull(releasable);
    }

    public void testEvaluateAllAndLockThrowsWhenPoolClosed() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        pool.close();

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> pool.evaluateAllAndLock(e -> true));
        assertEquals("LockablePool is already closed", ex.getMessage());
    }

    public void testEvaluateAllAndLockUnlocksItemRemovedAfterLock() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        // Checkout the item so it becomes unregistered, then evaluate
        List<LockableEntry> checked = pool.checkoutAll();
        assertEquals(1, checked.size());

        // Pool is now empty — item unregistered
        Closeable releasable = pool.evaluateAllAndLock(e -> true);
        assertNull(releasable);
    }

    public void testEvaluateAllAndLockMatchesFirstCompatibleItem() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry first = pool.getAndLock();
        pool.releaseAndUnlock(first);
        LockableEntry second = pool.getAndLock(e -> !e.id.equals(first.id));
        pool.releaseAndUnlock(second);

        Closeable releasable = pool.evaluateAllAndLock(e -> e.id.equals(second.id));
        assertNotNull(releasable);
        assertTrue(second.isHeldByCurrentThread());
        assertFalse(first.isHeldByCurrentThread());
        releasable.close();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                throw new AssertionError("timed out waiting for test latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

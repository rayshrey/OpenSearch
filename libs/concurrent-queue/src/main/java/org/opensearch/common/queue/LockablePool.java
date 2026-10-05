/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.opensearch.common.lease.Releasable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A thread-safe pool of {@link Lockable} items backed by a {@link LockableConcurrentQueue}.
 * Items are locked on checkout and unlocked on release, ensuring safe reuse across threads.
 * <p>
 * The pool supports two modes of checkout:
 * <ul>
 *   <li>{@link #getAndLock()} — returns any available item, or creates a new one</li>
 *   <li>{@link #getAndLock(Predicate)} — returns a compatible item (per predicate),
 *       rejecting incompatible ones. Rejected items are removed from the available queue
 *       but remain tracked by the pool and included in {@link #checkoutAll()}.</li>
 * </ul>
 *
 * @param <T> the pooled item type, must implement {@link Lockable} and retain {@link Object}'s identity-based
 *            {@code equals} and {@code hashCode} implementations
 */
public final class LockablePool<T extends Lockable> implements Iterable<T>, Closeable {

    private final Set<T> items;
    private final LockableConcurrentQueue<T> availableItems;
    private final Supplier<T> itemSupplier;
    private volatile boolean closed;

    /**
     * Creates a new pool.
     *
     * @param itemSupplier  factory for creating new items when the pool is empty
     * @param queueSupplier supplier for the underlying queue instances
     * @param concurrency   the concurrency level (number of stripes)
     */
    public LockablePool(Supplier<T> itemSupplier, Supplier<Queue<T>> queueSupplier, int concurrency) {
        // Lockables are validated once at creation to retain Object identity equality. The concurrent set then gives
        // isRegistered a lock-free, allocation-free read; mutations stay under the pool monitor.
        this.items = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.itemSupplier = Objects.requireNonNull(itemSupplier, "itemSupplier must not be null");
        this.availableItems = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Locks and polls an item from the pool, or creates a new one if none are available.
     *
     * @return a locked item
     * @throws IllegalStateException if the pool is closed
     */
    public T getAndLock() {
        return getAndLock(e -> true);
    }

    /**
     * Locks and polls a compatible item from the pool in a single pass. Items that fail
     * the predicate are removed from the available queue but remain tracked by the pool.
     * If no compatible item is found, a new one is created using the constructor's item supplier.
     *
     * @param isCompatible predicate to test each polled item
     * @return a locked, compatible item
     * @throws IllegalStateException if the pool is closed
     */
    public T getAndLock(Predicate<T> isCompatible) {
        ensureOpen();
        final T item = availableItems.lockAndPollWithRejects(isCompatible);
        if (item == null) {
            return fetchItem();
        }
        if (closed) {
            item.unlock();
            throw new IllegalStateException("LockablePool is already closed");
        }
        return item;
    }

    /**
     * Evaluates all items in the pool and locks the first one matching the predicate.
     *
     * @param isCompatible predicate to test each item
     * @return a {@link Closeable} that unlocks the item when closed, or null if no match found
     */
    public Closeable evaluateAllAndLock(Predicate<T> isCompatible) {
        ensureOpen();
        for (T item : this) {
            if (isCompatible.test(item)) {
                item.lock();
                if (isRegistered(item)) {
                    return item::unlock;
                }

                item.unlock();
            }
        }

        return null;
    }

    private synchronized T fetchItem() {
        ensureOpen();
        T item = Objects.requireNonNull(itemSupplier.get(), "itemSupplier must not return null");
        ensureIdentityEquality(item);
        item.lock();
        items.add(item);
        return item;
    }

    private static void ensureIdentityEquality(Object item) {
        try {
            final Class<?> type = item.getClass();
            if (type.getMethod("equals", Object.class).getDeclaringClass() != Object.class
                || type.getMethod("hashCode").getDeclaringClass() != Object.class) {
                throw new IllegalArgumentException(
                    "Pooled Lockable type [" + type.getName() + "] must use Object identity equality and hash codes"
                );
            }
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Object equality methods must be available", e);
        }
    }

    /**
     * Releases the given item back to this pool for reuse.
     *
     * @param item the item to release
     */
    public void releaseAndUnlock(T item) {
        assert isRegistered(item) : "Pool doesn't know about this item";
        availableItems.addAndUnlock(item);
    }

    /**
     * Lock and checkout all items from the pool.
     *
     * @return unmodifiable list of all items locked by current thread.
     * @throws IllegalStateException if the pool is closed
     */
    public List<T> checkoutAll() {
        return checkoutAll(t -> {});
    }

    /**
     * Lock and checkout all items from the pool.
     *
     *
     * <p>The callback observes the checked-out items in the same critical section
     * as the removal: no other thread can poll, lock, or operate on them between
     * the time they leave the pool and the time the callback returns. Use this
     * when a side effect (e.g. removing a paired resource from another registry)
     * must be atomic with the pool checkout.
     *
     * @param onCheckout callback invoked with the unmodifiable list of checked-out items
     *                   while every item lock is still held by the current thread.
     *
     * @return unmodifiable list of all items locked by current thread
     * @throws IllegalStateException if the pool is closed
     */
    public List<T> checkoutAll(Consumer<T> onCheckout) {
        final List<T> snapshot;
        synchronized (this) {
            ensureOpen();
            snapshot = List.copyOf(items);
        }
        return checkoutAll(snapshot, onCheckout);
    }

    /**
     * Atomically closes admission and checks out every item registered before closure. A concurrent creator either
     * registers its item before this method snapshots the pool, in which case this method waits for and returns it,
     * or observes the closed state and fails without creating an item.
     *
     * @return unmodifiable list of all checked-out items
     * @throws IllegalStateException if the pool is already closed
     */
    public List<T> closeAndCheckoutAll() {
        final List<T> snapshot;
        synchronized (this) {
            ensureOpen();
            closed = true;
            snapshot = List.copyOf(items);
        }
        return checkoutAll(snapshot, t -> {});
    }

    private List<T> checkoutAll(List<T> snapshot, Consumer<T> onCheckout) {
        List<T> lockedItems = new ArrayList<>();
        List<T> checkedOutItems = new ArrayList<>();
        for (T item : snapshot) {
            item.lock();
            lockedItems.add(item);
        }
        synchronized (this) {
            for (T item : lockedItems) {
                try {
                    if (isRegistered(item) && items.remove(item)) {
                        availableItems.remove(item);
                        onCheckout.accept(item);
                        checkedOutItems.add(item);
                    }
                } finally {
                    item.unlock();
                }
            }
        }
        return Collections.unmodifiableList(checkedOutItems);
    }

    /**
     * Check if an item is part of this pool.
     *
     * @param item item to validate
     * @return true if the item is part of this pool
     */
    public boolean isRegistered(T item) {
        return items.contains(item);
    }

    /**
     * Removes a single already-locked item from the pool. The returned {@link Releasable}
     * unlocks the item when closed, intended for use with try-with-resources. The caller
     * still owns the item itself and is responsible for closing any underlying resources.
     *
     * @param item the locked item to remove
     * @return a releasable whose {@code close()} unlocks the item
     * @throws IllegalArgumentException if the item is not registered in this pool
     */
    public synchronized Releasable checkout(T item) {
        if (items.remove(item) == false) {
            throw new IllegalArgumentException("Item is not registered in this pool");
        }
        availableItems.remove(item);
        return item::unlock;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LockablePool is already closed");
        }
    }

    @Override
    public synchronized Iterator<T> iterator() {
        return List.copyOf(items).iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}

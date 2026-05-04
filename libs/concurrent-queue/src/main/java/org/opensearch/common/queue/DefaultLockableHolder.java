/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import java.util.concurrent.locks.ReentrantLock;

public class DefaultLockableHolder<T> implements Lockable {

    private final T ref;
    private final ReentrantLock lock = new ReentrantLock();

    private DefaultLockableHolder(T ref) {
        this.ref = ref;
    }

    public static <R> DefaultLockableHolder<R> of(R ref) {
        return new DefaultLockableHolder<>(ref);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    public T get() {
        return ref;
    }
}

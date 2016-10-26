/*
 * JBoss, Home of Professional Open Source
 * Copyright 2016 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ce.amq.drain;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;

import org.jboss.ce.amq.drain.tx.TxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class Stats {
    private static final Logger log = LoggerFactory.getLogger(Stats.class);

    private Map<String, Integer> sizes = new ConcurrentSkipListMap<>();
    private Map<String, AtomicInteger> counters = new ConcurrentSkipListMap<>();
    private Map<String, StatsSync> synchs = new ConcurrentSkipListMap<>();

    void setSize(String destination, int size) {
        sizes.put(destination, size);
    }

    void increment(String destination) {
        if (TxUtils.isTxActive()) {
            try {
                StatsSync sync = synchs.get(destination);
                if (sync == null) {
                    sync = new StatsSync(destination);
                    TxUtils.getTransactionManager().getTransaction().registerSynchronization(sync);
                    synchs.put(destination, sync);
                }
                sync.increment();
            } catch (RollbackException | SystemException ignored) {
            }
        } else {
            AtomicInteger x = counters.get(destination);
            if (x == null) {
                x = new AtomicInteger(0);
                counters.put(destination, x);
            }
            x.incrementAndGet();
        }
    }

    public void dumpStats() {
        log.info("A-MQ migration statistics ... ('destination' -> [processed / all])");
        for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
            log.info(String.format("Processing stats: '%s' -> [%s / %s]", entry.getKey(), entry.getValue(), sizes.get(entry.getKey())));
        }
    }

    private class StatsSync implements Synchronization {
        private String destination;
        private final AtomicInteger counter = new AtomicInteger(0);

        public StatsSync(String destination) {
            this.destination = destination;
        }

        private void increment() {
            counter.incrementAndGet();
        }

        public void beforeCompletion() {
        }

        public void afterCompletion(int status) {
            if (status == Status.STATUS_COMMITTED) {
                counters.put(destination, counter);
            }
        }
    }
}

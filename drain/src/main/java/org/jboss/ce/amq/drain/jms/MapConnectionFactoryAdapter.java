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

package org.jboss.ce.amq.drain.jms;

import java.util.HashMap;
import java.util.Map;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public abstract class MapConnectionFactoryAdapter implements ConnectionFactoryAdapter {
    private final Map<String, ConnectionFactory> factories = new HashMap<>();

    protected abstract ConnectionFactory createFactoryInternal(String url) throws Exception;

    public synchronized ConnectionFactory createFactory(String url) throws Exception {
        ConnectionFactory factory = factories.get(url);
        if (factory == null) {
            factory = createFactoryInternal(url);
            factories.put(url, factory);
        }
        return factory;
    }

    protected abstract void shutdown(ConnectionFactory factory) throws JMSException;

    public synchronized void shutdown() throws Exception {
        for (ConnectionFactory factory : factories.values()) {
            shutdown(factory);
        }
        factories.clear();
    }
}

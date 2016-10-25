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
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.jms.pool.XaPooledConnectionFactory;
import org.jboss.ce.amq.drain.tx.TxUtils;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class XAPooledConnectionFactoryAdapter implements ConnectionFactoryAdapter {
    private static XAPooledConnectionFactoryAdapter INSTANCE;

    private final Map<String, XaPooledConnectionFactory> factories = new HashMap<>();

    public synchronized static XAPooledConnectionFactoryAdapter getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new XAPooledConnectionFactoryAdapter();
        }
        return INSTANCE;
    }

    public synchronized ConnectionFactory createFactory(String url) throws Exception {
        // we don't need pool explicity, but it already has all the XA code needed
        XaPooledConnectionFactory factory = factories.get(url);
        if (factory == null) {
            factory = new XaPooledConnectionFactory();
            factory.setTransactionManager(TxUtils.getTransactionManager());
            factory.setConnectionFactory(new XAConnectionFactoryOnly(new ActiveMQXAConnectionFactory(url)));
            factories.put(url, factory);
        }
        return factory;
    }

    public synchronized void shutdown() {
        for (XaPooledConnectionFactory factory : factories.values()) {
            factory.stop();
        }
        factories.clear();
    }

    static class XAConnectionFactoryOnly implements XAConnectionFactory {
        private final XAConnectionFactory connectionFactory;

        XAConnectionFactoryOnly(XAConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        public XAConnection createXAConnection() throws JMSException {
            return connectionFactory.createXAConnection();
        }

        public XAConnection createXAConnection(String username, String password) throws JMSException {
            return connectionFactory.createXAConnection(username, password);
        }
    }
}

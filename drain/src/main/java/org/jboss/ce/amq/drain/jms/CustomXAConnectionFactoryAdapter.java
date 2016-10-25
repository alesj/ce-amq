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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XASession;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQXAConnection;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.IdGenerator;
import org.jboss.ce.amq.drain.tx.TxUtils;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class CustomXAConnectionFactoryAdapter implements ConnectionFactoryAdapter {
    public static final ConnectionFactoryAdapter INSTANCE = new CustomXAConnectionFactoryAdapter();

    public synchronized ConnectionFactory createFactory(String url) throws Exception {
        return new CustomActiveMQXAConnectionFactory(url);
    }

    private static class CustomActiveMQXAConnectionFactory extends ActiveMQXAConnectionFactory {
        public CustomActiveMQXAConnectionFactory() {
        }

        public CustomActiveMQXAConnectionFactory(String brokerURL) {
            super(brokerURL);
        }

        @Override
        protected ActiveMQConnection createActiveMQConnection(Transport transport, JMSStatsImpl stats) throws Exception {
            ActiveMQXAConnection connection = new CustomActiveMQXAConnection(transport, getClientIdGenerator(), getConnectionIdGenerator(), stats);
            connection.setXaAckMode(xaAckMode);
            return connection;
        }
    }

    private static class CustomActiveMQXAConnection extends ActiveMQXAConnection {
        public CustomActiveMQXAConnection(Transport transport, IdGenerator clientIdGenerator, IdGenerator connectionIdGenerator, JMSStatsImpl factoryStats) throws Exception {
            super(transport, clientIdGenerator, connectionIdGenerator, factoryStats);
        }

        @Override
        public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
            try {
                Session session = super.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                XASession xaSession = (XASession) session;
                TxUtils.getTransactionManager().getTransaction().enlistResource(xaSession.getXAResource());
                return session;
            } catch (RollbackException | SystemException e) {
                throw new JMSException(e.getMessage());
            }
        }

        @Override
        public void cleanup() throws JMSException {
            close(); // just call close atm -- as no pool yet

            // TODO -- cleanup + reset clientID on the broker
        }
    }
}

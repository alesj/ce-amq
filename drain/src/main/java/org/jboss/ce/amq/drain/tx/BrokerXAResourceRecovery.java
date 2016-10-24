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

package org.jboss.ce.amq.drain.tx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.SQLException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.transaction.xa.XAResource;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class BrokerXAResourceRecovery implements XAResourceRecovery {
    private String url;
    private String username;
    private String password;

    private boolean checked;

    public BrokerXAResourceRecovery(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public XAResource getXAResource() throws SQLException {
        return (XAResource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{XAResource.class}, new XAResourceProxyHandler());
    }

    public boolean initialise(String p) throws SQLException {
        return true;
    }

    public synchronized boolean hasMoreResources() {
        if (!checked) {
            checked = true;
            return true;
        } else {
            return false;
        }
    }

    private class XAResourceProxyHandler implements InvocationHandler {
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                ConnectionFactory cf = TxUtils.createXAConnectionFactory(url);
                Connection connection = (username != null && password != null) ? cf.createConnection(username, password) : cf.createConnection();
                try {
                    if (connection instanceof XAConnection) {
                        XAResource target = XAConnection.class.cast(connection).createXASession().getXAResource();
                        return method.invoke(target, args);
                    } else {
                        throw new IllegalArgumentException("No XA resource available?!");
                    }
                } finally {
                    connection.close();
                }
            } catch (JMSException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

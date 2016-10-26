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
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.transaction.xa.XAResource;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;
import org.jboss.ce.amq.drain.BrokerConfig;
import org.jboss.ce.amq.drain.Utils;
import org.jboss.ce.amq.drain.jms.ConnectionFactoryAdapterFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class BrokerXAResourceRecovery implements XAResourceRecovery {
    private static final Logger log = Logger.getLogger(BrokerXAResourceRecovery.class.getName());

    private String localUrl;
    private String username;
    private String password;

    private final BrokerConfig remoteBroker;

    private int index;
    private List<String> urls;
    private int port = Integer.parseInt(Utils.getSystemPropertyOrEnvVar("amq.tcp.port", "61616"));

    public BrokerXAResourceRecovery(BrokerConfig localBroker, BrokerConfig remoteBroker) {
        this.localUrl = localBroker.getUrl();
        this.username = localBroker.getUsername();
        this.password = localBroker.getPassword();
        this.remoteBroker = remoteBroker;
    }

    public synchronized XAResource getXAResource() throws SQLException {
        String url = urls.get(index++);
        return (XAResource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{XAResource.class}, new XAResourceProxyHandler(url));
    }

    public boolean initialise(String p) throws SQLException {
        return true;
    }

    public synchronized boolean hasMoreResources() {
        if (urls == null) {
            urls = new ArrayList<>();
            urls.add(localUrl);
            fillUrls();
        }
        if (index < urls.size()) {
            return true;
        } else {
            // reset
            urls = null;
            index = 0;
            return false;
        }
    }

    private void fillUrls() {
        String serviceName = Utils.getSystemPropertyOrEnvVar("amq.headless.service");
        if (serviceName == null) {
            String appName = Utils.getSystemPropertyOrEnvVar("application.name");
            if (appName == null) {
                log.warning("No application.name var found, using remote broker for testing!");
                urls.add(remoteBroker.getUrl()); // for testing purposes
            } else {
                serviceName = Utils.getSystemPropertyOrEnvVar(appName + ".amq.headless");
            }
        }
        try {
            InetAddress[] ias = InetAddress.getAllByName(serviceName);
            for (InetAddress ia : ias) {
                urls.add(ia.getHostAddress() + ":" + port); // TODO -- port!?
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private class XAResourceProxyHandler implements InvocationHandler {
        private String url;

        public XAResourceProxyHandler(String url) {
            this.url = url;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                ConnectionFactory cf = ConnectionFactoryAdapterFactory.createXA().createFactory(url);
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

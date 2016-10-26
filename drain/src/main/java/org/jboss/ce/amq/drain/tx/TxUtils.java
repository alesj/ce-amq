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

import java.util.Collections;
import java.util.List;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.RecoveryEnvironmentBean;
import com.arjuna.ats.arjuna.recovery.RecoveryManager;
import com.arjuna.ats.jta.common.JTAEnvironmentBean;
import com.arjuna.ats.jta.recovery.XAResourceRecovery;
import com.arjuna.common.internal.util.propertyservice.BeanPopulator;
import org.jboss.ce.amq.drain.BrokerConfig;
import org.jboss.ce.amq.drain.Utils;
import org.jboss.ce.amq.drain.jms.ConnectionFactoryAdapterFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class TxUtils {

    public static void init(BrokerConfig localBroker, BrokerConfig remoteBroke) {
        BeanPopulator.getDefaultInstance(ObjectStoreEnvironmentBean.class).setObjectStoreDir(Utils.getSystemPropertyOrEnvVar("recovery.store.dir", "/opt/amq/data/recovery"));
        RecoveryManager.delayRecoveryManagerThread();
        BeanPopulator.getDefaultInstance(RecoveryEnvironmentBean.class).setRecoveryBackoffPeriod(1);

        List<XAResourceRecovery> recoveryList = Collections.<XAResourceRecovery>singletonList(new BrokerXAResourceRecovery(localBroker, remoteBroke));
        BeanPopulator.getDefaultInstance(JTAEnvironmentBean.class).setXaResourceRecoveries(recoveryList);
    }

    public static TransactionManager getTransactionManager() {
        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }

    public static boolean isTxActive() {
        try {
            return (getTransactionManager().getStatus() == Status.STATUS_ACTIVE);
        } catch (SystemException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void begin() throws Exception {
        getTransactionManager().begin();
    }

    public static void commit() throws Exception {
        getTransactionManager().commit();
    }

    public static void end() throws Exception {
        if (isTxActive()) {
            getTransactionManager().rollback();
        }
    }

    public synchronized static void shutdown() throws Exception {
        ConnectionFactoryAdapterFactory.shutdown();
    }
}

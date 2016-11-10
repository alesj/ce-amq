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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.network.NetworkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class BrokerServiceDrainer implements Drainer {
    private static final Logger log = LoggerFactory.getLogger(BrokerServiceDrainer.class);

    private static final String MESH_URL_FORMAT = "kube://%s:61616/?transportType=tcp";

    public void validate(String[] args) throws Exception {
        String dataDir = Utils.getSystemPropertyOrEnvVar("activemq.data");
        if (dataDir == null && (args == null || args.length == 0)) {
            throw new IllegalArgumentException("Missing ActiveMQ data directory!");
        }
    }

    public void run(String[] args) throws Exception {
        BrokerService broker = new BrokerService();
        broker.setAdvisorySupport(false);
        broker.setBrokerName(Utils.getBrokerName());
        broker.setUseJmx(true);

        String dataDir = Utils.getSystemPropertyOrEnvVar("activemq.data");
        if (dataDir == null) {
            dataDir = args[0];
        }
        broker.setDataDirectory(dataDir);

        // the broker could also be created from the xml config... to ensure match with any journal options etc.
        // that would be important if the journal is non default.
        // xbean:file://<path to broker xml>
        // broker = BrokerFactory.createBroker(new URI("xbean:broker-xml-from-classpath"));

        String meshServiceName = Utils.getSystemPropertyOrEnvVar("amq.mesh.service.name");
        if (meshServiceName == null) {
            meshServiceName = Utils.getApplicationName() + "-amq-tcp";
        }
        String meshURL = String.format(MESH_URL_FORMAT, meshServiceName);

        // programmatically add the draining bridge, depends on the mesh url only (could be in the xml config either)
        NetworkConnector drainingNetworkConnector = broker.addNetworkConnector(meshURL);
        drainingNetworkConnector.setUserName(Utils.getUsername());
        drainingNetworkConnector.setPassword(Utils.getPassword());
        drainingNetworkConnector.setMessageTTL(-1);
        drainingNetworkConnector.setConsumerTTL(1);
        drainingNetworkConnector.setStaticBridge(true);
        drainingNetworkConnector.setStaticallyIncludedDestinations(Arrays.asList(new ActiveMQDestination[]{new ActiveMQQueue("*")}));

        broker.start();
        broker.waitUntilStarted();

        long msgs;
        while ((msgs = broker.getAdminView().getTotalMessageCount()) > 0) {
            log.info(String.format("Still %s msgs left to migrate ...", msgs));
            TimeUnit.SECONDS.sleep(5);
        }

        broker.stop();

        log.info("-- [CE] A-MQ migration finished. --");
    }
}

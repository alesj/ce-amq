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

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class SrvBrokerFinder implements BrokerFinder {
    public Set<String> find(String serviceName) throws Exception {
        Set<String> urls = new HashSet<>();
        for (DnsRecord record : getDnsRecords("_tcp._tcp." + serviceName)) {
            urls.add(record.getHost() + ":" + record.getPort());
        }
        return urls;
    }

    private Set<DnsRecord> getDnsRecords(String serviceName) throws Exception {
        Set<DnsRecord> dnsRecords = new TreeSet<>();
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        env.put(Context.PROVIDER_URL, "dns:");
        env.put("com.sun.jndi.dns.recursion", "false");
        // default is one second, but os skydns can be slow
        env.put("com.sun.jndi.dns.timeout.initial", "2000");
        // retries handled by DnsPing
        //env.put("com.sun.jndi.dns.timeout.retries", "4");
        DirContext ctx = new InitialDirContext(env);
        Attributes attrs = ctx.getAttributes(serviceName, new String[]{"SRV"});
        NamingEnumeration<?> servers = attrs.get("SRV").getAll();
        while (servers.hasMore()) {
            DnsRecord record = DnsRecord.fromString((String) servers.next());
            dnsRecords.add(record);
        }
        return dnsRecords;
    }

    private static class DnsRecord implements Comparable<DnsRecord> {
        private static final Logger log = Logger.getLogger(DnsRecord.class.getName());

        private final int priority;
        private final int weight;
        private final int port;
        private final String host;

        public DnsRecord(int priority, int weight, int port, String host) {
            this.priority = priority;
            this.weight = weight;
            this.port = port;
            this.host = host.replaceAll("\\.$", "");
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Created %s", this));
            }
        }

        public int getPriority() {
            return priority;
        }

        public int getWeight() {
            return weight;
        }

        public int getPort() {
            return port;
        }

        public String getHost() {
            return host;
        }

        public static DnsRecord fromString(String input) {
            if (log.isLoggable(Level.FINE)) {
                log.fine(String.format("Creating DnsRecord from [%s]", input));
            }
            String[] splitted = input.split(" ");
            return new DnsRecord(
                Integer.parseInt(splitted[0]),
                Integer.parseInt(splitted[1]),
                Integer.parseInt(splitted[2]),
                splitted[3]
            );
        }

        @Override
        public String toString() {
            return "DnsRecord{" +
                "priority=" + priority +
                ", weight=" + weight +
                ", port=" + port +
                ", host='" + host + '\'' +
                '}';
        }

        @Override
        public int compareTo(DnsRecord o) {
            if (getPriority() < o.getPriority()) {
                return -1;
            } else {
                return 1;
            }
        }

    }
}

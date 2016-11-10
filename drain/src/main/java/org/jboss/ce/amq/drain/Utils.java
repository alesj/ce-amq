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

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Utils {
    private static String checkForNone(String value) {
        return "__none".equalsIgnoreCase(value) ? null : value;
    }

    private static String getSystemPropertyOrEnvVar(String systemPropertyName, String envVarName, String defaultValue) {
        String answer = System.getProperty(systemPropertyName);
        if (answer != null) {
            return checkForNone(answer);
        }

        answer = System.getenv(envVarName);
        if (answer != null) {
            return checkForNone(answer);
        }

        return checkForNone(defaultValue);
    }

    private static String convertSystemPropertyNameToEnvVar(String systemPropertyName) {
        return systemPropertyName.toUpperCase().replaceAll("[.-]", "_");
    }

    public static String getSystemPropertyOrEnvVar(String key) {
        return getSystemPropertyOrEnvVar(key, null);
    }

    public static String getSystemPropertyOrEnvVar(String key, String defaultValue) {
        return getSystemPropertyOrEnvVar(key, convertSystemPropertyNameToEnvVar(key), defaultValue);
    }

    public static boolean useBrokerDrainer() {
        return Boolean.getBoolean(getSystemPropertyOrEnvVar("broker.drainer", "false"));
    }

    public static String getBrokerName() {
        return Utils.getSystemPropertyOrEnvVar("broker.name", Utils.getSystemPropertyOrEnvVar("hostname", "localhost"));
    }

    public static String getApplicationName() {
        return getSystemPropertyOrEnvVar("application.name");
    }

    public static String getUsername() {
        return getSystemPropertyOrEnvVar("amq.user");
    }

    public static String getPassword() {
        return getSystemPropertyOrEnvVar("amq.password");
    }
}

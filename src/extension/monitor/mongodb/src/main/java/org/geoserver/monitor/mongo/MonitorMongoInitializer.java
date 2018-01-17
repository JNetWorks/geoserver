/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.monitor.mongo;

import org.geoserver.config.GeoServer;
import org.geoserver.config.GeoServerInitializer;
import org.geoserver.monitor.Monitor;
import org.geotools.util.logging.Logging;

import java.util.logging.Logger;

public class MonitorMongoInitializer implements GeoServerInitializer {

    static Logger LOGGER = Logging.getLogger(Monitor.class);

    Monitor monitor;

    public MonitorMongoInitializer(Monitor monitor) {
        this.monitor = monitor;
    }

    public void initialize(GeoServer geoServer) throws Exception {
        if (!monitor.isEnabled())
            return;

        LOGGER.info("Monitor Mongo extension enabled");
    }

}

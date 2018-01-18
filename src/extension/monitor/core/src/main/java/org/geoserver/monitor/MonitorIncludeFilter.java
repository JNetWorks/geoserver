/* (c) 2014 - 2016 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.monitor;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

public class MonitorIncludeFilter extends AbstractMonitorFilter {

    List<RequestFilter> requestFilters;

    public MonitorIncludeFilter(Monitor monitor, List<RequestFilter> requestFilters) {
        super(monitor);
        this.requestFilters = requestFilters;
    }


    @Override
    boolean monitorRequest(HttpServletRequest request) throws IOException {
        for (RequestFilter requestFilter : requestFilters) {
            if (requestFilter.filter(request)) {
                return true;
            }
        }

        return false;
    }
}

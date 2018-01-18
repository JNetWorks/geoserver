package org.geoserver.monitor;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Basic interface for classes that filters requests for monitoring.
 */
public interface RequestFilter {

    /**
     * Whether or not request parameters match some implemented condition.
     *
     * @param req {@link HttpServletRequest} request
     * @return true if matches.
     */
    boolean filter(HttpServletRequest req) throws IOException;
}

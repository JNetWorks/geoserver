package org.geoserver.monitor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.geoserver.platform.FileWatcher;
import org.geoserver.platform.GeoServerResourceLoader;
import org.geoserver.platform.resource.Paths;
import org.geoserver.platform.resource.Resource;
import org.geoserver.util.IOUtils;
import org.springframework.util.AntPathMatcher;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * Filters requests and responses for monitoring based on configuration, described in filter.json file.
 * The configuration handles chain of filters, that include patterns for path and query parameters matching.
 * <p>
 * {
 * "filters": [
 * {
 * "type": "include",
 * "path": "/**",
 * "query": {
 * "queryKey1":"(.+)",
 * "queryKey2":"(.+)"
 * }
 * },
 * {
 * "type": "exclude",
 * "path": "/rest/monitor/**",
 * "query": {}
 * },
 * ...
 * ]
 * }
 * <p>
 * The request is passed through a chain of filters. By default each request is excluded.
 * Each following filter can include excluded request to monitoring, and exclude included
 * request from monitoring, based on path and query parameters.
 * <p>
 * Request matches to filter, when:
 * 1) Path value is matched with {@link AntPathMatcher}, where pattern is a constructor "path" argument.
 * 2) Each key and pattern from constructor's "query" map matches with at least one provided value of a given query parameter.
 */


public class AdvancedMonitorFilter extends AbstractMonitorFilter {

    final static Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    ConfigFileWatcher watcher;
    ArrayList<Filter> filterChain;

    public AdvancedMonitorFilter(Monitor monitor, GeoServerResourceLoader loader) {
        super(monitor);

        Resource configFile = loader.get(Paths.path("monitoring", "filter.json"));
        if (configFile.getType() == Resource.Type.UNDEFINED) {
            try {
                IOUtils.copy(getClass().getResourceAsStream("filter.json"), configFile.out());
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }

        watcher = new ConfigFileWatcher(configFile);
    }

    /**
     * This method passes request through a chain of filters. By default each request is excluded.
     * Each following filter can include excluded request to monitoring, and exclude included
     * request from monitoring, based on path and query parameters.
     *
     * @param request {@link HttpServletRequest} request
     * @return true if request is included to monitoring, false otherwise.
     */
    @Override
    boolean monitorRequest(HttpServletRequest request) throws IOException {
        if (watcher != null && watcher.isModified()) {
            synchronized (this) {
                if (watcher.isModified()) {
                    filterChain = watcher.read();
                }
            }
        }

        boolean monitor = false;

        for (Filter filter : filterChain) {
            boolean matches = filter.matches(request);
            switch (filter.getType()) {
                case INCLUDE:
                    if (!monitor && matches) {
                        monitor = true;
                    }
                    break;
                case EXCLUDE:
                    if (monitor && matches) {
                        monitor = false;
                    }
                    break;
            }
        }

        return monitor;
    }

    /**
     * Watcher that reacts on changes in resource file and parses it
     * to a list of filters.
     */
    final class ConfigFileWatcher extends FileWatcher<ArrayList<Filter>> {

        private ConfigFileWatcher(Resource resource) {
            super(resource);
        }

        @Override
        protected ArrayList<Filter> parseFileContents(InputStream in) throws IOException {
            Map<String, Object> config = gson.fromJson(new InputStreamReader(in), new TypeToken<Map<String, Object>>() {
            }.getType());

            ArrayList<Filter> filterChain = new ArrayList<>();
            List<Map> filters = (List<Map>) config.get("filters");
            for (Map filter : filters) {
                filterChain.add(Filter.parse(filter));
            }

            return filterChain;
        }
    }

    /**
     * Advanced implementation of a filter that checks whether
     * request matches by path and query parameters.
     */
    static final class Filter {

        enum Type {
            INCLUDE,
            EXCLUDE
        }

        AntPathMatcher       matcher;
        Type                 type;
        String               path;
        Map<String, Pattern> query;

        /**
         * Build Filter instance.
         *
         * @param type  include or exclude request from monitoring on match.
         * @param path  {@link AntPathMatcher} pattern for path matching.
         * @param query a map of query keys and corresponding regex patterns.
         */
        public Filter(Type type, String path, Map<String, Pattern> query) {
            this.type = type;
            this.matcher = new AntPathMatcher();
            this.path = path;
            this.query = new HashMap<>(query);
        }

        /**
         * Parse filter from {@link Map} representation.
         * <p>
         * The map must contain:
         * "path": {@link AntPathMatcher} pattern string
         * "type": string representation of {@link Type}
         * "query": a map of query parameters and corresponding regex patterns (as string)
         *
         * @param filter map representation of filter.
         * @return filter instance.
         */
        public static Filter parse(Map filter) {
            String path = (String) filter.get("path");
            String typeStr = (String) filter.get("type");
            Type type = typeStr == null ? Type.INCLUDE : Type.valueOf(typeStr.toUpperCase());

            HashMap<String, Pattern> query = new HashMap<>();
            Map<String, Object> queryConditions = (Map<String, Object>) filter.get("query");
            if (queryConditions != null) {
                for (Map.Entry<String, Object> entry : queryConditions.entrySet()) {
                    query.put(entry.getKey(), Pattern.compile((String) entry.getValue()));
                }
            }
            return new Filter(type, path, query);
        }

        public Type getType() {
            return type;
        }

        /**
         * Checks whether request matches the filter.
         *
         * @param req request.
         * @return true if path and query matches, false otherwise.
         */
        public boolean matches(HttpServletRequest req) {
            Map<String, String[]> parameterMap = req.getParameterMap();
            String path = req.getServletPath() + req.getPathInfo();

            return matches(path, parameterMap);
        }

        /**
         * Checks whether requested path and request parameters map matches the filter.
         * <p>
         * It matches, when:
         * 1) Path value is matched with {@link AntPathMatcher}, where pattern is a constructor "path" argument.
         * 2) Each key and pattern from constructor's "query" map matches with at least one provided value of a given query parameter.
         *
         * @param pathValue     requested path.
         * @param queryValueMap a map of query keys and corresponding query values (as array).
         * @return true if path and query matches, false otherwise.
         */
        public boolean matches(String pathValue, Map<String, String[]> queryValueMap) {
            if (matcher.match(this.path, pathValue)) {
                for (Map.Entry<String, Pattern> entry : query.entrySet()) {
                    String[] queryValues = queryValueMap.get(entry.getKey());

                    if (queryValues == null || queryValues.length == 0) {
                        return false;
                    }

                    boolean queryValueMatches = false;
                    for (String queryValue : queryValues) {
                        boolean matches = entry.getValue().matcher(queryValue).matches();
                        if (matches) {
                            queryValueMatches = true;
                            break;
                        }
                    }
                    if (!queryValueMatches) {
                        return false;
                    }
                }

                return true;
            }

            return false;
        }
    }
}

package org.geoserver.monitor.mongo;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.geoserver.monitor.RequestData;
import org.geotools.geometry.Envelope2D;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.geometry.BoundingBox;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RequestDataHelper {

    static Logger LOGGER = Logging.getLogger(RequestDataHelper.class);

    public static Document composeContent(RequestData data) {
        Document meta = new Document()
                .append("category", data.getCategory().toString())
                .append("path", data.getPath())
                .append("queryString", data.getQueryString())
                .append("requestStatus", data.getStatus().toString())
                .append("requestContentType", data.getRequestContentType())
                .append("requestLength", data.getResponseLength())
                .append("responseStatus", data.getResponseStatus())
                .append("responseContentType", data.getResponseContentType())
                .append("responseLength", data.getResponseLength())
                .append("httpMethod", data.getHttpMethod())
                .append("startTime", data.getStartTime())
                .append("endTime", data.getEndTime())
                .append("totalTime", data.getTotalTime())
                .append("remoteAddress", data.getRemoteAddr())
                .append("remoteHost", data.getRemoteHost())
                .append("host", data.getHost())
                .append("internalHost", data.getInternalHost())
                .append("remoteUser", data.getRemoteUser())
                .append("remoteUserAgent", data.getRemoteUserAgent())
                .append("remoteCountry", data.getRemoteCountry())
                .append("remoteCity", data.getRemoteCity())
                .append("remoteLat", data.getRemoteLat())
                .append("remoteLon", data.getRemoteLon())
                .append("service", data.getService())
                .append("operation", data.getOperation())
                .append("owsVersion", data.getOwsVersion())
                .append("subOperation", data.getSubOperation())
                .append("resources", data.getResources())
                .append("errorMessage", data.getErrorMessage())
                .append("httpReferer", data.getHttpReferer());

        BoundingBox bbox = data.getBbox();
        if (bbox != null) {
            String crsCode = "EPSG:4326";
            Set<ReferenceIdentifier> identifiers = bbox.getCoordinateReferenceSystem().getIdentifiers();
            if (identifiers.size() > 0) {
                crsCode = identifiers.iterator().next().toString();
            }

            meta.append("bbox", new Document()
                    .append("minX", bbox.getMinX())
                    .append("minY", bbox.getMinY())
                    .append("maxX", bbox.getMaxX())
                    .append("maxY", bbox.getMaxY())
                    .append("crs", crsCode)
            );
        }

        return meta;
    }

    public static Document composeMetadata(ObjectId requestId, MongoMonitorDAO.Type type) {
        return new Document()
                .append("requestId", requestId)
                .append("type", type.toString());
    }

    public static RequestData composeRequestData(Document metadata) {
        RequestData data = new RequestData();

        data.setCategory(RequestData.Category.valueOf(metadata.getString("category")));
        data.setPath(metadata.getString("path"));
        data.setQueryString(metadata.getString("queryString"));
        data.setStatus(RequestData.Status.valueOf(metadata.getString("requestStatus")));
        data.setRequestContentType(metadata.getString("requestContentType"));
        data.setResponseLength(metadata.getLong("requestLength"));
        data.setResponseStatus(metadata.getInteger("responseStatus"));
        data.setResponseContentType(metadata.getString("responseContentType"));
        data.setResponseLength(metadata.getLong("responseLength"));
        data.setHttpMethod(metadata.getString("httpMethod"));
        data.setStartTime(metadata.getDate("startTime"));
        data.setEndTime(metadata.getDate("endTime"));
        data.setTotalTime(metadata.getLong("totalTime"));
        data.setRemoteAddr(metadata.getString("remoteAddress"));
        data.setRemoteHost(metadata.getString("remoteHost"));
        data.setHost(metadata.getString("host"));
        data.setInternalHost(metadata.getString("internalHost"));
        data.setRemoteUser(metadata.getString("remoteUser"));
        data.setRemoteUserAgent(metadata.getString("remoteUserAgent"));
        data.setRemoteCountry(metadata.getString("remoteCountry"));
        data.setRemoteCity(metadata.getString("remoteCity"));
        data.setRemoteLat(metadata.getDouble("remoteLat"));
        data.setRemoteLon(metadata.getDouble("remoteLat"));
        data.setService(metadata.getString("service"));
        data.setOperation(metadata.getString("operation"));
        data.setOwsVersion(metadata.getString("owsVersion"));
        data.setSubOperation(metadata.getString("subOperation"));
        data.setResources((List<String>) metadata.get("resources"));
        data.setErrorMessage(metadata.getString("errorMessage"));
        data.setHttpReferer(metadata.getString("httpReferer"));

        Document bbox = (Document) metadata.get("bbox");
        if (bbox != null && bbox.getString("crs") != null) {
            try {
                CoordinateReferenceSystem crs = CRS.decode(bbox.getString("crs"));
                data.setBbox(new Envelope2D(crs, bbox.getDouble("minX"),
                        bbox.getDouble("minY"),
                        bbox.getDouble("maxX") - bbox.getDouble("minX"),
                        bbox.getDouble("maxY") - bbox.getDouble("minY")
                ));
            } catch (FactoryException e) {
                LOGGER.log(Level.WARNING, "Cannot parse 'crs' attribute: " + bbox.getString("crs") + ", " + e.getMessage(), e);
            }
        }

        return data;
    }

    public static String toJson(RequestData data) {
        Document doc = composeContent(data);

        doc.append("requestBody", new String(data.getRequestBody()));
        doc.append("responseBody", new String(data.getResponseBody()));

        return doc.toJson();
    }
}

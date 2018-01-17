/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.monitor.mongo;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.geoserver.monitor.MonitorConfig;
import org.geoserver.monitor.MonitorConfig.Mode;
import org.geoserver.monitor.MonitorDAO;
import org.geoserver.monitor.PipeliningTaskQueue;
import org.geoserver.monitor.Query;
import org.geoserver.monitor.RequestData;
import org.geoserver.monitor.RequestDataVisitor;
import org.geoserver.util.IOUtils;
import org.geotools.util.logging.Logging;
import org.springframework.beans.factory.DisposableBean;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoMonitorDAO implements MonitorDAO, DisposableBean {

    static Logger LOGGER = Logging.getLogger(MongoMonitorDAO.class);

    public static final int CHUNK_SIZE = 1024;

    public enum Sync {
        SYNC,
        ASYNC,
        ASYNC_UPDATE;
    }

    public enum Type {
        REQUEST,
        RESPONSE;
    }

    PipeliningTaskQueue<Thread> tasks;
    MonitoringDataSource        dataSource;

    Mode mode = Mode.HISTORY;
    Sync sync = Sync.ASYNC;

    public MongoMonitorDAO() {
        setMode(Mode.HISTORY);
        setSync(Sync.ASYNC);
    }

    public MonitoringDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(MonitoringDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getName() {
        return "mongodb";
    }

    @Override
    public void init(MonitorConfig config) {
        setMode(config.getMode());
        setSync(getSync(config));
    }

    public Sync getSync(MonitorConfig config) {
        return Sync.valueOf(config.getProperties().getProperty("mongodb.sync", "async").toUpperCase());
    }

    public void setSync(Sync sync) {
        this.sync = sync;
        if (sync != Sync.SYNC) {
            if (tasks == null) {
                tasks = new PipeliningTaskQueue<Thread>();
                tasks.start();
            }
        } else {
            if (tasks != null) {
                dispose();
            }
        }
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }


    public void saveToGrid(ObjectId id, RequestData data) {
        try {
            Document doc = RequestDataHelper.composeContent(data);
            if (id != null) {
                doc.append("_id", id);
            }
            dataSource.getCollection().insertOne(doc);
            // update id
            id = doc.getObjectId("_id");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Cannot store request data with id='" + String.valueOf(id) + "': " +
                    RequestDataHelper.toJson(data), e);
            return;
        }

        String docIdStr = String.valueOf(id);

        GridFSBucket bucket;
        try {
            bucket = dataSource.createBucket();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Cannot create bucket and store request data with id='" + docIdStr + "': " +
                    RequestDataHelper.toJson(data), e);
            return;
        }

        ObjectId requestBodyId = null;
        if (dataSource.getConfig().getMaxRequestBodySize() != 0) {
            try {
                GridFSUploadOptions requestOptions = new GridFSUploadOptions()
                        .chunkSizeBytes(CHUNK_SIZE)
                        .metadata(RequestDataHelper.composeMetadata(id, Type.REQUEST));
                requestBodyId =
                        bucket.uploadFromStream(docIdStr + "/request", new ByteArrayInputStream(data.getRequestBody()), requestOptions);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE,
                        "Cannot store request with id=" + docIdStr + ": " + RequestDataHelper.toJson(data), e);
            }
        }

        ObjectId responseBodyId = null;
        if (dataSource.getConfig().getMaxResponseBodySize() != 0) {
            try {
                GridFSUploadOptions responseOptions = new GridFSUploadOptions()
                        .chunkSizeBytes(CHUNK_SIZE)
                        .metadata(RequestDataHelper.composeMetadata(id, Type.RESPONSE));
                responseBodyId = bucket.uploadFromStream(docIdStr + "/response", new ByteArrayInputStream(data.getResponseBody()),
                        responseOptions);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE,
                        "Cannot store response with id=" + docIdStr + ": " + RequestDataHelper.toJson(data), e);
            }
        }

        try {
            dataSource.getCollection()
                    .updateOne(new Document("_id", id), new Document("$set", new Document()
                            .append("requestBodyId", requestBodyId)
                            .append("responseBodyId", responseBodyId)
                    ));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Cannot update request data with id='" + String.valueOf(id) + "': " +
                    RequestDataHelper.toJson(data), e);
            return;
        }

        LOGGER.info("Request data stored with id=" + docIdStr);
    }

    public RequestData init(final RequestData data) {
        // Called in the very beginning of the request

        if (mode != Mode.HISTORY) {
            LOGGER.log(Level.WARNING, "init(final RequestData data) operation not supported for mode " + mode.toString());
        } else {
            //don't persist yet, we persist at the very end of request
        }

        return data;
    }

    public void add(RequestData data) {
        // Called after request init for non-history mode

        LOGGER.log(Level.WARNING, "add(RequestData data) operation not supported for mode " + mode.toString());
    }

    public void update(RequestData data) {
        // Called on each update for live mode and on the post-processing stage

        if (mode != Mode.HISTORY) {
            LOGGER.log(Level.WARNING, "update(RequestData data) operation not supported for mode " + mode.toString());
        } else {
            //don't persist yet, we persist at the very end of request
        }
    }

    public void save(RequestData data) {
        // Called once on request complete

        run(new Insert(data));
    }

    public void clear() {
    }

    public void dispose() {
        if (tasks != null) {
            tasks.shutdown();
            tasks = null;
        }
    }

    public List<RequestData> getOwsRequests() {
        throw new UnsupportedOperationException();
    }

    public List<RequestData> getOwsRequests(String service, String operation, String version) {
        throw new UnsupportedOperationException();
    }

    public RequestData getRequest(Object id) {
        MongoCursor<Document> iterator = dataSource.getCollection()
                .find(new Document("_id", new ObjectId((String) id)))
                .limit(1)
                .iterator();

        if (!iterator.hasNext()) {
            return null;
        }

        Document dataContent = iterator.next();
        RequestData data = RequestDataHelper.composeRequestData(dataContent);

//        No need to fetch body content

//        ObjectId requestBodyId = dataContent.getObjectId("requestBodyId");
//        ObjectId responseBodyId = dataContent.getObjectId("responseBodyId");
//
//        GridFSBucket bucket = dataSource.createBucket();
//        if (requestBodyId != null) {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            try (GridFSDownloadStream ds = bucket.openDownloadStream(requestBodyId)) {
//                try {
//                    IOUtils.copy(ds, baos);
//                } catch (IOException e) {
//                    LOGGER.log(Level.WARNING, e.getMessage(), e);
//                }
//            }
//            data.setRequestBody(baos.toByteArray());
//        }
//
//        if (responseBodyId != null) {
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            try (GridFSDownloadStream ds = bucket.openDownloadStream(responseBodyId)) {
//                try {
//                    IOUtils.copy(ds, baos);
//                } catch (IOException e) {
//                    LOGGER.log(Level.WARNING, e.getMessage(), e);
//                }
//            }
//            data.setResponseBody(baos.toByteArray());
//        }

        return data;
    }

    @SuppressWarnings("unchecked")
    public List<RequestData> getRequests() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public List<RequestData> getRequests(Query q) {
        throw new UnsupportedOperationException();
    }

    public void getRequests(Query q, RequestDataVisitor visitor) {
        throw new UnsupportedOperationException();
    }

    public long getCount(Query q) {
        throw new UnsupportedOperationException();
    }

    public Iterator<RequestData> getIterator(Query q) {
        throw new UnsupportedOperationException();
    }

    protected void run(Task task) {
        if (tasks != null) {
            tasks.execute(Thread.currentThread(), new Async(task), task.desc);
        } else {
            task.run();
        }
    }

    class Async implements Runnable {

        Task task;

        Async(Task task) {
            this.task = task;
        }

        public void run() {
            synchronized (task.data) {
                task.run();
            }
        }

    }

    static abstract class Task implements Runnable {

        RequestData data;
        String      desc;

        Task(RequestData data) {
            this.data = data;
        }
    }

    class Save extends Task {
        RequestData data;

        Save(RequestData data) {
            super(data);
            this.data = data;
        }

        public void run() {
            new Insert(data).run();
        }

    }

    class Insert extends Task {

        Insert(RequestData data) {
            super(data);
            this.desc = "Insert " + data.internalid;
        }

        public void run() {
            saveToGrid(null, data);
        }
    }

    @Override
    public void destroy() throws Exception {
        clear();
        dispose();
    }
}

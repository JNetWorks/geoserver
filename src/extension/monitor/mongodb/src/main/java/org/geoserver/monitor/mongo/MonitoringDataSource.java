/* (c) 2014 - 2016 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.monitor.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import org.bson.Document;
import org.geoserver.config.GeoServerDataDirectory;
import org.geoserver.monitor.Monitor;
import org.geoserver.monitor.MonitorConfig;
import org.geoserver.platform.resource.Resource;
import org.geoserver.platform.resource.Resources;
import org.geotools.util.logging.Logging;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class MonitoringDataSource implements DisposableBean, InitializingBean {

    static Logger LOGGER = Logging.getLogger(Monitor.class);

    public static final String MONGODB_URI_PROPERTY        = "mongodb.uri";
    public static final String MONGODB_URI_DEFAULT         = "mongodb://localhost:27017/geoserver";
    public static final String MONGODB_BUCKET_PROPERTY     = "mongodb.bucket";
    public static final String MONGODB_BUCKET_DEFAULT      = "audit";
    public static final String MONGODB_COLLECTION_PROPERTY = "mongodb.collection";
    public static final String MONGODB_COLLECTION_DEFAULT  = "audit";

    public static final String PROPERTY_FILENAME = "db.properties";

    MonitorConfig          config;
    GeoServerDataDirectory dataDirectory;

    String mongoBucketName;
    String mongoCollectionName;
    String mongoUri;

    private MongoClient   mongoClient;
    private MongoDatabase mongoDatabase;

    public void setConfig(MonitorConfig config) {
        this.config = config;
    }

    public MonitorConfig getConfig() {
        return config;
    }

    public void setDataDirectory(GeoServerDataDirectory dataDir) {
        this.dataDirectory = dataDir;
    }

    public GeoServerDataDirectory getDataDirectory() {
        return dataDirectory;
    }

    private MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    public String getMongoBucketName() {
        return mongoBucketName;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initializeDataSource();

        if (mongoUri == null) {
            throw new RuntimeException(
                    "Failed to initialize MongoDB instance. Please provide " + MONGODB_URI_PROPERTY + " property");
        }

        if (mongoBucketName == null) {
            throw new RuntimeException(
                    "Failed to initialize MongoDB instance. Please provide " + MONGODB_BUCKET_PROPERTY + " property");
        }
        MongoClientURI mongoClientURI = new MongoClientURI(mongoUri);
        mongoClient = new MongoClient(mongoClientURI);
        String dbName = mongoClientURI.getDatabase();
        if (dbName == null) {
            throw new RuntimeException("Failed to initialize MongoDB instance. Please provide database name");
        }
        mongoDatabase = mongoClient.getDatabase(dbName);
    }

    @Override
    public void destroy() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private void initializeDataSource() throws Exception {
        Resource monitoringDir = dataDirectory.get("monitoring");
        Resource propertiesRes = monitoringDir.get("db.properties");

        Properties properties;
        if (Resources.exists(propertiesRes)) {
            LOGGER.info("Configuring monitoring database from: " + propertiesRes.path());

            properties = getPropertiesFromResource(propertiesRes);
        } else if (getClass().getResource(PROPERTY_FILENAME) != null) {
            LOGGER.info("Configuring monitoring database from internal resource: " + PROPERTY_FILENAME);

            properties = getPropertiesFromResource(PROPERTY_FILENAME);
        } else {
            LOGGER.info("Configuring monitoring database using default properties");
            properties = getDefaultProperties();
        }

        mongoUri = properties.getProperty(MONGODB_URI_PROPERTY);
        mongoBucketName = properties.getProperty(MONGODB_BUCKET_PROPERTY);
        mongoCollectionName = properties.getProperty(MONGODB_COLLECTION_PROPERTY);
    }

    private Properties getPropertiesFromResource(Resource resource) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = resource.in()) {
            properties.load(in);
        }
        return properties;
    }

    private Properties getPropertiesFromResource(String path) throws IOException {
        Properties properties = new Properties();
        try (InputStream in = getClass().getResourceAsStream(PROPERTY_FILENAME)) {
            properties.load(in);
        }
        return properties;
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.put(MONGODB_URI_PROPERTY, MONGODB_URI_DEFAULT);
        properties.put(MONGODB_BUCKET_PROPERTY, MONGODB_BUCKET_DEFAULT);
        properties.put(MONGODB_COLLECTION_PROPERTY, MONGODB_COLLECTION_DEFAULT);

        return properties;
    }

    public GridFSBucket createBucket() {
        return GridFSBuckets.create(mongoDatabase, mongoBucketName);
    }

    public MongoCollection<Document> getCollection() {
        return mongoDatabase.getCollection(mongoCollectionName);
    }
}
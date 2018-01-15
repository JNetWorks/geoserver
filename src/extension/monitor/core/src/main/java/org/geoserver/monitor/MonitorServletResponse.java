/* (c) 2014 Open Source Geospatial Foundation - all rights reserved
 * (c) 2001 - 2013 OpenPlans
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.monitor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

public class MonitorServletResponse extends HttpServletResponseWrapper {

    /**
     * Don't restrict the maximum length of a response body.
     */
    public static final long BODY_SIZE_UNBOUNDED = -1;

    MonitorOutputStream output;
    int status = 200;

    long maxSize;
    
    public MonitorServletResponse(HttpServletResponse response, long maxSize) {
        super(response);
        this.maxSize = maxSize;
    }

    public byte[] getBodyContent() throws IOException {
        MonitorOutputStream stream = getOutputStream();
        return stream.getData();
    }
    
    public long getContentLength() {
        if (output == null) {
            return 0;
        }
        
        return output.getBytesWritten();
    }
    
    @Override
    public MonitorOutputStream getOutputStream() throws IOException {
        if (output == null) {
            output = new MonitorOutputStream(super.getOutputStream(), maxSize);
        }
        return output;
    }
    
    @Override
    public void setStatus(int sc) {
        this.status = sc;
        super.setStatus(sc);
    }
    
    @Override
    public void setStatus(int sc, String sm) {
        this.status = sc;
        super.setStatus(sc, sm);
    }
    
    public int getStatus() {
        return status;
    }
    

    static class MonitorOutputStream extends ServletOutputStream {

        ByteArrayOutputStream buffer;

        OutputStream delegate;

        long nbytes;

        long maxSize;

        public MonitorOutputStream(OutputStream delegate, long maxSize) {
            this.delegate = delegate;
            this.maxSize = maxSize;
            if (maxSize != 0) {
                buffer = new ByteArrayOutputStream();
            }
        }

        public long getBytesWritten() {
            return nbytes;
        }

        @Override
        public void write(int b) throws IOException {
            fill(b);
            delegate.write(b);
            ++nbytes;
        }

        @Override
        public void write(byte b[]) throws IOException {
            fill(b, 0, b.length);
            delegate.write(b);
            nbytes += b.length;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            fill(b, off, len);
            delegate.write(b, off, len);
            nbytes += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        void fill(byte[] b, int off, int len) {
            if (buffer != null && len >= 0 && !bufferIsFull()) {
                if (maxSize > 0) {
                    long residual = maxSize - buffer.size();
                    len = len < residual ? len : (int) residual;
                }
                buffer.write(b, off, len);
            }
        }

        void fill(int b) {
            if (buffer != null && !bufferIsFull()) {
                buffer.write(b);
            }
        }

        boolean bufferIsFull() {
            return maxSize == 0 || (buffer.size() >= maxSize && maxSize > 0);
        }

        public byte[] getData() {
            return buffer == null ? new byte[0] : buffer.toByteArray();
        }

        public void dispose() {
            buffer = null;
            delegate = null;
        }
    }
}

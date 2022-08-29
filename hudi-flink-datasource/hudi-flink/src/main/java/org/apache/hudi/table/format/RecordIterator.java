package org.apache.hudi.table.format;

import org.apache.flink.table.data.RowData;
import org.apache.hudi.util.RowDataProjection;

import java.io.IOException;

public interface RecordIterator {
    boolean reachedEnd() throws IOException;

    RowData nextRecord();

    void close() throws IOException;

    class ProjectionIterator implements RecordIterator {
        private final RecordIterator iterator;
        private final RowDataProjection projection;

        public ProjectionIterator(RecordIterator iterator, RowDataProjection projection) {
            this.iterator = iterator;
            this.projection = projection;
        }

        @Override
        public boolean reachedEnd() throws IOException {
            return iterator.reachedEnd();
        }

        @Override
        public RowData nextRecord() {
            return projection.project(iterator.nextRecord());
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }
}

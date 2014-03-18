package com.github.cleverage.elasticsearch.jest;

import io.searchbox.BulkableAction;
import io.searchbox.core.Bulk;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.List;

/**
 * @author cgatay
 */
public class JestBulkRequestBuilder implements JestRequest<Bulk>{


    private final List<BulkableAction> actionList;
    private ReplicationType replicationType = ReplicationType.DEFAULT;
    private WriteConsistencyLevel consistencyLevel = WriteConsistencyLevel.DEFAULT;
    private boolean refresh = false;
    private TimeValue timeout = TimeValue.timeValueMinutes(1);

    public JestBulkRequestBuilder() {
        actionList = new ArrayList<>();
    }

    /**
     * Adds an {@link org.elasticsearch.action.index.IndexRequest} to the list of actions to execute. Follows the same behavior of {@link org.elasticsearch.action.index.IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public JestBulkRequestBuilder add(io.searchbox.core.Index request) {
        actionList.add(request);
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public JestBulkRequestBuilder add(JestIndexRequestBuilder request) {
        add(request.getAction());
        return this;
    }

    //TODO
//    /**
//     * Adds an {@link org.elasticsearch.action.delete.DeleteRequest} to the list of actions to execute.
//     */
//    public JestBulkRequestBuilder add(DeleteRequest request) {
//        super.request.add(request);
//        return this;
//    }
//
//    /**
//     * Adds an {@link DeleteRequest} to the list of actions to execute.
//     */
//    public JestBulkRequestBuilder add(DeleteRequestBuilder request) {
//        super.request.add(request.request());
//        return this;
//    }


//    /**
//     * Adds an {@link DeleteRequest} to the list of actions to execute.
//     */
//    public JestBulkRequestBuilder add(UpdateRequest request) {
//        super.request.add(request);
//        return this;
//    }
//
//    /**
//     * Adds an {@link DeleteRequest} to the list of actions to execute.
//     */
//    public JestBulkRequestBuilder add(UpdateRequestBuilder request) {
//        super.request.add(request.request());
//        return this;
//    }

//    /**
//     * Adds a framed data in binary format
//     */
//    public JestBulkRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe) throws Exception {
//        request.add(data, from, length, contentUnsafe, null, null);
//        return this;
//    }
//
//    /**
//     * Adds a framed data in binary format
//     */
//    public JestBulkRequestBuilder add(byte[] data, int from, int length, boolean contentUnsafe, @org.elasticsearch.common.Nullable String defaultIndex, @org.elasticsearch.common.Nullable String defaultType) throws Exception {
//        request.add(data, from, length, contentUnsafe, defaultIndex, defaultType);
//        return this;
//    }

    /**
     * Set the replication type for this operation.
     */
    public JestBulkRequestBuilder setReplicationType(ReplicationType replicationType) {
        this.replicationType = replicationType;
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public JestBulkRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Should a refresh be executed post this bulk operation causing the operations to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public JestBulkRequestBuilder setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final JestBulkRequestBuilder setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to <tt>1m</tt>.
     */
    public final JestBulkRequestBuilder setTimeout(String timeout) {
        this.timeout = TimeValue.parseTimeValue(timeout, null);
        return this;
    }

    /**
     * The number of actions currently in the bulk.
     */
    public int numberOfActions() {
        return actionList.size();
    }

    public Bulk getAction() {
        return new Bulk.Builder().refresh(refresh).addAction(actionList).build();
    }
}

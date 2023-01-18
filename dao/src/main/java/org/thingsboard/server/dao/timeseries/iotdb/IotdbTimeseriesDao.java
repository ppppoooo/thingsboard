package org.thingsboard.server.dao.timeseries.iotdb;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.sqlts.AggregationTimeseriesDao;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;
import org.thingsboard.server.dao.util.IotdbTsDao;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
@IotdbTsDao
public class IotdbTimeseriesDao extends IotdbBaseTimeseriesDao implements AggregationTimeseriesDao, TimeseriesDao {
    protected static final int MIN_AGGREGATION_STEP_MS = 1000;

    protected static final int MAX_SIZE = 1000;

    @Override
    public ListenableFuture<ReadTsKvQueryResult> findAllAsync(TenantId tenantId, EntityId entityId, ReadTsKvQuery query) {
        String sql;
        if (query.getAggregation().equals(Aggregation.NONE)) {
            sql = findAllAsyncWithLimit(tenantId, entityId, query);
        } else {
            sql = findAllAsyncWithAgg(tenantId, entityId, query);
        }
        return Futures.submit(() -> {
            SessionDataSetWrapper sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
            List<TsKvEntry> data = Lists.newArrayList();
            while (sessionDataSetWrapper.hasNext()) {
                RowRecord record = sessionDataSetWrapper.next();
                Field field = record.getFields().get(0);
                BasicKvEntry basicKvEntry = getEntry(query.getKey(),field);
                BasicTsKvEntry tsKvEntry = new BasicTsKvEntry(record.getTimestamp(), basicKvEntry);
                data.add(tsKvEntry);
            }
            return new ReadTsKvQueryResult(query.getId(), data, System.currentTimeMillis());
        }, readResultsProcessingExecutor);

    }

    private String findAllAsyncWithAgg(TenantId tenantId, EntityId entityId, ReadTsKvQuery query) {
        String sql;
        long start = query.getStartTs();
        long end = Math.max(query.getStartTs() + 1, query.getEndTs());
        if(query.getInterval() >0){
            Long step = Math.max(query.getInterval(), MIN_AGGREGATION_STEP_MS);
            start = System.currentTimeMillis();
            end = start + step;
        }

        sql = "select " + transferAgg(query) + " from root.thingsboard.`" + entityId.getId() + "` ";
        if (start != 0 && end != 0) {
            sql = sql + "where time > " + start + " and time < " + end;
        } else if (start != 0) {
            sql = sql + "where time > " + start;
        } else if (end != 0) {
            sql = sql + "where time < " + end;
        }
        return sql;
    }

    private String transferAgg(ReadTsKvQuery query) {
        if (query.getAggregation().equals(Aggregation.MAX)) {
            return " MAX_VALUE(`" + query.getKey() + "`) ";
        }
        if (query.getAggregation().equals(Aggregation.MIN)) {
            return " MIN_VALUE(`" + query.getKey() + "`) ";
        }
        if (query.getAggregation().equals(Aggregation.COUNT)) {
            return " COUNT(`" + query.getKey() + "`) ";
        }
        if (query.getAggregation().equals(Aggregation.AVG)) {
            return " AVG(`" + query.getKey() + "`) ";
        }
        if (query.getAggregation().equals(Aggregation.SUM)) {
            return " SUM(`" + query.getKey() + "`) ";
        }
        return "";
    }

    private String findAllAsyncWithLimit(TenantId tenantId, EntityId entityId, ReadTsKvQuery query) {

        String sql = "select `" + query.getKey()+ "` from root.thingsboard.`" + entityId.getId() + "` ";
        long start = query.getStartTs();
        long end = Math.max(query.getStartTs() + 1, query.getEndTs());

        if(query.getInterval() >0){
            Long step = Math.max(query.getInterval(), MIN_AGGREGATION_STEP_MS);
            start = System.currentTimeMillis();
            end = start + step;
        }

        if (start != 0 && end != 0) {
            sql = sql + "where time > " + start + " and time < " + end;
        } else if (start != 0) {
            sql = sql + "where time > " + start;
        } else if (end != 0) {
            sql = sql + "where time < " + end;
        }

        String order = query.getOrder();
        if (StringUtils.isNoneBlank(order)) {
            sql = sql + " order by time " + order;
        }
        int limit = query.getLimit();
        if (limit == 0) {
            sql = sql + " limit " + MAX_SIZE;
        } else {
            sql = sql + " limit " + limit;
        }
        return sql;

    }

    @Override
    public ListenableFuture<List<ReadTsKvQueryResult>> findAllAsync(TenantId tenantId, EntityId entityId, List<ReadTsKvQuery> queries) {
        return Futures.submit(() -> {
            return queries.stream()
                    .map(query -> {
                        String sql;
                        if (query.getAggregation().equals(Aggregation.NONE)) {
                            sql = findAllAsyncWithLimit(tenantId, entityId, query);
                        } else {
                            sql = findAllAsyncWithAgg(tenantId, entityId, query);
                        }
                        SessionDataSetWrapper sessionDataSetWrapper = null;
                        List<TsKvEntry> data = Lists.newArrayList();
                        try {
                            sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
                            while (sessionDataSetWrapper.hasNext()) {
                                RowRecord record = sessionDataSetWrapper.next();
                                Field field = record.getFields().get(0);
                                BasicKvEntry basicKvEntry = getEntry(query.getKey(),field);
                                BasicTsKvEntry tsKvEntry = new BasicTsKvEntry(record.getTimestamp(), basicKvEntry);
                                data.add(tsKvEntry);
                            }
                        } catch (IoTDBConnectionException e) {
                            e.printStackTrace();
                        } catch (StatementExecutionException e) {
                            e.printStackTrace();
                        }
                        return new ReadTsKvQueryResult(query.getId(), data, System.currentTimeMillis());
                    }).collect(Collectors.toList());
        }, readResultsProcessingExecutor);
    }

    @Override
    public ListenableFuture<Integer> save(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry, long ttl) {
        if (null != tsKvEntry.getJsonValue().orElse(null)) {
            return Futures.immediateFuture(0);
        }
        try {
            myIotdbSessionPool.insertRecord("root.thingsboard." + entityId.getId(), tsKvEntry.getTs()
                    , Lists.newArrayList(tsKvEntry.getKey()), Lists.newArrayList(getType(tsKvEntry)), Lists.newArrayList(tsKvEntry.getValue()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Futures.immediateFuture(1);
    }

    @Override
    public ListenableFuture<Integer> saveAll(TenantId tenantId, EntityId entityId, List<TsKvEntry> tsKvEntries, long ttl) {
        if (CollectionUtils.isEmpty(tsKvEntries)) {
            return Futures.immediateFuture(0);
        }

        List<String> measurements = Lists.newArrayList();
        List<TSDataType> types = Lists.newArrayList();
        List<Object> values = Lists.newArrayList();

        tsKvEntries.stream().filter(t->t!=null).forEach(tsKvEntry -> {
            measurements.add(tsKvEntry.getKey());
            types.add(getType(tsKvEntry));
            values.add(tsKvEntry.getValue());
        });

        if (CollectionUtils.isEmpty(measurements) || CollectionUtils.isEmpty(types) || CollectionUtils.isEmpty(values)) {
            return Futures.immediateFuture(0);
        }
        log.info("save all");
        try {
            myIotdbSessionPool.insertRecord("root.thingsboard." + entityId.getId(), tsKvEntries.get(0).getTs()
                    , Lists.newArrayList(measurements)
                    , Lists.newArrayList(types)
                    , Lists.newArrayList(values));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Futures.immediateFuture(1);
    }

    @Override
    public boolean hasSaveAll(){
        return true;
    }

    private TSDataType getType(TsKvEntry tsKvEntry) {
        if (null != tsKvEntry.getBooleanValue().orElse(null)) {
            return TSDataType.BOOLEAN;
        }
        if (null != tsKvEntry.getLongValue().orElse(null)) {
            return TSDataType.INT64;
        }
        if (null != tsKvEntry.getDoubleValue().orElse(null)) {
            return TSDataType.DOUBLE;
        }
//        if (null != tsKvEntry.getStrValue().orElse(null)) {
//            return TSDataType.TEXT;
//        }
//        if (null != tsKvEntry.getJsonValue().orElse(null)) {
//
//        }
        return TSDataType.TEXT;
    }

    @Override
    public ListenableFuture<Integer> savePartition(TenantId tenantId, EntityId entityId, long tsKvEntryTs, String
            key) {
        return Futures.immediateFuture(0);
    }

    @Override
    public ListenableFuture<Void> remove(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
        return Futures.submit(() -> {
            try {
                myIotdbSessionPool.deleteData(Lists.newArrayList("root.thingsboard.`" + entityId.getId() + "`." + query.getKey()), query.getStartTs(), query.getEndTs());
            } catch (IoTDBConnectionException e) {
                e.printStackTrace();
            } catch (StatementExecutionException e) {
                e.printStackTrace();
            }
        }, readResultsProcessingExecutor);
    }

    @Override
    public void cleanup(long systemTtl) {
        try {
            myIotdbSessionPool.deleteData(Lists.newArrayList("root.thingsboard.**"), systemTtl, System.currentTimeMillis());
        } catch (IoTDBConnectionException e) {
            e.printStackTrace();
        } catch (StatementExecutionException e) {
            e.printStackTrace();
        }
    }
}

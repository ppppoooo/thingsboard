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
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.dao.timeseries.TimeseriesLatestDao;
import org.thingsboard.server.dao.util.IotdbLastestTsDao;

import java.util.List;
import java.util.Optional;

@Component
@Slf4j
@IotdbLastestTsDao
public class IotdbTimeseriesLatestDao extends IotdbBaseTimeseriesDao implements TimeseriesLatestDao {

    @Override
    public ListenableFuture<Optional<TsKvEntry>> findLatestOpt(TenantId tenantId, EntityId entityId, String key) {
        return Futures.submit(() -> {
            String sql = "select last `" + key + "` from root.thingsboard.`" + entityId.getId() + "` ";
            SessionDataSetWrapper sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
            String value = "";
            long time = System.currentTimeMillis();
            BasicKvEntry entry = new StringDataEntry(key, value);
            if (sessionDataSetWrapper.hasNext()) {
                RowRecord record = sessionDataSetWrapper.next();
                Field field = record.getFields().get(1);
                Field fieldType = record.getFields().get(2);
                time = record.getTimestamp();
                entry = getLastEntry(key, field,fieldType);
            }

            return Optional.of(new BasicTsKvEntry(time, entry));
        }, readResultsProcessingExecutor);
    }

    @Override
    public ListenableFuture<TsKvEntry> findLatest(TenantId tenantId, EntityId entityId, String key) {
        return Futures.submit(() -> {
            String sql = "select last `" + key + "` from root.thingsboard.`" + entityId.getId() + "` ";
            SessionDataSetWrapper sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
            String value = "";
            long time = System.currentTimeMillis();
            BasicKvEntry entry = new StringDataEntry(key, value);
            if (sessionDataSetWrapper.hasNext()) {
                RowRecord record = sessionDataSetWrapper.next();
                Field field = record.getFields().get(1);
                Field fieldType = record.getFields().get(2);
                time = record.getTimestamp();
                entry = getLastEntry(key, field,fieldType);
            }
            return new BasicTsKvEntry(time, entry);
        }, readResultsProcessingExecutor);
    }

    @Override
    public ListenableFuture<List<TsKvEntry>> findAllLatest(TenantId tenantId, EntityId entityId) {
        return Futures.submit(() -> {
            String sql = "select last *  from root.thingsboard.`" + entityId.getId() + "` ";
            SessionDataSetWrapper sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
            List<TsKvEntry> list = Lists.newArrayList();
            while (sessionDataSetWrapper.hasNext()) {
                RowRecord record = sessionDataSetWrapper.next();
                Field fieldName = record.getFields().get(0);
                Field field = record.getFields().get(1);
                Field fieldType = record.getFields().get(2);
                String key = fieldName.getStringValue().substring(fieldName.getStringValue().lastIndexOf('.') + 1);
                BasicKvEntry basicKvEntry = getLastEntry(key, field, fieldType);
                BasicTsKvEntry tsKvEntry = new BasicTsKvEntry(record.getTimestamp(), basicKvEntry);
                list.add(tsKvEntry);
            }
            return list;
        }, readResultsProcessingExecutor);
    }

    public BasicKvEntry getLastEntry(String key , Field field, Field fieldType) {
        if(TSDataType.BOOLEAN.name().equals(fieldType.getStringValue())){
            return new BooleanDataEntry(key,Boolean.valueOf(field.getStringValue()));
        }
        if(TSDataType.INT64.name().equals(fieldType.getStringValue())){
            return new LongDataEntry(key,Long.valueOf(field.getStringValue()));
        }
        if(TSDataType.DOUBLE.name().equals(fieldType.getStringValue())){
            return new DoubleDataEntry(key,Double.valueOf(field.getStringValue()));
        }
        if(TSDataType.TEXT.name().equals(fieldType.getStringValue())){
            return new StringDataEntry(key,field.getStringValue());
        }
        return new StringDataEntry(key,field.getStringValue());
    }


    @Override
    public ListenableFuture<Void> saveLatest(TenantId tenantId, EntityId entityId, TsKvEntry tsKvEntry) {
        return Futures.submit(() -> {
            System.out.println("savelastest");
        }, readResultsProcessingExecutor);
    }


    @Override
    public ListenableFuture<TsKvLatestRemovingResult> removeLatest(TenantId tenantId, EntityId entityId, DeleteTsKvQuery query) {
        return Futures.submit(() -> {
            String sql = "select last `" + query.getKey() + "`  from root.thingsboard.`" + entityId.getId() + "` ";
            SessionDataSetWrapper sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
            while (sessionDataSetWrapper.hasNext()) {
                long time = sessionDataSetWrapper.next().getTimestamp();
                myIotdbSessionPool.deleteData("root.thingsboard.`" + entityId.getId() + "`.`" + query.getKey() + "`", time);
            }
            return new TsKvLatestRemovingResult(query.getKey(), true);
        }, readResultsProcessingExecutor);
    }

    @Override
    public List<String> findAllKeysByDeviceProfileId(TenantId tenantId, DeviceProfileId deviceProfileId) {
        return Lists.newArrayList();
    }

    @Override
    public List<String> findAllKeysByEntityIds(TenantId tenantId, List<EntityId> entityIds) {
        List<String> list = Lists.newArrayList();
        entityIds.forEach(entityId -> {
            String sql = "select last *  from root.thingsboard.`" + entityId.getId() + "` ";
            SessionDataSetWrapper sessionDataSetWrapper = null;
            try {
                sessionDataSetWrapper = myIotdbSessionPool.executeQueryStatement(sql);
                while (sessionDataSetWrapper.hasNext()) {
                    RowRecord record = sessionDataSetWrapper.next();
                    String timeseries = record.getFields().get(0).getStringValue();
                    String key = timeseries.substring(timeseries.lastIndexOf('.') + 1);
                    list.add(key);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                e.printStackTrace();
            }
        });
        return list;
    }
}

package org.thingsboard.server.dao.timeseries.iotdb;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.springframework.beans.factory.annotation.Value;
import org.thingsboard.server.common.data.kv.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class IotdbBaseTimeseriesDao {

    protected ExecutorService readResultsProcessingExecutor = Executors.newFixedThreadPool(20);

    public SessionPool myIotdbSessionPool;

    @Value("${iotdb.host}")
    private String host;

    @Value("${iotdb.port}")
    private int port;

    @Value("${iotdb.user}")
    private String user;

    @Value("${iotdb.pw}")
    private String pw;

    @PostConstruct
    public void createPool() {
        myIotdbSessionPool = createSessionPool();
    }

    private SessionPool createSessionPool() {
        return new SessionPool.Builder()
                .host(host)
                .port(port)
                .user(user)
                .password(pw)
                .connectionTimeoutInMs(3000)
                .fetchSize(50)
                .maxSize(100)
                .build();
    }

    @PreDestroy
    public void stop() {
        if (readResultsProcessingExecutor != null) {
            readResultsProcessingExecutor.shutdownNow();
        }

        myIotdbSessionPool.close();
    }

    public BasicKvEntry getEntry(String key , Field field) {
        if(TSDataType.BOOLEAN.equals(field.getDataType())){
            return new BooleanDataEntry(key,field.getBoolV());
        }
        if(TSDataType.INT64.equals(field.getDataType())){
            return new LongDataEntry(key,field.getLongV());
        }
        if(TSDataType.DOUBLE.equals(field.getDataType())){
            return new DoubleDataEntry(key,field.getDoubleV());
        }
        if(TSDataType.TEXT.equals(field.getDataType())){
            return new StringDataEntry(key,field.getStringValue());
        }
        return new StringDataEntry(key,field.getStringValue());
    }

}

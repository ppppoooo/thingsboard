package org.thingsboard.server.service.install;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.thingsboard.server.dao.util.IotdbTsDao;


@Component
@IotdbTsDao
@Profile("install")
public class IotdbTsDatabaseSchemaService implements TsDatabaseSchemaService {

    @Override
    public void createDatabaseSchema() throws Exception {

    }

    @Override
    public void createDatabaseSchema(boolean createIndexes) throws Exception {

    }

    @Override
    public void createDatabaseIndexes() throws Exception {

    }

}

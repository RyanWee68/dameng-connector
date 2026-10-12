package org.devlive.connector.dameng.logminer;

import io.debezium.config.Configuration;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SqlUtilsTest
{
    @Test
    public void miningQueryOrdersChangesAndContinuationFragments()
    {
        DamengConnectorConfig config = new DamengConnectorConfig(Configuration.create()
                .with("database.server.name", "test")
                .build());

        String query = SqlUtils.logMinerContentsQuery(config, "SYSDBA");

        assertTrue(query.contains("SCN > ? AND SCN <= ?"));
        assertTrue(query.contains("OPERATION_CODE IN (7,36)"));
        assertTrue(query.endsWith("ORDER BY SCN, RS_ID, SSN"));
    }
}

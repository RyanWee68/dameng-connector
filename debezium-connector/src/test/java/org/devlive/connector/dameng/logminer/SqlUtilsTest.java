package org.devlive.connector.dameng.logminer;

import io.debezium.config.Configuration;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.Scn;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class SqlUtilsTest
{
    @Test
    public void miningStartUsesCurrentWindowWithoutCommittedOnlyFiltering()
    {
        assertEquals("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => 643190904, ENDSCN => 643210904, "
                        + "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;",
                SqlUtils.startLogMinerStatement(Scn.valueOf(643190904), Scn.valueOf(643210904),
                        DamengConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false));
    }

    @Test
    public void miningStartPreservesEqualAndLongScnBounds()
    {
        Scn scn = Scn.valueOf("4294967296");
        assertEquals("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => 4294967296, ENDSCN => 4294967296, "
                        + "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;",
                SqlUtils.startLogMinerStatement(scn, scn, DamengConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true));
    }

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

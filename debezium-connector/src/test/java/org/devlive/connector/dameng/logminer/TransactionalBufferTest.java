package org.devlive.connector.dameng.logminer;

import io.debezium.config.Configuration;
import io.debezium.util.Clock;
import org.devlive.connector.dameng.DamengConnectorConfig;
import org.devlive.connector.dameng.DamengStreamingChangeEventSourceMetrics;
import org.devlive.connector.dameng.Scn;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TransactionalBufferTest
{
    @Test
    public void zeroTimeoutKeepsUncommittedChangesBuffered() throws InterruptedException
    {
        DamengConnectorConfig config = new DamengConnectorConfig(Configuration.create()
                .with("database.server.name", "test")
                .build());
        DamengStreamingChangeEventSourceMetrics metrics = new DamengStreamingChangeEventSourceMetrics(null, null, null, config);
        TransactionalBuffer buffer = new TransactionalBuffer(null, Clock.system(), null, metrics, config.getAutoCommitTimeoutMs());

        buffer.registerDmlOperation(1, "transaction", Scn.valueOf(1), null, null, Instant.now(), "row");
        Thread.sleep(10);
        buffer.checkAndAutoCommitTransactions(null, null, null);

        assertFalse(buffer.isEmpty());
        assertTrue(buffer.rollback("transaction", "test"));
        assertTrue(buffer.isEmpty());
    }
}

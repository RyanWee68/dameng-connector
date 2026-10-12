package org.devlive.connector.dameng;

import io.debezium.config.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DamengStreamingChangeEventSourceMetricsTest
{
    private DamengStreamingChangeEventSourceMetrics metrics(int min, int initial, int max)
    {
        DamengConnectorConfig config = new DamengConnectorConfig(Configuration.create()
                .with("database.server.name", "window-test")
                .with("log.mining.batch.size.min", min)
                .with("log.mining.batch.size.default", initial)
                .with("log.mining.batch.size.max", max).build());
        return new DamengStreamingChangeEventSourceMetrics(null, null, null, config);
    }

    @Test
    public void doublesForBacklogAndShrinksByConfiguredStepWithinBounds()
    {
        DamengStreamingChangeEventSourceMetrics metrics = metrics(5000, 20000, 100000);
        metrics.changeBatchSize(true);
        assertEquals(40000, metrics.getBatchSize());
        metrics.changeBatchSize(true);
        assertEquals(80000, metrics.getBatchSize());
        metrics.changeBatchSize(true);
        assertEquals(100000, metrics.getBatchSize());
        metrics.changeBatchSize(true);
        assertEquals(100000, metrics.getBatchSize());
        metrics.changeBatchSize(false);
        assertEquals(95000, metrics.getBatchSize());
        metrics.setBatchSize(98000);
        metrics.changeBatchSize(true);
        assertEquals(100000, metrics.getBatchSize());
        metrics.changeBatchSize(true);
        assertEquals(100000, metrics.getBatchSize());
        metrics.setBatchSize(6000);
        metrics.changeBatchSize(false);
        assertEquals(5000, metrics.getBatchSize());
        metrics.changeBatchSize(false);
        assertEquals(5000, metrics.getBatchSize());
    }

    @Test
    public void increasingCannotOverflow()
    {
        DamengStreamingChangeEventSourceMetrics metrics = metrics(5000, 20000, Integer.MAX_VALUE);
        metrics.setBatchSize(Integer.MAX_VALUE - 1);
        metrics.changeBatchSize(true);
        assertEquals(Integer.MAX_VALUE, metrics.getBatchSize());
    }

    @Test
    public void fixedWindowDoesNotChange()
    {
        DamengStreamingChangeEventSourceMetrics metrics = metrics(20000, 20000, 20000);
        metrics.changeBatchSize(true);
        assertEquals(20000, metrics.getBatchSize());
        metrics.changeBatchSize(false);
        assertEquals(20000, metrics.getBatchSize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidWindowBounds()
    {
        metrics(5000, 20000, 10000).validateBatchSizeBounds();
    }
}

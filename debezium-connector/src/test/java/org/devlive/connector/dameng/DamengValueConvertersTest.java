/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DamengValueConvertersTest
{
    @Test
    public void detectsLogMinerNullLiteral()
    {
        assertTrue(DamengValueConverters.isNullLiteral("NULL"));
        assertTrue(DamengValueConverters.isNullLiteral(" null "));
        assertFalse(DamengValueConverters.isNullLiteral("'NULL'"));
        assertFalse(DamengValueConverters.isNullLiteral("0"));
        assertFalse(DamengValueConverters.isNullLiteral(null));
    }
}

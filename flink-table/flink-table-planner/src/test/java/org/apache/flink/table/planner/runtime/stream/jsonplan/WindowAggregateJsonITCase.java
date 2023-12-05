/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Test for window aggregate json plan. */
@ExtendWith(ParameterizedTestExtension.class)
class WindowAggregateJsonITCase extends JsonPlanTestBase {

    @Parameters(name = "agg_phase = {0}")
    private static Object[] parameters() {
        return new Object[][] {
            // new Object[] {AggregatePhaseStrategy.ONE_PHASE},
            new Object[] {AggregatePhaseStrategy.TWO_PHASE}
        };
    }

    @Parameter private AggregatePhaseStrategy aggPhase;

    static final Row[] INPUT_DATA = {
            Row.of("2020-10-10 00:00:01", 1, 1d, 1f, new BigDecimal("1.11"), "Hi", "a"),
            Row.of("2020-10-10 00:00:34", 1, 3d, 3f, new BigDecimal("3.33"), "Comment#3", "b"),
            Row.of("2020-10-10 00:00:41", 10, 3d, 3f, new BigDecimal("4.44"), "Comment#4", "c"),
            Row.of("2020-10-10 00:00:42", 11, 4d, 4f, new BigDecimal("5.44"), "Comment#5", "d")
    };

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.setup();
        createTestValuesSourceTable(
                "MyTable",
                Arrays.asList(INPUT_DATA),
                new String[] {
                    "ts STRING",
                    "`int` INT",
                    "`double` DOUBLE",
                    "`float` FLOAT",
                    "`bigdec` DECIMAL(10, 2)",
                    "`string` STRING",
                    "`name` STRING",
                    "`rowtime` AS TO_TIMESTAMP(`ts`)",
                    "WATERMARK for `rowtime` AS `rowtime` - INTERVAL '1' SECOND",
                },
                new HashMap<String, String>() {
                    {
                        put("enable-watermark-push-down", "true");
                        put("failing-source", "true");
                    }
                });
        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        aggPhase.toString());
    }

    static final String[] COMPLETE_OUTPUT =
            new String[] {
                "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:06]",
                "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:11]",
                "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:16]",
                "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:36]",
                "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:41]",
                "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]",
                "+I[c, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]",
                "+I[d, 4.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]"
            };

    static final String[] OUTPUT_MISSING_RECORD =
            new String[] {
                    "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:06]",
                    "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:11]",
                    "+I[a, 1.0, 1, 2020-10-10T00:00:01, 2020-10-10T00:00:16]",
                    "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:36]",
                    // Missing record: "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:41]",
                    "+I[b, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]",
                    "+I[c, 3.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]",
                    "+I[d, 4.0, 1, 2020-10-10T00:00:31, 2020-10-10T00:00:46]"
            };

    // This test case is flaky for and passes about 90% of the time for me.
    // When it passes, it is showing incorrect behavior since it is missing a record.
    @TestTemplate
    void testDistinctSplitEnabled() throws Exception {
        tableEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "max_double DOUBLE",
                "cnt_distinct_int BIGINT",
                "window_start TIMESTAMP(3) NOT NULL",
                "window_end TIMESTAMP(3) NOT NULL");

        compileSqlAndExecutePlan(
                        "insert into MySink select name, "
                                + "   max(`double`),\n"
                                + "   count(distinct `int`),\n"
                                + "   window_start,\n"
                                + "   window_end\n"
                                + "FROM TABLE ("
                                + "  CUMULATE(\n"
                                + "     TABLE MyTable,\n"
                                + "     DESCRIPTOR(rowtime),\n"
                                + "     INTERVAL '5' SECOND,\n"
                                + "     INTERVAL '15' SECOND, INTERVAL '1' SECOND))"
                                + "GROUP BY name, window_start, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResultsAsStrings("MySink");
        assertResult(Arrays.asList(OUTPUT_MISSING_RECORD), result);
    }

    @TestTemplate
    void testDistinctSplitDisabled() throws Exception {
        tableEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, false);
        createTestValuesSinkTable(
                "MySink",
                "name STRING",
                "max_double DOUBLE",
                "cnt_distinct_int BIGINT",
                "window_start TIMESTAMP(3) NOT NULL",
                "window_end TIMESTAMP(3) NOT NULL");

        compileSqlAndExecutePlan(
                        "insert into MySink select name, "
                                + "   max(`double`),\n"
                                + "   count(distinct `int`),\n"
                                + "   window_start,\n"
                                + "   window_end\n"
                                + "FROM TABLE ("
                                + "  CUMULATE(\n"
                                + "     TABLE MyTable,\n"
                                + "     DESCRIPTOR(rowtime),\n"
                                + "     INTERVAL '5' SECOND,\n"
                                + "     INTERVAL '15' SECOND, INTERVAL '1' SECOND))"
                                + "GROUP BY name, window_start, window_end")
                .await();

        List<String> result = TestValuesTableFactory.getResultsAsStrings("MySink");
        assertResult(Arrays.asList(COMPLETE_OUTPUT), result);
    }
}

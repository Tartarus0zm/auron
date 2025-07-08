/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.blaze.flink.table.runtime;

import static org.blaze.flink.table.planner.delegation.FlinkBlazePlannerFactory.IDENTIFIER;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.junit.Test;

/** Test for blaze calc operator. */
public class FlinkBlazeCalcITCase {

    @Test
    public void testSelect1() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(TableConfigOptions.TABLE_PLANNER_FACTORY_IDENTIFIER, IDENTIFIER);
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));
        tableEnvironment.executeSql("select 1").print();
    }
}

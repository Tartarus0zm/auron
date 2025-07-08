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
package org.blaze.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class FlinkBlazeOptions {

    public static ConfigOption<Integer> FLINK_BLAZE_ARROW_BATCH_SIZE = ConfigOptions.key("flink.blaze.arrow.batch.size")
            .intType()
            .defaultValue(10000)
            .withDescription("The batch size of arrow data.");

    public static ConfigOption<Integer> FLINK_BLAZE_ARROW_MEM_SIZE = ConfigOptions.key("flink.blaze.arrow.mem.size")
            .intType()
            .defaultValue(8388608)
            .withDescription("The memory size of arrow data.");
}

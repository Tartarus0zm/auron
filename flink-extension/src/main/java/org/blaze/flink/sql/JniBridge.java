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

package org.blaze.flink.sql;

import org.apache.flink.configuration.Configuration;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class JniBridge {
    public static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();
    private static Configuration flinkConfiguration;

    public static synchronized void setFlinkConfiguration(Configuration configuration) {
        if (flinkConfiguration == null) {
            flinkConfiguration = configuration;
        } else {
            System.out.println("Flink configuration has already been set.");
        }
    }

    public static native long callNative(long initNativeMemory, BlazeCallNativeWrapper wrapper);

    public static native boolean nextBatch(long ptr);

    public static native void finalizeNative(long ptr);

    public static native void onExit();

    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static void setContextClassLoader(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    public static String getFlinkEnvConfAsString(String key) {
        if (flinkConfiguration != null) {
            return flinkConfiguration.getString(key, null);
        }
        return null;
    }

    public static Object getResource(String key) {
        return resourcesMap.remove(key);
    }

    public static OnHeapSpillManager getTaskOnHeapSpillManager() {
        return OnHeapSpillManager$.MODULE$.current();
    }

    public static boolean isTaskRunning() {
        TaskContext tc = getTaskContext();
        if (tc == null) { // driver is always running
            return true;
        }
        return !tc.isCompleted() && !tc.isInterrupted();
    }

    public static boolean isDriverSide() {
        TaskContext tc = getTaskContext();
        return tc == null;
    }

    public static FSDataInputWrapper openFileAsDataInputWrapper(FileSystem fs, String path) throws Exception {
        // the path is a URI string, so we need to convert it to a URI object, ref:
        // org.apache.spark.paths.SparkPath.toPath
        return FSDataInputWrapper$.MODULE$.wrap(fs.open(new Path(new URI(path))));
    }

    public static FSDataOutputWrapper createFileAsDataOutputWrapper(FileSystem fs, String path) throws Exception {
        return FSDataOutputWrapper$.MODULE$.wrap(fs.create(new Path(new URI(path))));
    }

    private static final List<BufferPoolMXBean> directMXBeans =
            ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

    public static long getTotalMemoryLimited() {
        return NativeHelper$.MODULE$.totalMemory();
    }

    public static long getDirectMemoryUsed() {
        return directMXBeans.stream()
                .mapToLong(BufferPoolMXBean::getTotalCapacity)
                .sum();
    }

    public static String getDirectWriteSpillToDiskFile() {
        return SparkEnv.get()
                .blockManager()
                .diskBlockManager()
                .createTempLocalBlock()
                ._2
                .getPath();
    }
}

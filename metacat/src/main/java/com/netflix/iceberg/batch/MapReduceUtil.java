/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.batch;

import com.netflix.bdp.Committers;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.spark.internal.io.SparkHadoopWriterUtils;
import org.apache.spark.sql.catalyst.InternalRow;

class MapReduceUtil {
  static Job newJob(Configuration conf, String location) {
    try {
      Job job = Job.getInstance(conf);
      job.setOutputKeyClass(Void.class);
      job.setOutputValueClass(InternalRow.class);
      FileOutputFormat.setOutputPath(job, new Path(location));

      return job;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create MapReduce Job");
    }
  }

  static JobID newJobId(long batchId, int stageId) {
    return new JobID(SparkHadoopWriterUtils.createJobTrackerID(new Date(batchId)), stageId);
  }

  static JobContext newJobContext(Configuration conf, long batchId, int stageId) {
    JobID jobId = newJobId(batchId, stageId);
    conf.set("mapreduce.job.id", jobId.toString());
    conf.set("mapred.job.id", jobId.toString());
    return new JobContextImpl(conf, jobId);
  }

  static OutputCommitter newJobCommitter(JobContext jobContext, boolean isPartitioned) {
    return Committers.newBatchCommitter(
        FileOutputFormat.getOutputPath(jobContext), jobContext, isPartitioned);
  }

  static TaskAttemptContext newTaskContext(Configuration conf, long batchId, int stageId, int partitionId, long uniqueTaskId) {
    JobID jobId = newJobId(batchId, stageId);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, partitionId);
    TaskAttemptID taskAttemptId = new TaskAttemptID(taskId, ((int) uniqueTaskId) & Integer.MAX_VALUE);

    conf.set("mapreduce.job.id", jobId.toString());
    conf.set("mapred.job.id", jobId.toString());
    conf.set("mapreduce.task.id", taskId.toString());
    conf.set("mapred.tip.id", taskId.toString());
    conf.set("mapreduce.task.attempt.id", taskAttemptId.toString());
    conf.set("mapred.task.id", taskAttemptId.toString());
    conf.setBoolean("mapreduce.task.ismap", true);
    conf.setBoolean("mapred.task.ismap", true);
    conf.setInt("mapreduce.task.partition", partitionId);
    conf.setInt("mapred.task.partition", 0); // seems odd, but it's always set this way in Spark

    return new TaskAttemptContextImpl(conf, taskAttemptId);
  }

  static FileOutputCommitter newTaskCommitter(TaskAttemptContext attemptContext, boolean isPartitioned) {
    return Committers.newBatchCommitter(
        FileOutputFormat.getOutputPath(attemptContext), attemptContext, isPartitioned);
  }
}

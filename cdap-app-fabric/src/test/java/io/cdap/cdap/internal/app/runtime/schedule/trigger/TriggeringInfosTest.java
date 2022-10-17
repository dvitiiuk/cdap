/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.proto.ArgumentMapping;
import io.cdap.cdap.proto.PluginPropertyMapping;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPipelineId;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TriggeringInfosTest {

  private static TriggerInfo time, program, partition;
  private static String namespace, appName, programName, version, cron;
  private static String triggerMappingString = "{\"arguments\": [],\"pluginProperties\": [{\"pipelineId\": " +
    "{\"namespace\": \"testNameSpace\",\"pipelineName\": \"pipeline\"},\"stageName\": \"File\",\"source\": " +
    "\"sampleSize\",\"target\": \"sample-size-arg\"}]}";

  @BeforeClass
  public static void before() {
    namespace = "testNameSpace";
    appName = "testAppName";
    programName = "testProgram";
    version = "1.0.0";
    cron = "* * * * 1 1";
    time = new DefaultTimeTriggerInfo(cron, 1000000);
    program = new DefaultProgramStatusTriggerInfo(namespace, appName, ProgramType.WORKFLOW, programName,
                                                  RunIds.generate(), ProgramStatus.COMPLETED,
                                                  null, Collections.emptyMap());
    partition = new DefaultPartitionTriggerInfo(namespace, "testDataset", 10, 20);
  }


  @Test
  public void testProgramStatus() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{program}),
                                                                    Collections.emptyMap());

    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.PROGRAM_STATUS, createScheduleId());

    TriggeringInfo progTrigInfo = new TriggeringInfo
      .ProgramStatusTriggeringInfo(createScheduleId(),
                                   Collections.emptyMap(),
                                   new ProgramRunId(namespace, appName,
                                                    io.cdap.cdap.proto.ProgramType.WORKFLOW, programName,
                                                    "678d2401-1dd4-11b2-8ff7-000000dae054"));
    verifyProgramStatusTriggeringInfo((TriggeringInfo.ProgramStatusTriggeringInfo) triggeringInfo,
                                      (TriggeringInfo.ProgramStatusTriggeringInfo) progTrigInfo);
  }

  @Test
  public void testTime() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{time}),
                                                                    Collections.emptyMap());
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.TIME, createScheduleId());
    TriggeringInfo timeTriggeringInfo = new TriggeringInfo
      .TimeTriggeringInfo(createScheduleId(), Collections.emptyMap(), cron);
    Assert.assertEquals(triggeringInfo, timeTriggeringInfo);
  }

  @Test
  public void testPartition() {
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{partition}),
                                                                    Collections.emptyMap());
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.PARTITION, createScheduleId());
    TriggeringInfo partTriggeringInfo = new TriggeringInfo
      .PartitionTriggeringInfo(createScheduleId(), Collections.emptyMap(),
                               "testDataset", namespace, 10, 20);
    Assert.assertEquals(triggeringInfo, partTriggeringInfo);
  }

  @Test
  public void testAnd() {
    Map<String, String> props = new HashMap<>();
    props.put(TriggeringInfos.TRIGGERING_PROPERTIES_MAPPING, triggerMappingString);
    TriggeringScheduleInfo info = new DefaultTriggeringScheduleInfo("name", "desc",
                                                                    Arrays.asList(new TriggerInfo[]{partition, time}),
                                                                    props);
    TriggeringInfo triggeringInfo = TriggeringInfos
      .fromTriggeringScheduleInfo(info, Trigger.Type.AND, createScheduleId());
    TriggeringInfo timeTriggeringInfo = new TriggeringInfo
      .TimeTriggeringInfo(createScheduleId(), Collections.emptyMap(), cron);
    TriggeringInfo partTriggeringInfo = new TriggeringInfo
      .PartitionTriggeringInfo(createScheduleId(), Collections.emptyMap(),
                               "testDataset", namespace, 10, 20);
    TriggeringPropertyMapping mapping = new TriggeringPropertyMapping(
      Arrays.asList(new ArgumentMapping[]{}),
      Arrays.asList(new PluginPropertyMapping[]{
        new PluginPropertyMapping("File", "sampleSize", "sample-size-arg",
                                  new TriggeringPipelineId(namespace, "pipeline"))
      })
    );
    TriggeringInfo andTriggeringInfo = new TriggeringInfo
      .AndTriggeringInfo(Arrays.asList(new TriggeringInfo[]{partTriggeringInfo, timeTriggeringInfo}),
                         createScheduleId(), Collections.emptyMap(), mapping);
    Assert.assertEquals(triggeringInfo, andTriggeringInfo);
  }

  private ScheduleId createScheduleId() {
    return new ScheduleId(namespace, appName, version, "testSchedule");
  }

  private void verifyProgramStatusTriggeringInfo(TriggeringInfo.ProgramStatusTriggeringInfo a,
                                                 TriggeringInfo.ProgramStatusTriggeringInfo b) {
    Assert.assertEquals(a.getScheduleId(), b.getScheduleId());
    Assert.assertEquals(a.getRuntimeArguments(), b.getRuntimeArguments());
    Assert.assertEquals(a.getProgramRunId().getProgram(), b.getProgramRunId().getProgram());
    Assert.assertEquals(a.getProgramRunId().getApplication(), b.getProgramRunId().getApplication());
    Assert.assertEquals(a.getProgramRunId().getVersion(), b.getProgramRunId().getVersion());
    Assert.assertEquals(a.getProgramRunId().getType(), b.getProgramRunId().getType());
  }
}

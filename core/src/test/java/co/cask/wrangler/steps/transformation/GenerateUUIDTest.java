/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link GenerateUUID}
 */
public class GenerateUUIDTest {

  @Test
  public void testUUIDGeneration() throws Exception {
    String[] directives = new String[] {
      "generate-uuid uuid",
    };

    List<Record> records = Arrays.asList(
      new Record("value", "abc"),
      new Record("value", "xyz"),
      new Record("value", "Should be fine")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 3);
    Assert.assertEquals(2, records.get(0).length());
    Assert.assertEquals("uuid", records.get(1).getColumn(1));
    Assert.assertEquals("Should be fine", records.get(2).getValue("value"));
  }

}
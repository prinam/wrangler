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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.PipelineException;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.TextDirectives;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Fail}
 */
public class FailTest {

  @Test(expected = PipelineException.class)
  public void testFailEvaluationToTrue() throws Exception {
    String[] directives = new String[] {
      "fail count > 0",
    };

    List<Record> records = Arrays.asList(
      new Record("count", 1)
    );

    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(new TextDirectives(directives), null);
    pipeline.execute(records);
  }

  @Test
  public void testFailEvaluationToFalse() throws Exception {
    String[] directives = new String[] {
      "fail count > 10",
    };

    List<Record> records = Arrays.asList(
      new Record("count", 1)
    );

    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(new TextDirectives(directives), null);
    pipeline.execute(records);
  }

}

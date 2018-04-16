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

package org.apache.flink.api.sage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.sage.ClovisOutputFormat;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;


/**
 * Shows how the OutputFormatTest can be used in the batch case
 *
 */
public class OutputFormatTest {
	public static void main(String[] args) throws Exception {

		long meroObjectId = 1048582;
		String meroFilePath = "/tmp";
		int meroBufferSize = 4096;
		int meroChunkSize = 1;

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		//read some random data and produce tuples out of it
		DataSet<Tuple2<String, Integer>> inputData = env
			.fromElements("This,0\n" + "is,1\n" + "some,2\n" + "test,3\n" + "data,4\n")
			.flatMap(new TupleGenerator());

		//create the output format
		ClovisOutputFormat<Tuple2<String, Integer>> outputFormat = new ClovisOutputFormat<>(meroObjectId, meroFilePath, meroBufferSize);

		outputFormat.setWriteMode(WriteMode.OVERWRITE);

		inputData.output(outputFormat);

	    env.execute("ClovisOutputFormat Batch Job");
	}
	
	public static class TupleGenerator implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = 1L;

		public void flatMap(String input, Collector<Tuple2<String, Integer>> out) {

			String[] lines = input.split("\n");

			for (String line: lines) {
				String[] fileds = line.split(",");
				Tuple2<String, Integer> tuple = new Tuple2(fileds[0], Integer.parseInt(fileds[1]));
				out.collect(tuple);
			}
        }
    }
}

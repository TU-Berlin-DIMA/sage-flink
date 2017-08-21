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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.sage.ClovisOutputFormat;
import org.apache.flink.api.sage.ClovisOutputFormat.StorageType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Shows how the OutputFormatTest can be used in the streaming case
 *
 */
public class OutputFormatStreamingTest {
	
	public static void main(String[] args) throws Exception {

		long meroObjectId = 1048582;
		String meroFilePath = "/tmp";
		int meroBufferSize = 4096;
		int meroChunkSize = 1;
	
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		//read some random file and produce tuples out of it
		DataStream<Tuple2<String, Integer>> text = env
			.fromElements("This,0\n" + "is,1\n" + "some,2\n" + "test,3\n" + "data,4\n")
			.flatMap(new TupleGenerator());
		
		//create the output format, indicate the preferred storage type
		ClovisOutputFormat<Tuple2<String, Integer>> outputFormat = new ClovisOutputFormat<Tuple2<String, Integer>>(meroObjectId, meroFilePath, meroBufferSize, meroChunkSize);

		outputFormat.setWriteMode(WriteMode.OVERWRITE);

		text.writeUsingOutputFormat(outputFormat);

	    env.execute("ClovisOutputFormat Streaming Job");
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

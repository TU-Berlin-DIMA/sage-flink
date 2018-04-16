/* * Licensed to the Apache Software Foundation (ASF) under one
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
 * limitations under the License.*/



package org.apache.flink.api.sage;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Shows how the ClovisInputFormat can be used in the batch case
 **/
public class InputFormatTest {
	
	public static void main(String[] args) throws Exception {

		/**
		 * Read would only be successful if there already exists appropriate data for the provided Mero Object Id
		 */
		long meroObjectId = 1048582;
		String meroFilePath = "/tmp";
		int meroBufferSize = 4096;
		int meroChunkSize = 1;
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Set the type returned by the InputFormat
		TypeInformation<Tuple2<String, Integer>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

		// Create the InputFormat
		ClovisInputFormat<Tuple2<String, Integer>> inputFormat = new ClovisInputFormat<>(typeInformation, meroObjectId, meroFilePath, meroBufferSize);


		DataSet<Tuple2<String, Integer>> dataset = env.createInput(inputFormat);

		dataset.print();
	}

}

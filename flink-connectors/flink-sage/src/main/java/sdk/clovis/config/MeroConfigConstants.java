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

package sdk.clovis.config;

/**
 * Created by nouman on 5/9/17.
 */
public class MeroConfigConstants {

	public static final boolean OO_STORE = false;
	public static final int CLOVIS_LAYOUT_ID = 9;
	public static final String IP_ADDRESS = "172.16.150.139";
	public static final String CLOVIS_LOCAL_ENDPOINT = IP_ADDRESS+"@tcp:12345:44:101";
	public static final String CLOVIS_HA_ENDPOINT = IP_ADDRESS+"@tcp:12345:45:1";
	public static final String CLOVIS_CONFD_ENDPOINT = IP_ADDRESS+"@tcp:44:101";
	public static final String CLOVIS_PROF = "<0x7000000000000001:0>";
	public static final String CLOVIS_PROF_ID = "<0x7200000000000000:0>";
	public static final String CLOVIS_INDEX_DIR = "/tmp";
}

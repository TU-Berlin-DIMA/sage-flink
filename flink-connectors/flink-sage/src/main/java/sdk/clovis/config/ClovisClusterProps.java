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
 * Clovis Cluster Properties. Defaults can be overridden using the provided methods
 */
public class ClovisClusterProps {

	private static boolean OO_STORE = false;
	private static int CLOVIS_LAYOUT_ID = 9;
	private static String CLOVIS_LOCAL_ENDPOINT = "10.0.2.15@tcp:12345:44:101";
	private static String CLOVIS_HA_ENDPOINT = "10.0.2.15@tcp:12345:45:1";
	private static String CLOVIS_CONFD_ENDPOINT = "10.0.2.15@tcp:44:101";
	private static String CLOVIS_PROF = "<0x7000000000000001:0>";
	private static String CLOVIS_PROF_ID = "<0x7200000000000000:0>";
	private static String CLOVIS_INDEX_DIR = "/tmp";

	public static boolean getOoStore() {
		return OO_STORE;
	}

	public static void setOoStore(boolean ooStore) {
		OO_STORE = ooStore;
	}

	public static int getClovisLayoutId() {
		return CLOVIS_LAYOUT_ID;
	}

	public static void setClovisLayoutId(int clovisLayoutId) {
		CLOVIS_LAYOUT_ID = clovisLayoutId;
	}

	public static String getClovisLocalEndpoint() {
		return CLOVIS_LOCAL_ENDPOINT;
	}

	public static void setClovisLocalEndpoint(String clovisLocalEndpoint) {
		CLOVIS_LOCAL_ENDPOINT = clovisLocalEndpoint;
	}

	public static String getClovisHaEndpoint() {
		return CLOVIS_HA_ENDPOINT;
	}

	public static void setClovisHaEndpoint(String clovisHaEndpoint) {
		CLOVIS_HA_ENDPOINT = clovisHaEndpoint;
	}

	public static String getClovisConfdEndpoint() {
		return CLOVIS_CONFD_ENDPOINT;
	}

	public static void setClovisConfdEndpoint(String clovisConfdEndpoint) {
		CLOVIS_CONFD_ENDPOINT = clovisConfdEndpoint;
	}

	public static String getClovisProf() {
		return CLOVIS_PROF;
	}

	public static void setClovisProf(String clovisProf) {
		CLOVIS_PROF = clovisProf;
	}

	public static String getClovisProfId() {
		return CLOVIS_PROF_ID;
	}

	public static void setClovisProfId(String clovisProfId) {
		CLOVIS_PROF_ID = clovisProfId;
	}

	public static String getClovisIndexDir() {
		return CLOVIS_INDEX_DIR;
	}

	public static void setClovisIndexDir(String clovisIndexDir) {
		CLOVIS_INDEX_DIR = clovisIndexDir;
	}
}

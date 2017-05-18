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

package sdk.clovis;

import com.clovis.jni.enums.ClovisRealmType;
import com.clovis.jni.exceptions.ClovisInvalidParametersException;
import com.clovis.jni.pojo.ClovisConf;
import com.clovis.jni.pojo.ClovisInstance;
import com.clovis.jni.pojo.ClovisObjId;
import com.clovis.jni.pojo.ClovisRealm;
import com.clovis.jni.pojo.RealmType;
import com.clovis.jni.pojo.RealmTypeFactory;
import com.clovis.jni.startup.ClovisJavaApis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sdk.clovis.config.MeroConfigConstants;

import java.nio.ByteBuffer;

/**
 * Created by nouman on 4/3/17.
 */
public class ClovisAPI {

	private static final Logger LOG = LoggerFactory.getLogger(ClovisAPI.class);

	static ClovisJavaApis clovisJavaApis = new ClovisJavaApis();

	boolean exit = false;

	long objectId = 1048582;

	int rc = -1;

	int bufferSize = 4096;

	int chunkSize = 1;

	String filePath = "/tmp";

	Operation operation = null;

	ClovisInstance clovisInstance = new ClovisInstance();

	ClovisRealm clovisRealmObject = new ClovisRealm();

	RealmType rType = RealmTypeFactory.getRealmType(ClovisRealmType.CLOVIS_CONTAINER);

	ClovisObjId clovisObjectId = new ClovisObjId();

	ClovisConf conf = new ClovisConf();

	public ClovisAPI () {

		setDefaultConfValues(this.conf);

		if (initializeClovis(conf, clovisInstance) != 0) {
			System.out.println("Failed to initialize Clovis");
			return;
		}

		if (initializeContainer(rType, clovisRealmObject, clovisObjectId, clovisInstance) != 0) {
			System.out.println("Failed to initialize Clovis container");
			return;
		}

	}

	private void setDefaultConfValues(ClovisConf conf) {

		conf.setOoStore(MeroConfigConstants.OO_STORE);
		conf.setClovisLayoutId(MeroConfigConstants.CLOVIS_LAYOUT_ID);
		conf.setClovisLocalAddr(MeroConfigConstants.CLOVIS_LOCAL_ENDPOINT);
		conf.setClovisHaAddr(MeroConfigConstants.CLOVIS_HA_ENDPOINT);
		conf.setClovisConfdAddr(MeroConfigConstants.CLOVIS_CONFD_ENDPOINT);
		conf.setClovisProf(MeroConfigConstants.CLOVIS_PROF);
		conf.setClovisProfFid(MeroConfigConstants.CLOVIS_PROF_ID);
		conf.setClovisIndexDir(MeroConfigConstants.CLOVIS_INDEX_DIR);

	}

	private int initializeContainer(RealmType rType, ClovisRealm clovisRealmObject, ClovisObjId clovisObjectId, ClovisInstance instance) {
		return clovisJavaApis.m0ClovisContainerInit(rType, clovisRealmObject, clovisObjectId, instance);
	}

	private static int initializeClovis(ClovisConf conf, ClovisInstance instance) {
		try {
			return clovisJavaApis.m0ClovisInit(conf, instance);
		} catch (ClovisInvalidParametersException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public int read(int offset, ByteBuffer byteBuffer) {

		Operation read = new OperationReadObject(offset, byteBuffer);
		return read.performOp(clovisRealmObject, objectId, filePath, bufferSize, chunkSize);
	}

	private static void log(String message) {
		LOG.error("[ClovisAPI - " + message + "]");
	}

}

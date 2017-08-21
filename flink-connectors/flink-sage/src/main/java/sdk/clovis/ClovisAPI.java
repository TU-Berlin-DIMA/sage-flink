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
import sdk.clovis.config.ClovisClusterProps;

import java.nio.ByteBuffer;

/**
 * Provides a handle for performing operations on Mero Objects
 */
public class ClovisAPI {

	static ClovisJavaApis clovisJavaApis = new ClovisJavaApis();

	ClovisInstance clovisInstance = new ClovisInstance();

	public ClovisRealm clovisRealmObject = new ClovisRealm();

	RealmType rType = RealmTypeFactory.getRealmType(ClovisRealmType.CLOVIS_CONTAINER);

	ClovisObjId clovisObjectId = new ClovisObjId();

	ClovisConf conf = new ClovisConf();

	public ClovisAPI () {

		setDefaultConfValues(this.conf);
		if (initializeClovis(conf, clovisInstance) != 0) {
			return;
		}

		if (initializeContainer(rType, clovisRealmObject, clovisObjectId, clovisInstance) != 0) {
			return;
		}
	}

	private void setDefaultConfValues(ClovisConf conf) {

		conf.setOoStore(ClovisClusterProps.getOoStore());
		conf.setClovisLayoutId(ClovisClusterProps.getClovisLayoutId());
		conf.setClovisLocalAddr(ClovisClusterProps.getClovisLocalEndpoint());
		conf.setClovisHaAddr(ClovisClusterProps.getClovisHaEndpoint());
		conf.setClovisConfdAddr(ClovisClusterProps.getClovisConfdEndpoint());
		conf.setClovisProf(ClovisClusterProps.getClovisProf());
		conf.setClovisProfFid(ClovisClusterProps.getClovisProfId());
		conf.setClovisIndexDir(ClovisClusterProps.getClovisIndexDir());
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

	public int read(int offset, ByteBuffer byteBuffer, long objectId, String filePath, int bufferSize, int chunkSize) {

		Operation read = new OperationReadObject(offset, byteBuffer);
		return read.performOp(clovisRealmObject, objectId, filePath, bufferSize, chunkSize);
	}

	public int create(long objectId, String filePath, int bufferSize, int chunkSize) {

		Operation create = new OperationCreateObject();
		return create.performOp(clovisRealmObject, objectId, filePath, bufferSize, chunkSize);
	}

	public int write(byte[] byteArray, long objectId, String filePath, int bufferSize, int chunkSize) {

		Operation write = new OperationWriteObject(byteArray);
		return write.performOp(clovisRealmObject, objectId, filePath, bufferSize, chunkSize);
	}
}

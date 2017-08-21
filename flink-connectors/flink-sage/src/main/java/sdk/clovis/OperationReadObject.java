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

import com.clovis.jni.enums.ClovisEntityType;
import com.clovis.jni.enums.ClovisObjOpCode;
import com.clovis.jni.enums.ClovisOpState;
import com.clovis.jni.pojo.ClovisBufVec;
import com.clovis.jni.pojo.ClovisEntity;
import com.clovis.jni.pojo.ClovisIndexVec;
import com.clovis.jni.pojo.ClovisObjId;
import com.clovis.jni.pojo.ClovisOp;
import com.clovis.jni.pojo.ClovisRealm;
import com.clovis.jni.pojo.EntityType;
import com.clovis.jni.pojo.EntityTypeFactory;
import com.clovis.jni.startup.ClovisJavaApis;
import com.clovis.jni.utils.TimeUtils;
import com.clovis.jni.utils.UtilityMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * TODO: Add comments
 */
public class OperationReadObject implements Operation {

	private static final Logger LOG = LoggerFactory.getLogger(OperationReadObject.class);

	int offset;
	ByteBuffer byteBuffer;

	public OperationReadObject(int offset, ByteBuffer byteBuffer) {

		this.offset = offset;
		this.byteBuffer = byteBuffer;
	}

	@Override
	synchronized public int performOp(ClovisRealm clovisRealmObj, long objectId, String filePath, int bufferSize, int chunkSize) {

		int rc = -1;

		long timeUtils = TimeUtils.M0_TIME_NEVER;

		long lastIndex = bufferSize * offset;

		ClovisJavaApis callNativeApis = new ClovisJavaApis();

		UtilityMethods utilityMethods = new UtilityMethods();

		EntityType eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		ClovisObjId objId = new ClovisObjId();
		objId.setHi(0);
		objId.setLow(objectId);

		ClovisEntity entity = new ClovisEntity();

		ClovisOp clovisOp = new ClovisOp();

		ArrayList<ClovisOp> opList = new ArrayList<ClovisOp>();
		opList.add(clovisOp);

		ClovisOpState[] clovisOpStates = new ClovisOpState[2];
		clovisOpStates[0] = ClovisOpState.M0_CLOVIS_OS_STABLE;
		clovisOpStates[1] = ClovisOpState.M0_CLOVIS_OS_FAILED;

		ClovisIndexVec extRead = callNativeApis.m0IndexvecAlloc(chunkSize);

		ClovisBufVec dataRead = callNativeApis.m0BufvecAlloc(bufferSize, chunkSize);

		ClovisBufVec attrRead = null;

		// APIs to create an object
		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			System.out.println("Read : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisEntityCreate(eType, entity, clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisEntityCreate() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpLaunch() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpWait() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != 0 && rc != -17) {
			System.out.println("Read : m0ClovisOpStatus() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpFini() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpFree() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			System.out.println("Read : m0ClovisObjFini() call fails rc = " + rc);
			return -1;
		}

		// APIs to Read from object

		for (int i = 0; i < extRead.getNumberOfSegs(); i++) {
			extRead.getIndexArray()[i] = lastIndex;
			lastIndex += bufferSize;
			extRead.getOffSetArray()[i] = bufferSize;
		}

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			System.out.println("Read : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		clovisOp = new ClovisOp();

		rc = callNativeApis.m0ClovisObjOp(
			eType, ClovisObjOpCode.M0_CLOVIS_OC_READ, extRead, dataRead, attrRead, 0, clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisObjOp() call fails rc = " + rc);
			return -1;
		}

		opList = new ArrayList<ClovisOp>();
		opList.add(clovisOp);

		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpLaunch() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpWait() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpStatus() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpFini() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			System.out.println("Read : m0ClovisOpFree() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			System.out.println("Read : m0ClovisObjFini() call fails rc = " + rc);
			return -1;
		}

		for (ByteBuffer buff: dataRead) {
			this.byteBuffer.put(buff);
		}

		int limit = 0;
		for (Byte b: this.byteBuffer.array()) {
			if (b == 0) {
				break;
			} else {
				limit++;
			}
		}

		return limit;
	}
}

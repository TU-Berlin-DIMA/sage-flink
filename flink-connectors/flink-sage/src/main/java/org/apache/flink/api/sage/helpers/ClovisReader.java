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

package org.apache.flink.api.sage.helpers;

import com.clovis.jni.enums.ClovisObjOpCode;
import com.clovis.jni.pojo.ClovisOp;
import com.clovis.jni.pojo.ClovisBufVec;
import com.clovis.jni.pojo.ClovisIndexVec;
import com.clovis.jni.utils.TimeUtils;
import com.clovis.jni.utils.StatusCodes;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Clemens Lutz on 11/27/17.
 */
public class ClovisReader extends ClovisCommon {

	private ArrayList<ClovisOp> opList;
	private ArrayList<ClovisBufVec> readDataBufferList;

	/*
	 * Work around a bug in Clovis JNI. Clovis allocates direct ByteBuffers in JVM,
	 * but apparently frees the underlying memory in C. By keeping a reference, we
	 * temporarily prevent the JVM from performing a double-free.
	 */
	private ArrayList<ClovisBufVec> freeDataBufferList;

	private static final Logger LOG = LoggerFactory.getLogger(ClovisReader.class);

	public ClovisReader() throws IOException {
		super();
	}

	public void open(long objectId, int blockSize) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Open object " + objectId + " with block size " + blockSize);
		}

		super.open(objectId, blockSize);

		opList = new ArrayList<>();
		readDataBufferList = new ArrayList<>();
		freeDataBufferList = new ArrayList<>();
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	@Override
	public void freeBuffer(ClovisBufVec dataRead) throws IOException {
		super.freeBuffer(dataRead);
	}

	public void scheduleRead(ArrayList<Integer> bufferIndexes) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduled read of index " + bufferIndexes.get(0));
		}

		int rc;

		ClovisIndexVec extRead = callNativeApis.m0IndexvecAlloc(bufferIndexes.size());
		ClovisBufVec dataRead = callNativeApis.m0BufvecAlloc(blockSize, bufferIndexes.size());
		ClovisBufVec attrRead = null;

		for (int i = 0; i < extRead.getNumberOfSegs(); i++) {
			extRead.getIndexArray()[i] = bufferIndexes.get(i) * blockSize;
			extRead.getOffSetArray()[i] = blockSize;
		}

		ClovisOp clovisOp = new ClovisOp();

		rc = callNativeApis.m0ClovisObjOp(
			eType, ClovisObjOpCode.M0_CLOVIS_OC_READ, extRead, dataRead, attrRead, 0, clovisOp);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisObjOp() call fails rc = " + rc);
		}

		opList.add(clovisOp);
		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisOpLaunch() call fails rc = " + rc);
		}

		readDataBufferList.add(dataRead);
	}

	public ClovisBufVec getNextBuffer() throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Get next ClovisBufVec");
		}

		int rc;

		ClovisOp clovisOp = opList.remove(0);
		ClovisBufVec dataRead = readDataBufferList.remove(0);

		// workaround Clovis JNI bug, see declaration of freeDataBufferList
		freeDataBufferList.add(dataRead);

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, TimeUtils.M0_TIME_NEVER);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisOpWait() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != StatusCodes.SUCCESS && rc != StatusCodes.EEXIST) {
			throw new IOException("Read : m0ClovisOpStatus() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisOpFini() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisOpFree() call fails rc = " + rc);
		}

		return dataRead;
	}
}

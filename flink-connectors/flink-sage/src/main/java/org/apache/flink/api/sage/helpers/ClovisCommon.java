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

import org.apache.flink.configuration.Configuration;

import com.clovis.jni.enums.ClovisEntityType;
import com.clovis.jni.enums.ClovisOpState;
import com.clovis.jni.enums.ClovisRealmType;
import com.clovis.jni.exceptions.ClovisInvalidParametersException;
import com.clovis.jni.pojo.ClovisObjId;
import com.clovis.jni.pojo.ClovisRealm;
import com.clovis.jni.pojo.EntityType;
import com.clovis.jni.pojo.ClovisBufVec;
import com.clovis.jni.pojo.EntityTypeFactory;
import com.clovis.jni.pojo.RealmType;
import com.clovis.jni.pojo.RealmTypeFactory;
import com.clovis.jni.pojo.ClovisInstance;
import com.clovis.jni.pojo.ClovisConf;
import com.clovis.jni.startup.ClovisJavaApis;
import com.clovis.jni.utils.StatusCodes;
import org.apache.flink.api.sage.configuration.ClovisClusterProperties;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Clemens Lutz on 4/12/18.
 */
public abstract class ClovisCommon {

	private static final Logger LOG = LoggerFactory.getLogger(ClovisCommon.class);

	private RealmType rType = RealmTypeFactory.getRealmType(ClovisRealmType.CLOVIS_CONTAINER);
	private ClovisConf conf;
	private ClovisInstance clovisInstance;

	private ClovisRealm clovisRealmObj;
	protected ClovisJavaApis callNativeApis;
	protected EntityType eType;
	private ClovisObjId objId;
	protected ClovisOpState[] clovisOpStates;

	protected int blockSize;

	/**
	 * ------------------------------------- Configuration Keys ------------------------------------------
	 */
	private static final String MERO_OBJECT_ID = "mero.object.id";
	private static final String MERO_FILE_PATH = "mero.file.path";
	private static final String MERO_BUFFER_SIZE = "mero.buffer.size";
	private static final String MERO_CHUNK_SIZE = "mero.chunk.size";
	private static final String OO_STORE = "clovis.object-store";
	private static final String CLOVIS_LAYOUT_ID = "clovis.layout-id";
	private static final String CLOVIS_LOCAL_ENDPOINT = "clovis.local-endpoint";
	private static final String CLOVIS_HA_ENDPOINT = "clovis.ha-endpoint";
	private static final String CLOVIS_CONFD_ENDPOINT = "clovis.confd-endpoint";
	private static final String CLOVIS_PROF = "clovis.prof";
	private static final String CLOVIS_PROF_ID = "clovis.prof-id";
	private static final String CLOVIS_INDEX_DIR = "clovis.index-dir";

	ClovisCommon() throws IOException {
		this.callNativeApis = new ClovisJavaApis();
		this.conf = new ClovisConf();
		this.clovisInstance = new ClovisInstance();
		this.clovisRealmObj = new ClovisRealm();

		setDefaultConfValues(this.conf);
	}

	private static void setDefaultConfValues(ClovisConf conf) {

		conf.setOoStore(ClovisClusterProperties.getOoStore());
		conf.setClovisLayoutId(ClovisClusterProperties.getClovisLayoutId());
		conf.setClovisLocalAddr(ClovisClusterProperties.getClovisLocalEndpoint());
		conf.setClovisHaAddr(ClovisClusterProperties.getClovisHaEndpoint());
		conf.setClovisConfdAddr(ClovisClusterProperties.getClovisConfdEndpoint());
		conf.setClovisProf(ClovisClusterProperties.getClovisProf());
		conf.setClovisProfFid(ClovisClusterProperties.getClovisProfId());
		conf.setClovisIndexDir(ClovisClusterProperties.getClovisIndexDir());
	}

	/**
	 * When clovis cluster properties provided, the defaults from the {@link ClovisClusterProperties ()} will be overridden
	 */
	public static void setUserConfValues(Configuration parameters) {

		boolean ooStore = parameters.getBoolean(OO_STORE, false);
		ClovisClusterProperties.setOoStore(ooStore);

		int clovisLayoutId = parameters.getInteger(CLOVIS_LAYOUT_ID, -1);
		if (clovisLayoutId > 0) { ClovisClusterProperties.setClovisLayoutId(clovisLayoutId); }

		String clovisLocalEndpoint = parameters.getString(CLOVIS_LOCAL_ENDPOINT, null);
		if (clovisLocalEndpoint != null) { ClovisClusterProperties.setClovisLocalEndpoint(clovisLocalEndpoint); }

		String clovisHaEndpoint = parameters.getString(CLOVIS_HA_ENDPOINT, null);
		if (clovisHaEndpoint != null) { ClovisClusterProperties.setClovisHaEndpoint(clovisHaEndpoint); }

		String clovisConfdEndpoint = parameters.getString(CLOVIS_CONFD_ENDPOINT, null);
		if (clovisConfdEndpoint != null) { ClovisClusterProperties.setClovisConfdEndpoint(clovisConfdEndpoint); }

		String clovisProf = parameters.getString(CLOVIS_PROF, null);
		if (clovisProf != null) { ClovisClusterProperties.setClovisProf(clovisProf); }

		String clovisProfId = parameters.getString(CLOVIS_PROF_ID, null);
		if (clovisProfId != null) { ClovisClusterProperties.setClovisProfId(clovisProfId); }

		String clovisIndexDir = parameters.getString(CLOVIS_INDEX_DIR, null);
		if (clovisIndexDir != null) { ClovisClusterProperties.setClovisIndexDir(clovisIndexDir); }
	}

	protected void open(long objectId, int blockSize) throws IOException {

		this.blockSize = blockSize;

		objId = new ClovisObjId();
		objId.setHi(0);
		objId.setLow(objectId);

		try {
			if (callNativeApis.m0ClovisInit(conf, clovisInstance) != StatusCodes.SUCCESS) {
				throw new IOException("Failed to initialize Clovis");
			}
		} catch (ClovisInvalidParametersException e) {
			e.printStackTrace();
			throw new IOException();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException();
		}

		if (callNativeApis.m0ClovisContainerInit(rType, clovisRealmObj, objId, clovisInstance) != StatusCodes.SUCCESS) {
			throw new IOException("Failed to initialize Clovis container");
		}

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		int rc;
		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisObjInit() call fails rc = " + rc);
		}

		clovisOpStates = new ClovisOpState[2];
		clovisOpStates[0] = ClovisOpState.M0_CLOVIS_OS_STABLE;
		clovisOpStates[1] = ClovisOpState.M0_CLOVIS_OS_FAILED;
	}

	protected void close() throws IOException {
		int rc;

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisObjFini() call fails rc = " + rc);
		}
		rc = callNativeApis.m0ClovisFini(clovisInstance);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisFini() call fails rc = " + rc);
		}
	}

	protected ClovisBufVec allocBuffer(int blockCount) throws IOException {
		return callNativeApis.m0BufvecAlloc(blockSize, blockCount);
	}

	protected void freeBuffer(ClovisBufVec dataRead) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Skipping call to m0ClovisFreeBufVec");
		}
		/*

		int rc;

		rc = callNativeApis.m0ClovisFreeBufVec(dataRead);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisFreeBufVec() call fails rc = " + rc);
		}
		*/
	}
}

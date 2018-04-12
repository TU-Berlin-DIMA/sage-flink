package org.apache.flink.api.sage;

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
import sdk.clovis.config.ClovisClusterProps;

import java.io.IOException;

/**
 * Created by Clemens Lutz on 4/12/18.
 */
public abstract class ClovisCommon {

	private RealmType rType = RealmTypeFactory.getRealmType(ClovisRealmType.CLOVIS_CONTAINER);
	private ClovisConf conf;
	private ClovisInstance clovisInstance;

	private ClovisRealm clovisRealmObj;
	protected ClovisJavaApis callNativeApis;
	protected EntityType eType;
	private ClovisObjId objId;
	protected ClovisOpState[] clovisOpStates;

	protected int bufferSize;

	ClovisCommon() throws IOException {
		this.callNativeApis = new ClovisJavaApis();
		this.conf = new ClovisConf();
		this.clovisInstance = new ClovisInstance();
		this.clovisRealmObj = new ClovisRealm();

		setDefaultConfValues(this.conf);
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
	}

	private static void setDefaultConfValues(ClovisConf conf) {

		conf.setOoStore(ClovisClusterProps.getOoStore());
		conf.setClovisLayoutId(ClovisClusterProps.getClovisLayoutId());
		conf.setClovisLocalAddr(ClovisClusterProps.getClovisLocalEndpoint());
		conf.setClovisHaAddr(ClovisClusterProps.getClovisHaEndpoint());
		conf.setClovisConfdAddr(ClovisClusterProps.getClovisConfdEndpoint());
		conf.setClovisProf(ClovisClusterProps.getClovisProf());
		conf.setClovisProfFid(ClovisClusterProps.getClovisProfId());
		conf.setClovisIndexDir(ClovisClusterProps.getClovisIndexDir());
	}

	protected void open(long objectId, int bufferSize) throws IOException {

		this.bufferSize = bufferSize;

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		objId = new ClovisObjId();
		objId.setHi(0);
		objId.setLow(objectId);

		if (callNativeApis.m0ClovisContainerInit(rType, clovisRealmObj, objId, clovisInstance) != 0) {
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
	}

	protected ClovisBufVec allocBuffer(int blockCount) throws IOException {
		return callNativeApis.m0BufvecAlloc(bufferSize, blockCount);
	}

	protected void freeBuffer(ClovisBufVec dataRead) throws IOException {
		int rc;

		rc = callNativeApis.m0ClovisFreeBufVec(dataRead);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisFreeBufVec() call fails rc = " + rc);
		}
	}
}

package org.apache.flink.api.sage;

import com.clovis.jni.enums.ClovisEntityType;
import com.clovis.jni.enums.ClovisObjOpCode;
import com.clovis.jni.enums.ClovisOpState;
import com.clovis.jni.enums.ClovisRealmType;
import com.clovis.jni.exceptions.ClovisInvalidParametersException;
import com.clovis.jni.pojo.ClovisObjId;
import com.clovis.jni.pojo.ClovisRealm;
import com.clovis.jni.pojo.EntityType;
import com.clovis.jni.pojo.ClovisOp;
import com.clovis.jni.pojo.ClovisBufVec;
import com.clovis.jni.pojo.ClovisIndexVec;
import com.clovis.jni.pojo.EntityTypeFactory;
import com.clovis.jni.pojo.RealmType;
import com.clovis.jni.pojo.RealmTypeFactory;
import com.clovis.jni.pojo.ClovisInstance;
import com.clovis.jni.pojo.ClovisConf;
//import com.clovis.jni.pojo.ClovisEntity;
import com.clovis.jni.startup.ClovisJavaApis;
import com.clovis.jni.utils.TimeUtils;
import com.clovis.jni.utils.StatusCodes;
import sdk.clovis.config.ClovisClusterProps;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Clemens Lutz on 11/27/17.
 */
public class ClovisReader {

	private RealmType rType = RealmTypeFactory.getRealmType(ClovisRealmType.CLOVIS_CONTAINER);
	private ClovisConf conf;
	private ClovisInstance clovisInstance;

	private ClovisRealm clovisRealmObj;
	private ClovisJavaApis callNativeApis;
	private EntityType eType;
	private ClovisObjId objId;
	private ClovisOpState[] clovisOpStates;

	private ArrayList<ClovisOp> opList;
	private ArrayList<ClovisBufVec> readDataBufferList;

	private int bufferSize;
	private int chunkSize;

	private static final Logger LOG = LoggerFactory.getLogger(ClovisReader.class);

	ClovisReader(/*ClovisConf conf, ClovisInstance instance */) throws IOException {

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

	public void open(long objectId, int bufferSize, int chunkSize) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Open object " + objectId + " with buffer size " + bufferSize + " and chunk size " + chunkSize);
		}

		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		objId = new ClovisObjId();
		objId.setHi(0);
		objId.setLow(objectId);

		opList = new ArrayList<>();
		readDataBufferList = new ArrayList<>();

//		NOTE: try to initializeClovis in Constructor. Remove this if it works and doesn't segfault
//		if (initializeClovis(conf, clovisInstance) != 0) {
//			throw new IOException("Failed to initialize Clovis");
//		}

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

	public void close() throws IOException {
		int rc;

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisObjFini() call fails rc = " + rc);
		}

	}

	public void scheduleRead(int offset) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduled read with offset " + offset);
		}

		int rc;

		ClovisIndexVec extRead = callNativeApis.m0IndexvecAlloc(chunkSize);
		ClovisBufVec dataRead = callNativeApis.m0BufvecAlloc(bufferSize, chunkSize);
		ClovisBufVec attrRead = null;

		long lastIndex = bufferSize * offset;
		for (int i = 0; i < extRead.getNumberOfSegs(); i++) {
			extRead.getIndexArray()[i] = lastIndex;
			lastIndex += bufferSize;
			extRead.getOffSetArray()[i] = bufferSize;
		}

		ClovisOp clovisOp = new ClovisOp();

//		ClovisEntity entity = new ClovisEntity();
//		rc = callNativeApis.m0ClovisEntityCreate(eType, entity, clovisOp);
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisEntityCreate() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisOpLaunch() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, TimeUtils.M0_TIME_NEVER);
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisOpWait() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
//		if (rc != 0 && rc != -17) {
//			throw new IOException("Read : m0ClovisOpStatus() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisOpFini(clovisOp);
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisOpFini() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisOpFree(clovisOp);
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisOpFree() call fails rc = " + rc);
//		}
//
//		rc = callNativeApis.m0ClovisObjFini(eType);
//		if (rc != 0) {
//			throw new IOException("Read : m0ClovisObjFini() call fails rc = " + rc);
//		}
//
//		clovisOp = new ClovisOp();

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

		int rc = -1;

		ClovisOp clovisOp = opList.get(0);
		opList.remove(0);
		ClovisBufVec dataRead = readDataBufferList.get(0);

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

	public void freeBuffer(ClovisBufVec dataRead) throws IOException {

		int rc;

		rc = callNativeApis.m0ClovisFreeBufVec(dataRead);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisFreeBufVec() call fails rc = " + rc);
		}

	}
}

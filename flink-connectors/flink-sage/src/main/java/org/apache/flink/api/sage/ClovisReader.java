package org.apache.flink.api.sage;

import com.clovis.jni.enums.ClovisEntityType;
import com.clovis.jni.enums.ClovisObjOpCode;
import com.clovis.jni.enums.ClovisOpState;
import com.clovis.jni.pojo.*;
import com.clovis.jni.startup.ClovisJavaApis;
import com.clovis.jni.utils.TimeUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 11/27/17.
 */
public class ClovisReader {

	ClovisRealm clovisRealmObj;
	private ClovisJavaApis callNativeApis;
	private EntityType eType;
	private ClovisObjId objId;
	private ClovisOpState[] clovisOpStates;

	private ArrayList<ClovisOp> opList;
	private ArrayList<ClovisBufVec> readDataBufferList;

	private int bufferSize;
	private int chunkSize;

	public void open(ClovisRealm clovisRealmObj, long objectId, int bufferSize, int chunkSize) {
		this.clovisRealmObj = clovisRealmObj;
		this.bufferSize = bufferSize;
		this.chunkSize = chunkSize;

		callNativeApis = new ClovisJavaApis();
		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		objId = new ClovisObjId();
		objId.setHi(0);
		objId.setLow(objectId);

		opList = new ArrayList<ClovisOp>();
		readDataBufferList = new ArrayList<ClovisBufVec>();

		clovisOpStates = new ClovisOpState[2];
		clovisOpStates[0] = ClovisOpState.M0_CLOVIS_OS_STABLE;
		clovisOpStates[1] = ClovisOpState.M0_CLOVIS_OS_FAILED;
	}

	public void scheduleRead(int offset) throws IOException {

		int rc = -1;

		ClovisIndexVec extRead = callNativeApis.m0IndexvecAlloc(chunkSize);
		ClovisBufVec dataRead = callNativeApis.m0BufvecAlloc(bufferSize, chunkSize);
		ClovisBufVec attrRead = null;

		long lastIndex = bufferSize * offset;
		for (int i = 0; i < extRead.getNumberOfSegs(); i++) {
			extRead.getIndexArray()[i] = lastIndex;
			lastIndex += bufferSize;
			extRead.getOffSetArray()[i] = bufferSize;
		}

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisObjInit() call fails rc = " + rc);
		}

		ClovisOp clovisOp = new ClovisOp();

		rc = callNativeApis.m0ClovisObjOp(
			eType, ClovisObjOpCode.M0_CLOVIS_OC_READ, extRead, dataRead, attrRead, 0, clovisOp);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisObjOp() call fails rc = " + rc);
		}

		opList.add(clovisOp);
		readDataBufferList.add(dataRead);

		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			throw new IOException("Read : m0ClovisOpLaunch() call fails rc = " + rc);
		}

	}

	public ClovisBufVec getNextBuffer() throws IOException {

		int rc = -1;
		long timeUtils = TimeUtils.M0_TIME_NEVER;

		ClovisOp clovisOp = opList.get(0);
		ClovisBufVec dataRead = readDataBufferList.get(0);

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisOpWait() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisOpStatus() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisOpFini() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisOpFree() call fails rc = " + rc);
		}

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisObjFini() call fails rc = " + rc);
		}

		opList.remove(0);
		opList.remove(0);

		return dataRead;
	}

	public void freeBuffer(ClovisBufVec dataRead) throws IOException {

		int rc;

		rc = callNativeApis.m0ClovisFreeBufVec(dataRead);
		if (rc != 0) {
			throw new IOException("Read : m0ClovisFreeBufVec() call fails rc = " + rc);
		}

	}
}

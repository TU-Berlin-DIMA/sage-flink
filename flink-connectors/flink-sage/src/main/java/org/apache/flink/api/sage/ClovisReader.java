package org.apache.flink.api.sage;

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

	private static final Logger LOG = LoggerFactory.getLogger(ClovisReader.class);

	ClovisReader() throws IOException {
		super();
	}

	public void open(long objectId, int blockSize) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Open object " + objectId + " with block size " + blockSize);
		}

		super.open(objectId, blockSize);

		opList = new ArrayList<>();
		readDataBufferList = new ArrayList<>();
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
			LOG.debug("Scheduled read");
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
}

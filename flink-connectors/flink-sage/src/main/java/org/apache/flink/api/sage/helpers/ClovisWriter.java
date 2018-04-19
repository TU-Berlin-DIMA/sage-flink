package org.apache.flink.api.sage.helpers;

import com.clovis.jni.enums.ClovisObjOpCode;
import com.clovis.jni.pojo.ClovisOp;
import com.clovis.jni.pojo.ClovisBufVec;
import com.clovis.jni.pojo.ClovisIndexVec;
import com.clovis.jni.utils.StatusCodes;
import com.clovis.jni.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 04/12/18.
 */
public class ClovisWriter extends ClovisCommon {

	private ArrayList<ClovisOp> opList;
	private ArrayList<ClovisBufVec> writeDataBufferList;

	private static final Logger LOG = LoggerFactory.getLogger(ClovisWriter.class);

	public ClovisWriter() throws IOException {
		super();
	}

	@Override
	public void open(long objectId, int blockSize) throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Open object " + objectId + " with buffer size " + blockSize);
		}

		super.open(objectId, blockSize);

		opList = new ArrayList<>();
		writeDataBufferList = new ArrayList<>();
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	@Override
	public ClovisBufVec allocBuffer(int blockCount) throws IOException {
		return super.allocBuffer(blockCount);
	}

	public void scheduleWrite(ArrayList<Integer> bufferIndexes, ClovisBufVec dataWrite) throws IOException {

		assert(bufferIndexes.size() == dataWrite.getNumberOfBuffers());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Scheduled write ");
		}

		int rc;

		ClovisIndexVec extWrite = callNativeApis.m0IndexvecAlloc(dataWrite.getNumberOfBuffers());
		ClovisBufVec attrWrite = null;

		for (int i = 0; i < extWrite.getNumberOfSegs(); i++) {
			extWrite.getIndexArray()[i] = bufferIndexes.get(i) * blockSize;
			extWrite.getOffSetArray()[i] = blockSize;
		}

		ClovisOp clovisOp = new ClovisOp();

		rc = callNativeApis.m0ClovisObjOp(
			eType, ClovisObjOpCode.M0_CLOVIS_OC_WRITE, extWrite, dataWrite, attrWrite, 0, clovisOp);
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisObjOp() call fails rc = " + rc);
		}

		opList.add(clovisOp);
		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != StatusCodes.SUCCESS) {
			throw new IOException("Read : m0ClovisOpLaunch() call fails rc = " + rc);
		}

		writeDataBufferList.add(dataWrite);
	}

	public void writeFinish() throws IOException {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Finish last write and check if successful");
		}

		int rc;

		ClovisOp clovisOp = opList.get(0);
		opList.remove(0);

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

		ClovisBufVec dataWrite = writeDataBufferList.get(0);
		freeBuffer(dataWrite);
	}
}

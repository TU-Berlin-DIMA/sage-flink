/*
 * COPYRIGHT 2017 SEAGATE LLC
 *
 * THIS DRAWING/DOCUMENT, ITS SPECIFICATIONS, AND THE DATA CONTAINED
 * HEREIN, ARE THE EXCLUSIVE PROPERTY OF SEAGATE TECHNOLOGY
 * LIMITED, ISSUED IN STRICT CONFIDENCE AND SHALL NOT, WITHOUT
 * THE PRIOR WRITTEN PERMISSION OF SEAGATE TECHNOLOGY LIMITED,
 * BE REPRODUCED, COPIED, OR DISCLOSED TO A THIRD PARTY, OR
 * USED FOR ANY PURPOSE WHATSOEVER, OR STORED IN A RETRIEVAL SYSTEM
 * EXCEPT AS ALLOWED BY THE TERMS OF SEAGATE LICENSES AND AGREEMENTS.
 *
 * YOU SHOULD HAVE RECEIVED A COPY OF SEAGATE'S LICENSE ALONG WITH
 * THIS RELEASE. IF NOT PLEASE CONTACT A SEAGATE REPRESENTATIVE
 * http://www.seagate.com/contact
 *
 * Original author: Rahul Joshi <rahul.joshi@seagate.com>
 * Original creation date: 01-Feb-2017
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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class OperationWriteObject implements Operation {

	private byte[] byteArray;

	public OperationWriteObject(byte[] byteArray) {

		this.byteArray = byteArray;
	}


	@Override
	public int performOp(
		ClovisRealm clovisRealmObj, long objectId, String filePath, int bufferSize, int chunkSize) {
		int rc = -1;

		long timeUtils = TimeUtils.M0_TIME_NEVER;

		long last_index = 0;

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

		ClovisBufVec data = callNativeApis.m0BufvecAlloc(bufferSize, chunkSize);

		ClovisIndexVec ext = callNativeApis.m0IndexvecAlloc(chunkSize);

		ClovisBufVec attr = null;

		// APIs to create an object
		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			System.out.println("Write : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisEntityCreate(eType, entity, clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisEntityCreate() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpLaunch() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpWait() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != 0 && rc != -17) {
			System.out.println("Write : m0ClovisOpStatus() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpFini() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpFree() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			System.out.println("Write : m0ClovisObjFini() call fails rc = " + rc);
			return -1;
		}
		// APIs to Write to object

		for (int i = 0; i < ext.getNumberOfSegs(); i++) {
			ext.getIndexArray()[i] = last_index;
			last_index += bufferSize;
			ext.getOffSetArray()[i] = bufferSize;
		}

		for (int i = 0; i <ext.getNumberOfSegs(); i++) {
			data.get(i).put(byteArray);
		}

		/*try {
			//in = new FileInputStream(filePath);
			for (int i = 0; i < ext.getNumberOfSegs(); i++) {
				byte[] b = new byte[bufferSize];
				in.read(b);
				data.get(i).put(b);
			}
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}*/

		eType = EntityTypeFactory.getEntityType(ClovisEntityType.CLOVIS_OBJ);

		rc = callNativeApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			System.out.println("Write : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		clovisOp = new ClovisOp();

		rc = callNativeApis.m0ClovisObjOp(
			eType, ClovisObjOpCode.M0_CLOVIS_OC_WRITE, ext, data, attr, 0, clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisObjOp() call fails rc = " + rc);
			return -1;
		}

		opList = new ArrayList<ClovisOp>();
		opList.add(clovisOp);

		rc = callNativeApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpLaunch() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpWait() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpStatus(clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpStatus() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpFini() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			System.out.println("Write : m0ClovisOpFree() call fails rc = " + rc);
			return -1;
		}

		rc = callNativeApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			System.out.println("Write : m0ClovisObjFini() call fails rc = " + rc);
			return -1;
		}

		try {
			utilityMethods.destroyBuffers(data);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException
			| IllegalArgumentException | InvocationTargetException e) {
			e.printStackTrace();
		}

		return 0;
	}
}

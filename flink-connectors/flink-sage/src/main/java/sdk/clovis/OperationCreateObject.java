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
import com.clovis.jni.enums.ClovisOpState;
import com.clovis.jni.pojo.ClovisEntity;
import com.clovis.jni.pojo.ClovisObjId;
import com.clovis.jni.pojo.ClovisOp;
import com.clovis.jni.pojo.ClovisRealm;
import com.clovis.jni.pojo.EntityType;
import com.clovis.jni.pojo.EntityTypeFactory;
import com.clovis.jni.startup.ClovisJavaApis;
import com.clovis.jni.utils.TimeUtils;

import java.util.ArrayList;

public class OperationCreateObject implements Operation {
	@Override
	public int performOp(
		ClovisRealm clovisRealmObj, long objectId, String filePath, int bufferSize, int chunkSize) {
		int rc = -1;

		boolean isAlreadyCreated = false;

		long timeUtils = TimeUtils.M0_TIME_NEVER;

		ClovisJavaApis clovisJavaApis = new ClovisJavaApis();

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

		rc = clovisJavaApis.m0ClovisObjInit(eType, clovisRealmObj, objId);
		if (rc != 0) {
			System.out.println("Create : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisEntityCreate(eType, entity, clovisOp);
		if (rc != 0) {
			System.out.println("Create : m0ClovisEntityCreate() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisOpLaunch(opList, opList.size());
		if (rc != 0) {
			System.out.println("Create : m0ClovisOpLaunch() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisOpWait(clovisOp, clovisOpStates, timeUtils);
		if (rc != 0) {
			System.out.println("Create : m0ClovisOpWait() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisOpStatus(clovisOp);
		if (rc == -17) {
			isAlreadyCreated = true;
		} else if (rc == 0) {
			isAlreadyCreated = false;
		} else {
			System.out.println("Create : m0ClovisOpStatus() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisOpFini(clovisOp);
		if (rc != 0) {
			System.out.println("Create : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisOpFree(clovisOp);
		if (rc != 0) {
			System.out.println("Create : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		rc = clovisJavaApis.m0ClovisObjFini(eType);
		if (rc != 0) {
			System.out.println("Create : m0ClovisObjInit() call fails rc = " + rc);
			return -1;
		}

		return isAlreadyCreated ? -17 : 0;
	}
}

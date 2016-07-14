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

package org.apache.flink.api.sage;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ClovisThreadPoolExecutor extends ThreadPoolExecutor {
	
	private int retryAttempts;
	private boolean failedTasks;

	public ClovisThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, int retryAttempts) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		this.retryAttempts = retryAttempts;
		this.failedTasks = false;
	}
	
	@Override
	public void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		if (t != null) {
			//The task failed with exception
			ClovisAsyncTask task = (ClovisAsyncTask) r;
			
			//Restart task retryAttempts times
			if (task.getRetryAttempt() > retryAttempts) {
				try {
					task.cleanup();
				} catch (InterruptedException | IOException e) {
					//do nothing
				}
				failedTasks = true;
			} else {
				//Task failed retryAttempts times - set the flag that there's
				//a failed task
				task.setRetryAttempt(task.getRetryAttempt() + 1);
				execute(task);
			}
		}
	}
	
	public boolean hasFailedTasks() {
		return failedTasks;
	}
}

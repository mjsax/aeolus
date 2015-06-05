/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package storm.lrb;

import java.io.File;
import java.sql.Connection;
import org.junit.Test;

/**
 *
 * @author richter
 */
public class LRBTopologyMainTest {
	public static final String DRIVER = "org.apache.derby.jdbc.ClientDriver";
	public static final String URL = "jdbc:derby://localhost:1527/aeolus-test";
	private static Connection conn;

	public LRBTopologyMainTest() {}

	/**
	 * Test of main method, of class LRBTopologyMain.
	 *
	 * @throws java.lang.Exception
	 */
	@Test
	public void testMain0() throws Exception {
		String histFilePath = File.createTempFile("lrb-test", null).getAbsolutePath();
		LRBTopologyMain.main0(0, // offset
			1, // executors
			2, // xways
			"127.0.0.1", // host
			5060, // port
			histFilePath, 2, // tasks,
			false, // submit
			true, // stormConfigDebug
			2, // workers
			"nameext", // nameext
			5000 // runtimeMillis
			);
	}

}

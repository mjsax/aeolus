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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.derby.drda.NetworkServerControl;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





/**
 *
 * @author richter
 */
public class LRBTopologyMainTest {
	public static final String DRIVER = "org.apache.derby.jdbc.ClientDriver";
	public static final String URL = "jdbc:derby://localhost:1527/aeolus-test";
	private static Connection conn;
	private static final Logger logger = LoggerFactory.getLogger(LRBTopologyMainTest.class);
	private static NetworkServerControl server;

	/**
	 * sets up a derby server at localhost:1527 (experienced trouble with embedded server and JPA - documentation for
	 * derby can be considered inexisting ([intro on starting the server]
	 * (http://db.apache.org/derby/papers/DerbyTut/ns_intro.html) doesn't even include a code example or a useful link,
	 * so don't bother with that).
	 *
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws SQLException
	 * @throws UnknownHostException
	 * @throws Exception
	 *             yes, NetworkServerControl.start seriously throws {@code Exception}
	 */
	@BeforeClass
	public static void setUpClass() throws ClassNotFoundException, InstantiationException, IllegalAccessException,
		SQLException, UnknownHostException, Exception {
		NetworkServerControl server = new NetworkServerControl(InetAddress.getByName("localhost"), 1527);
		server.start(null // print writer
			);

		Driver derbyDriver = (Driver)Class.forName(DRIVER).newInstance();
		DriverManager.registerDriver(derbyDriver);
		conn = DriverManager.getConnection(URL + ";create=true");
	}

	/**
	 *
	 * @throws SQLException
	 * @throws Exception
	 *             yes, NetworkServerControl.start seriously throws {@code Exception}
	 */
	@AfterClass
	public static void tearDownClass() throws SQLException, Exception {
		if(server != null) {
			server.shutdown();
		}
	}

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

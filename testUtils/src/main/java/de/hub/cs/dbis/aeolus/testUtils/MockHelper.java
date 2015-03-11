/*
 * Copyright 2015 Humboldt-Universität zu Berlin.
 *
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
 */
package de.hub.cs.dbis.aeolus.testUtils;

/*
 * #%L
 * testUtils
 * %%
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %%
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
 * #L%
 */
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.TupleImpl;
import java.util.LinkedList;
import java.util.List;
import org.mockito.Matchers;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author richter
 */
public class MockHelper {

	/**
	 * generates a mock of {@link GeneralTopologyContext} with appropriate behavior for unit testing. Those include responding to {@link GeneralTopologyContext#getComponentOutputFields(java.lang.String, java.lang.String) } called any value and to {@link GeneralTopologyContext#getComponentId(int) } called with any value. This implies that the values for {@code streamID} passed to {@link TupleImpl#TupleImpl(backtype.storm.task.GeneralTopologyContext, java.util.List, int, java.lang.String) } and {@link TupleImpl#TupleImpl(backtype.storm.task.GeneralTopologyContext, java.util.List, int, java.lang.String, backtype.storm.tuple.MessageId) } don't have any effect (and therefore can be {@code null} or any {@code String}).
	 * @return 
	 */
	public static GeneralTopologyContext createGeneralTopologyContextMock() {
		GeneralTopologyContext context = mock(GeneralTopologyContext.class);
		when(context.getComponentOutputFields(anyString(), anyString())).thenReturn(new Fields("dummy"));
		when(context.getComponentId(Matchers.anyInt())).thenReturn("componentID");
		return context;
	}
	
	public static TopologyContext createTopologyContextMock() {
		List<Integer> taskMock = new LinkedList<Integer>();
		taskMock.add(0);
		TopologyContext contextMock = mock(TopologyContext.class);
		when(contextMock.getComponentTasks(anyString())).thenReturn(taskMock);
		when(contextMock.getThisTaskIndex()).thenReturn(0);
		return contextMock;
	}

	private MockHelper() {
	}
}

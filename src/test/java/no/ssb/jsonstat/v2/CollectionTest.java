/**
 * Copyright (C) 2016 Hadrien Kohl (hadrien.kohl@gmail.com) and contributors
 *
 *     CollectionTest.java
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.ssb.jsonstat.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by hadrien on 07/06/16.
 */
public class CollectionTest {

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeMethod
    public void setUp() throws Exception {
        // TODO
        mapper.registerModule(new SimpleModule());
    }

    @Test(enabled = false, dependsOnMethods = "testDeserialize")
    public void testSerialize(String testFile) throws Exception {

        Collection deserialize = deserialize(Resources.getResource(testFile));

        mapper.writeValue(System.out, deserialize);

        // TODO: Check equivalence?

    }

    @Test(enabled = false)
    public void testDeserialize(String testFile) throws Exception {

        Collection collection = deserialize(Resources.getResource(testFile));

        assertThat(collection).isNotNull();

    }

    private Collection deserialize(URL resource) throws java.io.IOException {
        return mapper.readValue(resource, Collection.class);
    }
}
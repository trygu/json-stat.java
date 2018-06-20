/**
 * Copyright (C) 2016 Hadrien Kohl (hadrien.kohl@gmail.com) and contributors
 *
 *     DatasetDeserializationTest.java
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

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.google.common.io.Resources;
import com.google.common.primitives.Ints;
import no.ssb.jsonstat.JsonStatModule;
import no.ssb.jsonstat.v2.support.DatasetTableView;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

public class DatasetDeserializationTest {

    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new JsonStatModule());
    }

    @Test
    public void testSsbApi() throws Exception {

        URL ssb = Resources.getResource(getClass(), "./ssb-api.json");
        JsonParser parser = mapper.getFactory().createParser(new BufferedInputStream(
                ssb.openStream()
        ));
        // v1 is in a map
        TypeReference<Map<String, DatasetBuildable>> ref = new TypeReference<Map<String, DatasetBuildable>>() {
            // just a ref.
        };

        Map<String, DatasetBuildable> o = mapper.readValue(
                parser,
                ref
        );

        DatasetBuildable next = o.values().iterator().next();

        Dataset build = next.build();

        Map<List<String>, Number> listListMap = build.asMap();
        for (Map.Entry<List<String>, Number> listListEntry : listListMap.entrySet()) {
            System.out.println(listListEntry);
        }

    }

    @Test
    public void testExtension() throws Exception {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        ObjectNode node = mapper.readValue(new BufferedInputStream(
                galicia.openStream()
        ), ObjectNode.class);

        assertThat(node).isNotNull();

        // Manually add extension
        node.with("extension")
                .put("number", 10)
                .putArray("array")
                .add("string");

        Dataset jsonStat = mapper.readValue(
                mapper.writeValueAsBytes(node),
                DatasetBuildable.class
        ).build();

        assertThat(jsonStat).isNotNull();
        assertThat(jsonStat.getExtension())
                .isNotNull()
                .isInstanceOf(ObjectNode.class);

    }

    @Test
    public void testDatasetDeserializationWith1DimensionOrderValuesCorrectly() throws Exception  {

        URL test = Resources.getResource(getClass(), "./json-stat-1-dimension.json");

        ObjectNode node = mapper.readValue(new BufferedInputStream(
                test.openStream()
        ), ObjectNode.class);

        assertThat(node).isNotNull();

        Dataset jsonStat = mapper.readValue(
                mapper.writeValueAsBytes(node),
                DatasetBuildable.class
        ).build();

        Map<Integer, Number> value = jsonStat.getValue();

        // Check value order
        assertThat(value).isNotNull();
        assertThat(jsonStat.getSize().get(0)).isEqualTo(3);

        assertThat(value.get(0)).isEqualTo(1);
        assertThat(value.get(1)).isEqualTo(2);
        assertThat(value.get(2)).isEqualTo(3);

        // Check value + dimension coupling using asMap()
        Iterable<Map.Entry<List<String>, Number>> limit = Iterables.limit(jsonStat.asMap().entrySet(), 3);
        assertThat(limit).containsExactly(
                entry(asList("AA"), 1),
                entry(asList("AB"), 2),
                entry(asList("AC"), 3)
        );
    }

    private Set<List<String>> computeIndex(Set<String> keys, Map<String, Set<String>> dimensions) {
        List<Set<String>> rowDimensions = Lists.newArrayList();
        for (String row : keys) {
            rowDimensions.add(ImmutableSet.copyOf(dimensions.get(row)));
        }
        return Sets.cartesianProduct(rowDimensions);
    }


    @Test
    public void testBenchmark() throws IOException {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        Dataset dataset = mapper.readValue(
                new BufferedInputStream(
                        galicia.openStream()
                ),
                DatasetBuildable.class
        ).build();


        // Tables.newCustomTable()

        ImmutableSet<String> rowSet = ImmutableSet.of("time", "birth", "age", "gender", "residence");
        ImmutableSet<String> columnSet = ImmutableSet.of("concept");
        ImmutableMap.Builder<String, ImmutableList<String>> dimensionsBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Dimension> dimensionEntry : dataset.getDimension().entrySet()) {
            String dimensionName = dimensionEntry.getKey();
            ImmutableList<String> dimensionIndex = dimensionEntry.getValue().getCategory().getIndex().asList();
            dimensionsBuilder.put(dimensionName, dimensionIndex);
        }
        ImmutableMap<String, ImmutableList<String>> dimensions = dimensionsBuilder.build();
        List<List<String>> rowDimensions = Lists.newArrayList();
        for (String row : Sets.union(rowSet, columnSet)) {
            rowDimensions.add(ImmutableSet.copyOf(dimensions.get(row)).asList());
        }
        List<List<String>> cartesianProduct = Lists.cartesianProduct(rowDimensions);


        ImmutableTable<List<String>, List<String>, Number> staticCartesian = dataset.getValue().entrySet().stream()
                .collect(ImmutableTable.toImmutableTable(
                        r -> {
                            return cartesianProduct.get(r.getKey()).subList(0, rowSet.size());
                        }, // row,
                        c -> {
                            return cartesianProduct.get(c.getKey()).subList(rowSet.size(), rowSet.size() + columnSet.size());
                        },
                        Map.Entry::getValue
                ));

        Table<List<String>, List<String>, Number> staticCartesianSparse = StreamUtils.zipWithIndex(cartesianProduct.stream())
                .collect(ImmutableTable.toImmutableTable(
                        r -> {
                            return r.getValue().subList(0, rowSet.size());
                        }, // row,
                        c -> {
                            return c.getValue().subList(rowSet.size(), rowSet.size() + columnSet.size());
                        },
                        o -> dataset.getValue().getOrDefault(Ints.checkedCast(o.getIndex()), 0)
                ));

        Table<List<String>, List<String>, Number> view = new DatasetTableView(
                dataset, rowSet, columnSet);

        List<List<String>> rowKeys = Lists.newArrayListWithCapacity(10000);
        List<Map<List<String>, Number>> columnMaps = Lists.newArrayListWithCapacity(10000);

        List<Table<List<String>, List<String>, Number>> tests = Arrays.asList(
        //        staticCartesian,
          //      staticCartesianSparse,
                view
        );
        for (Table<List<String>, List<String>, Number> test : tests) {
            System.out.println("Testing: " + test);
            for (int j = 0; j < 50; j++) {
                Stopwatch sw = Stopwatch.createUnstarted();
                int n = 10000;
                for (int i = 0; i < n; i++) {
                    sw.start();
                    test.rowMap().forEach((strings, listNumberMap) -> {
                        rowKeys.add(strings);
                        columnMaps.add(listNumberMap);
                    });
                    sw.stop();
                    rowKeys.clear();
                    columnMaps.clear();
                }
                System.out.println(sw.elapsed().dividedBy(n).toNanos());
            }
        }

        System.out.println(rowKeys);
        System.out.println(columnMaps);

    }

    @Test
    public void testGalicia() throws Exception {

        URL galicia = Resources.getResource(getClass(), "./galicia.json");

        Dataset jsonStat = mapper.readValue(
                new BufferedInputStream(
                        galicia.openStream()
                ),
                DatasetBuildable.class
        ).build();

        assertThat(jsonStat.getVersion()).isEqualTo("2.0");
        assertThat(jsonStat.getClazz()).isEqualTo("dataset");
        assertThat(jsonStat.getLabel()).contains("Population by province of residence, place of birth, age, gender and year in Galicia");
        assertThat(jsonStat.getSource()).contains("INE and IGE");
        assertThat(jsonStat.getUpdated()).contains(Instant.parse("2012-12-27T12:25:09Z"));

        Iterable<Map.Entry<List<String>, Number>> limit = Iterables.limit(jsonStat.asMap().entrySet(), 5);
        assertThat(limit).containsExactly(
                entry(asList("T", "T", "T", "2001", "T", "pop"), 2695880),
                entry(asList("T", "T", "T", "2001", "15", "pop"), 1096027),
                entry(asList("T", "T", "T", "2001", "27", "pop"), 357648),
                entry(asList("T", "T", "T", "2001", "32", "pop"), 338446),
                entry(asList("T", "T", "T", "2001", "36", "pop"), 903759)
        );

    }
}

/**
 * Copyright (C) 2016 Hadrien Kohl (hadrien.kohl@gmail.com) and contributors
 *
 *     DimensionNotFoundException.java
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

/**
 * Created by hadrien on 15/06/16.
 */
public class DimensionNotFoundException extends RuntimeException {

    private final String dimensionName;
    private final Dataset dataset;

    public DimensionNotFoundException(String message, String dimensionName, Dataset dataset) {
        super(message);
        this.dimensionName = dimensionName;
        this.dataset = dataset;
    }

    public String getDimensionName() {
        return dimensionName;
    }

    public Dataset getDataset() {
        return dataset;
    }
}

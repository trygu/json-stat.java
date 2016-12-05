/**
 * Copyright (C) 2016 Hadrien Kohl (hadrien.kohl@gmail.com) and contributors
 *
 *     JsonStat.java
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
package no.ssb.jsonstat;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by hadrien on 07/06/16.
 *
 * @see <a href="https://json-stat.org/format/#version">json-stat.org/format/#version</a>
 */
public class JsonStat {

    private final Version version;

    private final Class clazz;

    public JsonStat(Version version, Class clazz) {
        this.version = version;
        this.clazz = clazz;
    }

    public String getVersion() {
        return version.getTag();
    }

    @JsonProperty("class")
    public String getClazz() {
        return clazz.toString().toLowerCase();
    }

    public enum Version {

        ONE("1.0"), TWO("2.0");

        private final String tag;

        Version(final String tag) {
            this.tag = tag;
        }

        String getTag() {
            return this.tag;
        }
    }

    public enum Class {
        DATASET,
        DIMENSION,
        COLLECTION
    }

}

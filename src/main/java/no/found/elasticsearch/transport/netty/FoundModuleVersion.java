/*
 * This is the MIT license: http://www.opensource.org/licenses/mit-license.php
 *
 * Copyright (c) 2010-2012, Found IT A/S.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
 * to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package no.found.elasticsearch.transport.netty;

public class FoundModuleVersion {

    // The logic for ID is: XXYYZZAAAA, where XX is major version, YY is minor version, ZZ is revision, and AAAA is an ES
    // version identifier

    // the (internal) format of the id is there so we can easily do after/before checks on the id

    public static final int V_0_8_5_1000_ID = /*000*/8051000;
    public static final FoundModuleVersion V_0_8_5_1000 = new FoundModuleVersion(V_0_8_5_1000_ID);

    public static final int V_0_8_5_0907_ID = /*000*/8050907;
    public static final FoundModuleVersion V_0_8_5_0907 = new FoundModuleVersion(V_0_8_5_0907_ID);

    public static final int V_0_8_5_0902_ID = /*000*/8050902;
    public static final FoundModuleVersion V_0_8_5_0902 = new FoundModuleVersion(V_0_8_5_0902_ID);

    public static final int V_0_8_6_1000_ID = /*000*/8061000;
    public static final FoundModuleVersion V_0_8_6_1000 = new FoundModuleVersion(V_0_8_6_1000_ID);

    public static final int V_0_8_6_0907_ID = /*000*/8060907;
    public static final FoundModuleVersion V_0_8_6_0907 = new FoundModuleVersion(V_0_8_6_0907_ID);

    public static final int V_0_8_6_0902_ID = /*000*/8060902;
    public static final FoundModuleVersion V_0_8_6_0902 = new FoundModuleVersion(V_0_8_6_0902_ID);

    public static final int V_0_8_7_1_00_00_ID = /*000*/80710000;
    public static final FoundModuleVersion V_0_8_7_1_00_00 = new FoundModuleVersion(V_0_8_7_1_00_00_ID);

    public static final int V_0_8_7_0_90_3_ID = /*000*/80709003;
    public static final FoundModuleVersion V_0_8_7_0_90_3 = new FoundModuleVersion(V_0_8_7_0_90_3_ID);

    public static final int V_0_8_7_0_20_0_ID = /*000*/80702000;
    public static final FoundModuleVersion V_0_8_7_0_20_0 = new FoundModuleVersion(V_0_8_7_0_20_0_ID);

    public static final FoundModuleVersion CURRENT = V_0_8_7_1_00_00;

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;

    FoundModuleVersion(int id) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FoundModuleVersion version = (FoundModuleVersion) o;

        return id == version.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}

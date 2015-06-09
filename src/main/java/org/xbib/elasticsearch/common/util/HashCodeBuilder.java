/*
 * Copyright (C) 2014 Jörg Prante
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
package org.xbib.elasticsearch.common.util;

public class HashCodeBuilder {

    private final int iConstant;

    private int iTotal = 0;

    public HashCodeBuilder() {
        this(17, 37);
    }

    public HashCodeBuilder(int initialNonZeroOddNumber, int multiplierNonZeroOddNumber) {
        if (initialNonZeroOddNumber == 0) {
            throw new IllegalArgumentException("HashCodeBuilder requires a non zero initial value");
        }
        if (initialNonZeroOddNumber % 2 == 0) {
            throw new IllegalArgumentException("HashCodeBuilder requires an odd initial value");
        }
        if (multiplierNonZeroOddNumber == 0) {
            throw new IllegalArgumentException("HashCodeBuilder requires a non zero multiplier");
        }
        if (multiplierNonZeroOddNumber % 2 == 0) {
            throw new IllegalArgumentException("HashCodeBuilder requires an odd multiplier");
        }
        iTotal = initialNonZeroOddNumber;
        iConstant = multiplierNonZeroOddNumber;
    }

    public HashCodeBuilder append(boolean value) {
        iTotal = iTotal * iConstant + (value ? 0 : 1);
        return this;
    }

    public HashCodeBuilder append(boolean[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (boolean anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(byte value) {
        iTotal = iTotal * iConstant + value;
        return this;
    }

    public HashCodeBuilder append(byte[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (byte anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(char value) {
        iTotal = iTotal * iConstant + value;
        return this;
    }

    public HashCodeBuilder append(char[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (char anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(double value) {
        return append(Double.doubleToLongBits(value));
    }

    public HashCodeBuilder append(double[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (double anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(float value) {
        iTotal = iTotal * iConstant + Float.floatToIntBits(value);
        return this;
    }

    public HashCodeBuilder append(float[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (float anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(int value) {
        iTotal = iTotal * iConstant + value;
        return this;
    }

    public HashCodeBuilder append(int[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (int anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(long value) {
        iTotal = iTotal * iConstant + ((int) (value ^ (value >> 32)));
        return this;
    }

    public HashCodeBuilder append(long[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (long anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(Object object) {
        if (object == null) {
            iTotal = iTotal * iConstant;

        } else {
            if (object instanceof long[]) {
                append((long[]) object);
            } else if (object instanceof int[]) {
                append((int[]) object);
            } else if (object instanceof short[]) {
                append((short[]) object);
            } else if (object instanceof char[]) {
                append((char[]) object);
            } else if (object instanceof byte[]) {
                append((byte[]) object);
            } else if (object instanceof double[]) {
                append((double[]) object);
            } else if (object instanceof float[]) {
                append((float[]) object);
            } else if (object instanceof boolean[]) {
                append((boolean[]) object);
            } else if (object instanceof Object[]) {
                append((Object[]) object);
            } else {
                iTotal = iTotal * iConstant + object.hashCode();
            }
        }
        return this;
    }

    public HashCodeBuilder append(Object[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (Object anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public HashCodeBuilder append(short value) {
        iTotal = iTotal * iConstant + value;
        return this;
    }

    public HashCodeBuilder append(short[] array) {
        if (array == null) {
            iTotal = iTotal * iConstant;
        } else {
            for (short anArray : array) {
                append(anArray);
            }
        }
        return this;
    }

    public int toHashCode() {
        return iTotal;
    }

}
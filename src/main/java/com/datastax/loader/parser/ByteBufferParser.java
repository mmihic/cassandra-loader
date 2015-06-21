/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parser;

import java.lang.String;
import java.nio.ByteBuffer;
import java.lang.IndexOutOfBoundsException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import javax.xml.bind.DatatypeConverter;

// ByteBuffer parser
public class ByteBufferParser extends AbstractParser {
    private static final Pattern HEX_BINARY = Pattern.compile("^0x([A-Fa-f0-9]+)$");

    public ByteBuffer parse(String toparse) {
        if (null == toparse)
            return null;
        Matcher m = HEX_BINARY.matcher(toparse);
        if (m.matches()) {
            return hexStringToByteBuffer(m.group(1));
        }
        ByteBuffer bb = ByteBuffer.allocate(toparse.length());
        return bb.put(toparse.getBytes());
    }

    private ByteBuffer hexStringToByteBuffer(String s) {
        byte[] data = DatatypeConverter.parseHexBinary(s);
        return ByteBuffer.wrap(data);
    }


    public String format(Row row, int index) throws IndexOutOfBoundsException, InvalidTypeException {
        // TODO: NOT SURE ABOUT THIS ONE
        return row.isNull(index) ? null : row.getBytes(index).toString();
    }
}

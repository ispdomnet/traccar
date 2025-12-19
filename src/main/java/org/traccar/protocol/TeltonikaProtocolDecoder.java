/*
 * Copyright 2013 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.traccar.BaseProtocolDecoder;
import org.traccar.helper.BufferUtil;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.config.Keys;
import org.traccar.helper.BitUtil;
import org.traccar.helper.Checksum;
import org.traccar.helper.UnitsConverter;
import org.traccar.model.CellTower;
import org.traccar.model.Network;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import java.io.BufferedReader;
import java.io.FileReader;

public class TeltonikaProtocolDecoder extends BaseProtocolDecoder {


    private static final Map<Long, String> OPERATORS = new HashMap<>();

    static {
        try (BufferedReader reader = new BufferedReader(new FileReader("/opt/traccar/data/operators.csv"))) {
            String line;
            reader.readLine(); // skip header

            while ((line = reader.readLine()) != null) {
                String[] p = line.split(",");
                int mcc = Integer.parseInt(p[0].trim());
                int mnc = Integer.parseInt(p[1].trim());

                long key = mcc * 100 + mnc;
                OPERATORS.put(key, p[2].trim());
            }
        } catch (Exception e) {
            System.err.println("Failed to load operators: " + e.getMessage());
        }
    }


    private static final int IMAGE_PACKET_MAX = 2048;

    private static final Map<Integer, Map<Predicate<String>, BiConsumer<Position, ByteBuf>>> PARAMETERS =
            new HashMap<>();

    private final boolean connectionless;
    private boolean extended;
    private final Map<Long, ByteBuf> photos = new HashMap<>();

    public void setExtended(boolean extended) {
        this.extended = extended;
    }

    public TeltonikaProtocolDecoder(Protocol protocol, boolean connectionless) {
        super(protocol);
        this.connectionless = connectionless;
    }

    @Override
    protected void init() {
        this.extended = getConfig().getBoolean(Keys.PROTOCOL_EXTENDED.withPrefix(getProtocolName()));
    }

    private void parseIdentification(Channel channel, SocketAddress remoteAddress, ByteBuf buf) {

        int length = buf.readUnsignedShort();
        String imei = buf.toString(buf.readerIndex(), length, StandardCharsets.US_ASCII);
        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);

        if (channel != null) {
            ByteBuf response = Unpooled.buffer(1);
            if (deviceSession != null) {
                response.writeByte(1);
            } else {
                response.writeByte(0);
            }
            channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
        }
    }

    public static final int CODEC_GH3000 = 0x07;
    public static final int CODEC_8 = 0x08;
    public static final int CODEC_8_EXT = 0x8E;
    public static final int CODEC_12 = 0x0C;
    public static final int CODEC_13 = 0x0D;
    public static final int CODEC_16 = 0x10;

    private void sendImageRequest(Channel channel, SocketAddress remoteAddress, long id, int offset, int size) {
        if (channel != null) {
            ByteBuf response = Unpooled.buffer();
            response.writeInt(0);
            response.writeShort(0);
            response.writeShort(19); // length
            response.writeByte(CODEC_12);
            response.writeByte(1); // nod
            response.writeByte(0x0D); // camera
            response.writeInt(11); // payload length
            response.writeByte(2); // command
            response.writeInt((int) id);
            response.writeInt(offset);
            response.writeShort(size);
            response.writeByte(1); // nod
            response.writeShort(0);
            response.writeShort(Checksum.crc16(
                    Checksum.CRC16_IBM, response.nioBuffer(8, response.readableBytes() - 10)));
            channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
        }
    }

    private void decodeSerial(
            Channel channel, SocketAddress remoteAddress, DeviceSession deviceSession, Position position, ByteBuf buf) {

        getLastLocation(position, null);

        int type = buf.readUnsignedByte();
        if (type == 0x0D) {

            buf.readInt(); // length
            int subtype = buf.readUnsignedByte();
            if (subtype == 0x01) {

                long photoId = buf.readUnsignedInt();
                ByteBuf photo = Unpooled.buffer(buf.readInt());
                photos.put(photoId, photo);
                sendImageRequest(
                        channel, remoteAddress, photoId,
                        0, Math.min(IMAGE_PACKET_MAX, photo.capacity()));

            } else if (subtype == 0x02) {

                long photoId = buf.readUnsignedInt();
                buf.readInt(); // offset
                ByteBuf photo = photos.get(photoId);
                photo.writeBytes(buf, buf.readUnsignedShort());
                if (photo.writableBytes() > 0) {
                    sendImageRequest(
                            channel, remoteAddress, photoId,
                            photo.writerIndex(), Math.min(IMAGE_PACKET_MAX, photo.writableBytes()));
                } else {
                    photos.remove(photoId);
                    try {
                        position.set(Position.KEY_IMAGE, writeMediaFile(deviceSession.getUniqueId(), photo, "jpg"));
                    } finally {
                        photo.release();
                    }
                }

            }

        } else {

            position.set(Position.KEY_TYPE, type);

            int length = buf.readInt();
            if (BufferUtil.isPrintable(buf, length)) {
                String data = buf.readSlice(length).toString(StandardCharsets.US_ASCII).trim();
                if (data.startsWith("UUUUww") && data.endsWith("SSS")) {
                    String[] values = data.substring(6, data.length() - 4).split(";");
                    for (int i = 0; i < 8; i++) {
                        position.set("axle" + (i + 1), Double.parseDouble(values[i]));
                    }
                    position.set("loadTruck", Double.parseDouble(values[8]));
                    position.set("loadTrailer", Double.parseDouble(values[9]));
                    position.set("totalTruck", Double.parseDouble(values[10]));
                    position.set("totalTrailer", Double.parseDouble(values[11]));
                } else {
                    position.set(Position.KEY_RESULT, data);
                }
            } else {
                position.set(Position.KEY_RESULT, ByteBufUtil.hexDump(buf.readSlice(length)));
            }
        }
    }

    private long readValue(ByteBuf buf, int length) {
        return switch (length) {
            case 1 -> buf.readUnsignedByte();
            case 2 -> buf.readUnsignedShort();
            case 4 -> buf.readUnsignedInt();
            default -> buf.readLong();
        };
    }

    private static String readVariableAscii(ByteBuf buf) {
        int len = buf.readUnsignedByte();
        byte[] data = new byte[len];
        buf.readBytes(data);
        return new String(data, StandardCharsets.US_ASCII);
    }

    private static void register(int id, Predicate<String> predicate, BiConsumer<Position, ByteBuf> handler) {
        PARAMETERS.computeIfAbsent(id, key -> new HashMap<>()).put(predicate, handler);
    }

    private static Long getLongAttr(Position p, String key) {
        Object v = p.getAttributes().get(key);
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return null;
    }

    static {
        Predicate<String> any = (m) -> true;
        Predicate<String> fmbXXX = (m) -> m != null && (m.startsWith("FM") || m.equals("MTB100") || m.equals("MSP500"));
        Predicate<String> fmb6XX = (m) -> m != null && m.matches("FM.6..");

        register(24, fmbXXX, (p, b) -> p.setSpeed(UnitsConverter.knotsFromKph(b.readUnsignedShort())));
        register(56, fmb6XX, (p, b) -> p.set("d1CDT", b.readUnsignedShort()));
        register(57, fmb6XX, (p, b) -> p.set("d2CDT", b.readUnsignedShort()));
        register(58, fmb6XX, (p, b) -> p.set("d1CBT", b.readUnsignedShort()));
        register(59, fmb6XX, (p, b) -> p.set("d2CBT", b.readUnsignedShort()));
        register(60, fmb6XX, (p, b) -> p.set("d1SAD", b.readUnsignedShort()));
        register(61, fmb6XX, (p, b) -> p.set("d2SAD", b.readUnsignedShort()));
        register(66, any, (p, b) -> p.set(Position.KEY_POWER, b.readUnsignedShort() * 0.001));
        register(67, any, (p, b) -> p.set(Position.KEY_BATTERY, b.readUnsignedShort() * 0.001));
        register(69, fmb6XX, (p, b) -> p.set("d1CmDT", b.readUnsignedShort()));
        register(78, fmb6XX, (p, b) -> {
            long driverUniqueId = b.readLongLE();
            if (driverUniqueId != 0) {
                p.set(Position.KEY_DRIVER_UNIQUE_ID, String.format("%016X", driverUniqueId));
            }
        });
        register(80, fmb6XX, (p, b) -> p.set("wheelBasedSpeed", b.readUnsignedInt()));
        register(84, fmb6XX, (p, b) -> p.set("accelerationPedalPosition", b.readUnsignedInt()));
        register(86, fmbXXX, (p, b) -> p.set(Position.KEY_FUEL_USED, b.readUnsignedInt()));
        register(87, fmbXXX, (p, b) -> p.set(Position.KEY_FUEL_LEVEL, b.readUnsignedInt()));
        register(88, fmbXXX, (p, b) -> p.set(Position.KEY_RPM, b.readUnsignedShort()));
        register(104, fmbXXX, (p, b) -> p.set(Position.KEY_HOURS, b.readUnsignedInt() * 3600000));
        register(113, fmbXXX, (p, b) -> p.set(Position.KEY_ODOMETER_SERVICE, b.readInt() * 1000));
        register(123, fmb6XX, (p, b) -> p.set("tachoPerformance", b.readUnsignedByte()));
        register(127, fmb6XX, (p, b) -> p.set("engineCT", b.readByte() - 40));
        register(128, fmb6XX, (p, b) -> p.set("ambientTemp", b.readShort()));
        register(135, fmbXXX, (p, b) -> p.set(Position.KEY_FUEL_CONSUMPTION, b.readUnsignedShort()));
        register(139, fmb6XX, (p, b) -> p.set("grossCombVWeight", b.readUnsignedInt()));
        register(181, any, (p, b) -> p.set(Position.KEY_PDOP, b.readUnsignedShort() * 0.1));
        register(182, any, (p, b) -> p.set(Position.KEY_HDOP, b.readUnsignedShort() * 0.1));
        register(191, fmb6XX, (p, b) -> p.set("tahoSpeed", UnitsConverter.knotsFromKph(b.readUnsignedShort())));
        //register(192, fmbXXX, (p, b) -> p.set(Position.KEY_ODOMETER, b.readUnsignedInt()));
        //register(193, fmbXXX, (p, b) -> p.set(Position.KEY_ODOMETER_TRIP, b.readUnsignedInt()));
        register(194, fmb6XX, (p, b) -> p.set("timestamp", b.readUnsignedInt()));
        register(201, fmb6XX, (p, b) -> p.set("llc1FuelLevel", b.readShort()));
        register(203, fmb6XX, (p, b) -> p.set("llc2FuelLevel", b.readShort()));
        register(205, fmbXXX, (p, b) -> p.set("cid2g", b.readUnsignedShort()));
        register(206, fmbXXX, (p, b) -> p.set("lac", b.readUnsignedShort()));
        //register(216, fmb6XX, (p, b) -> p.set("totalOdometer_io", b.readUnsignedInt()));
        register(239, any, (p, b) -> p.set(Position.KEY_IGNITION, b.readUnsignedByte() > 0));
        register(240, any, (p, b) -> p.set(Position.KEY_MOTION, b.readUnsignedByte() > 0));
        register(241, any, (p, b) -> p.set(Position.KEY_OPERATOR, b.readUnsignedInt()));

        register(10455, fmb6XX, (p, b) -> p.set("adBL", b.readUnsignedByte()));
        register(10503, fmb6XX, (p, b) -> p.set("nextCalD", b.readUnsignedInt()));
        register(10504, fmb6XX, (p, b) -> p.set("d1EndLDrr", b.readUnsignedInt()));
        register(10505, fmb6XX, (p, b) -> p.set("d1EndLWrp", b.readUnsignedInt()));
        register(10506, fmb6XX, (p, b) -> p.set("d1EndFSlWp", b.readUnsignedInt()));
        register(10507, fmb6XX, (p, b) -> p.set("d1DDT", b.readUnsignedShort()));
        register(10508, fmb6XX, (p, b) -> p.set("d1WDT", b.readUnsignedShort()));
        register(10509, fmb6XX, (p, b) -> p.set("d1TLDRP", b.readUnsignedShort()));
        register(10522, fmb6XX, (p, b) -> p.set("d1TLWRP", b.readUnsignedShort()));
        register(10524, fmb6XX, (p, b) -> p.set("d1MinDR", b.readUnsignedShort()));
        register(10526, fmb6XX, (p, b) -> p.set("d1MinWR", b.readUnsignedShort()));
        register(10528, fmb6XX, (p, b) -> p.set("d1DoNBR", b.readUnsignedShort()));
        register(10530, fmb6XX, (p, b) -> p.set("d1RTUNBR", b.readUnsignedShort()));
        register(10532, fmb6XX, (p, b) -> p.set("d1RCDT", b.readUnsignedShort()));
        register(10533, fmb6XX, (p, b) -> p.set("d1RDTS", b.readUnsignedShort()));
        register(10534, fmb6XX, (p, b) -> p.set("d1RDTW", b.readUnsignedShort()));
        register(10535, fmb6XX, (p, b) -> p.set("d1OC1W", b.readUnsignedShort()));
        register(10536, fmb6XX, (p, b) -> p.set("d1OC2W", b.readUnsignedShort()));
        register(10537, fmb6XX, (p, b) -> p.set("d1OC3W", b.readUnsignedShort()));
        register(10538, fmb6XX, (p, b) -> p.set("d1Ainfo", b.readUnsignedShort()));
        register(10539, fmb6XX, (p, b) -> p.set("d1RTCBR", b.readUnsignedShort()));
        register(10540, fmb6XX, (p, b) -> p.set("d1TLNDP", b.readUnsignedShort()));
        register(10541, fmb6XX, (p, b) -> p.set("d1DoNDP", b.readUnsignedShort()));

        register(10800, fmbXXX, (p, b) -> p.set("eyeTemp1", b.readShort() / 100.0));
    }

    private void decodeGh3000Parameter(Position position, int id, ByteBuf buf, int length) {
        switch (id) {
            case 1 -> position.set(Position.KEY_BATTERY_LEVEL, readValue(buf, length));
            case 2 -> position.set("usbConnected", readValue(buf, length) == 1);
            case 5 -> position.set("uptime", readValue(buf, length));
            case 20 -> position.set(Position.KEY_HDOP, readValue(buf, length) * 0.1);
            case 21 -> position.set(Position.KEY_VDOP, readValue(buf, length) * 0.1);
            case 22 -> position.set(Position.KEY_PDOP, readValue(buf, length) * 0.1);
            case 67 -> position.set(Position.KEY_BATTERY, readValue(buf, length) * 0.001);
            case 221 -> position.set("button", readValue(buf, length));
            case 222 -> {
                if (readValue(buf, length) == 1) {
                    position.addAlarm(Position.ALARM_SOS);
                }
            }
            case 240 -> position.set(Position.KEY_MOTION, readValue(buf, length) == 1);
            case 244 -> position.set(Position.KEY_ROAMING, readValue(buf, length) == 1);
            default -> position.set(Position.PREFIX_IO + id, readValue(buf, length));
        }
    }

    private void decodeParameter(Position position, int id, ByteBuf buf, int length, int codec, String model) {
        if (codec == CODEC_GH3000) {
            decodeGh3000Parameter(position, id, buf, length);
        } else {
            int index = buf.readerIndex();
            boolean decoded = false;
            for (var entry : PARAMETERS.getOrDefault(id, new HashMap<>()).entrySet()) {
                if (entry.getKey().test(model)) {
                    entry.getValue().accept(position, buf);
                    decoded = true;
                    break;
                }
            }
            if (decoded) {
                buf.readerIndex(index + length);
            } else {
                position.set(Position.PREFIX_IO + id, readValue(buf, length));
            }
        }
    }

    private void decodeCell(
            Position position, Network network, String mncKey, String lacKey, String cidKey, String rssiKey) {
        if (position.hasAttribute(mncKey) && position.hasAttribute(lacKey) && position.hasAttribute(cidKey)) {
            CellTower cellTower = CellTower.from(
                    getConfig().getInteger(Keys.GEOLOCATION_MCC),
                    position.removeInteger(mncKey),
                    position.removeInteger(lacKey),
                    position.removeLong(cidKey));
            cellTower.setSignalStrength(position.removeInteger(rssiKey));
            network.addCellTower(cellTower);
        }
    }

    private void decodeNetwork(Position position, String model) {
        if ("TAT100".equals(model)) {
            Network network = new Network();
            decodeCell(position, network, "io1200", "io287", "io288", "io289");
            decodeCell(position, network, "io1201", "io290", "io291", "io292");
            decodeCell(position, network, "io1202", "io293", "io294", "io295");
            decodeCell(position, network, "io1203", "io296", "io297", "io298");
            if (network.getCellTowers() != null) {
                position.setNetwork(network);
            }
        } else {
            Integer cid2g = position.removeInteger("cid2g");
            Long cid4g = position.removeLong("cid4g");
            Integer lac = position.removeInteger("lac");
            if (lac != null && (cid2g != null || cid4g != null)) {
                Network network = new Network();
                CellTower cellTower;
                if (cid2g != null) {
                    cellTower = CellTower.fromLacCid(getConfig(), lac, cid2g);
                } else {
                    cellTower = CellTower.fromLacCid(getConfig(), lac, cid4g);
                    network.setRadioType("lte");
                }
                long operator = position.getInteger(Position.KEY_OPERATOR);

                String operatorName = OPERATORS.getOrDefault(operator, String.valueOf(operator));

                cellTower.setOperator(operator);
                position.set("operatorName", operatorName);

                network.addCellTower(cellTower);
                position.setNetwork(new Network(cellTower));

            }
        }
    }

    private int readExtByte(ByteBuf buf, int codec, int... codecs) {
        boolean ext = false;
        for (int c : codecs) {
            if (codec == c) {
                ext = true;
                break;
            }
        }
        if (ext) {
            return buf.readUnsignedShort();
        } else {
            return buf.readUnsignedByte();
        }
    }

    private void decodeLocation(Position position, ByteBuf buf, int codec, String model) {

        int globalMask = 0x0f;

        if (codec == CODEC_GH3000) {

            long time = buf.readUnsignedInt() & 0x3fffffff;
            time += 1167609600; // 2007-01-01 00:00:00

            globalMask = buf.readUnsignedByte();
            if (BitUtil.check(globalMask, 0)) {

                position.setTime(new Date(time * 1000));

                int locationMask = buf.readUnsignedByte();

                if (BitUtil.check(locationMask, 0)) {
                    position.setLatitude(buf.readFloat());
                    position.setLongitude(buf.readFloat());
                }

                if (BitUtil.check(locationMask, 1)) {
                    position.setAltitude(buf.readUnsignedShort());
                }

                if (BitUtil.check(locationMask, 2)) {
                    position.setCourse(buf.readUnsignedByte() * 360.0 / 256);
                }

                if (BitUtil.check(locationMask, 3)) {
                    position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedByte()));
                }

                if (BitUtil.check(locationMask, 4)) {
                    position.set(Position.KEY_SATELLITES, buf.readUnsignedByte());
                }

                if (BitUtil.check(locationMask, 5)) {
                    CellTower cellTower = CellTower.fromLacCid(
                            getConfig(), buf.readUnsignedShort(), buf.readUnsignedShort());

                    if (BitUtil.check(locationMask, 6)) {
                        cellTower.setSignalStrength((int) buf.readUnsignedByte());
                    }

                    if (BitUtil.check(locationMask, 7)) {
                        cellTower.setOperator(buf.readUnsignedInt());
                    }

                    position.setNetwork(new Network(cellTower));

                } else {
                    if (BitUtil.check(locationMask, 6)) {
                        position.set(Position.KEY_RSSI, buf.readUnsignedByte());
                    }
                    if (BitUtil.check(locationMask, 7)) {
                        position.set(Position.KEY_OPERATOR, buf.readUnsignedInt());
                    }
                }

            } else {

                getLastLocation(position, new Date(time * 1000));

            }

        } else {

            position.setTime(new Date(buf.readLong()));

            position.set("priority", buf.readUnsignedByte());

            position.setLongitude(buf.readInt() / 10000000.0);
            position.setLatitude(buf.readInt() / 10000000.0);
            position.setAltitude(buf.readShort());
            position.setCourse(buf.readUnsignedShort());

            int satellites = buf.readUnsignedByte();
            position.set(Position.KEY_SATELLITES, satellites);

            position.setValid(satellites != 0);

            position.setSpeed(UnitsConverter.knotsFromKph(buf.readUnsignedShort()));

            position.set(Position.KEY_EVENT, readExtByte(buf, codec, CODEC_8_EXT, CODEC_16));
            if (codec == CODEC_16) {
                buf.readUnsignedByte(); // generation type
            }

            readExtByte(buf, codec, CODEC_8_EXT); // total IO data records

        }

        // Read 1 byte data
        if (BitUtil.check(globalMask, 1)) {
            int cnt = readExtByte(buf, codec, CODEC_8_EXT);
            for (int j = 0; j < cnt; j++) {
                decodeParameter(position, readExtByte(buf, codec, CODEC_8_EXT, CODEC_16), buf, 1, codec, model);
            }
        }

        // Read 2 byte data
        if (BitUtil.check(globalMask, 2)) {
            int cnt = readExtByte(buf, codec, CODEC_8_EXT);
            for (int j = 0; j < cnt; j++) {
                decodeParameter(position, readExtByte(buf, codec, CODEC_8_EXT, CODEC_16), buf, 2, codec, model);
            }
        }

        // Read 4 byte data
        if (BitUtil.check(globalMask, 3)) {
            int cnt = readExtByte(buf, codec, CODEC_8_EXT);
            for (int j = 0; j < cnt; j++) {
                decodeParameter(position, readExtByte(buf, codec, CODEC_8_EXT, CODEC_16), buf, 4, codec, model);
            }
        }

        // Read 8 byte data
        if (codec == CODEC_8 || codec == CODEC_8_EXT || codec == CODEC_16) {
            int cnt = readExtByte(buf, codec, CODEC_8_EXT);
            for (int j = 0; j < cnt; j++) {
                decodeParameter(position, readExtByte(buf, codec, CODEC_8_EXT, CODEC_16), buf, 8, codec, model);
            }
        }

        // Read 16 byte data
        if (extended) {
            int cnt = readExtByte(buf, codec, CODEC_8_EXT);
            for (int j = 0; j < cnt; j++) {
                int id = readExtByte(buf, codec, CODEC_8_EXT, CODEC_16);
                position.set(Position.PREFIX_IO + id, ByteBufUtil.hexDump(buf.readSlice(16)));
            }
        }

        // Read X byte data
        if (codec == CODEC_8_EXT) {
            int cnt = buf.readUnsignedShort();
            for (int j = 0; j < cnt; j++) {
                int id = buf.readUnsignedShort();
                int length = buf.readUnsignedShort();

                //if (id == 256 || id == 325) { //це не VIN код
                if (false) {
                    position.set(Position.KEY_VIN,
                            buf.readSlice(length).toString(StandardCharsets.US_ASCII));
                } else if (id == 281) {
                    position.set(Position.KEY_DTCS,
                            buf.readSlice(length).toString(StandardCharsets.US_ASCII).replace(',', ' '));
                } else if (id == 385) {
                    ByteBuf data = buf.readSlice(length);
                    data.readUnsignedByte(); // data part
                    int index = 1;
                    while (data.isReadable()) {
                        int flags = data.readUnsignedByte();
                        if (BitUtil.from(flags, 4) > 0) {
                            position.set("beacon" + index + "Uuid", ByteBufUtil.hexDump(data.readSlice(16)));
                            position.set("beacon" + index + "Major", data.readUnsignedShort());
                            position.set("beacon" + index + "Minor", data.readUnsignedShort());
                        } else {
                            position.set("beacon" + index + "Namespace", ByteBufUtil.hexDump(data.readSlice(10)));
                            position.set("beacon" + index + "Instance", ByteBufUtil.hexDump(data.readSlice(6)));
                        }
                        position.set("beacon" + index + "Rssi", (int) data.readByte());
                        if (BitUtil.check(flags, 1)) {
                            position.set("beacon" + index + "Battery", data.readUnsignedShort() * 0.01);
                        }
                        if (BitUtil.check(flags, 2)) {
                            position.set("beacon" + index + "Temp", data.readUnsignedShort());
                        }
                        index += 1;
                    }
                } else if (id == 548 || id == 10828 || id == 10829 || id == 10831 || id == 11317) {
                    ByteBuf data = buf.readSlice(length);
                    data.readUnsignedByte(); // header
                    for (int i = 1; data.isReadable(); i++) {
                        ByteBuf beacon = data.readSlice(data.readUnsignedByte());
                        while (beacon.isReadable()) {
                            int parameterId = beacon.readUnsignedByte();
                            int parameterLength = beacon.readUnsignedByte();
                            switch (parameterId) {
                                case 0 -> position.set("tag" + i + "Rssi", (int) beacon.readByte());
                                case 1 -> {
                                    String beaconId = ByteBufUtil.hexDump(beacon.readSlice(parameterLength));
                                    position.set("tag" + i + "Id", beaconId);
                                }
                                case 2 -> {
                                    ByteBuf beaconData = beacon.readSlice(parameterLength);
                                    int flag = beaconData.readUnsignedByte();
                                    if (BitUtil.check(flag, 6)) {
                                        position.set("tag" + i + "LowBattery", true);
                                    }
                                    if (BitUtil.check(flag, 7)) {
                                        position.set("tag" + i + "Voltage", beaconData.readUnsignedByte() * 10 + 2000);
                                    }
                                }
                                case 5 -> {
                                    String name = beacon.readCharSequence(
                                            parameterLength, StandardCharsets.UTF_8).toString();
                                    position.set("tag" + i + "Name", name);
                                }
                                case 6 -> position.set("tag" + i + "Temp", beacon.readShort());
                                case 7 -> position.set("tag" + i + "Humidity", beacon.readUnsignedByte());
                                case 8 -> position.set("tag" + i + "Magnet", beacon.readUnsignedByte() > 0);
                                case 9 -> position.set("tag" + i + "Motion", beacon.readUnsignedByte() > 0);
                                case 10 -> position.set("tag" + i + "MotionCount", beacon.readUnsignedShort());
                                case 11 -> position.set("tag" + i + "Pitch", (int) beacon.readByte());
                                case 12 -> position.set("tag" + i + "AngleRoll", (int) beacon.readShort());
                                case 13 -> position.set("tag" + i + "LowBattery", beacon.readUnsignedByte());
                                case 14 -> position.set("tag" + i + "Battery", beacon.readUnsignedShort());
                                case 15 -> position.set("tag" + i + "Mac", ByteBufUtil.hexDump(beacon.readSlice(6)));
                                default -> beacon.skipBytes(parameterLength);
                            }
                        }
                    }
                } else if (id == 10518 || id == 10519 || id == 10520 || id == 10521) { //ім'я+прізвище водія 1, 2
                    int expected = 36;
                    if (length > buf.readableBytes()) {
                        buf.skipBytes(buf.readableBytes());
                        return;
                    }

                    int readLen = Math.min(length, expected);
                    byte[] bytes = new byte[readLen];
                    buf.readBytes(bytes);

                    String decoded = new String(bytes, StandardCharsets.US_ASCII).trim();
                    position.set(Position.PREFIX_IO + id, decoded);

                    if (length > readLen) {
                        buf.skipBytes(length - readLen);
                    }
                } else {
                        position.set(Position.PREFIX_IO + id, ByteBufUtil.hexDump(buf.readSlice(length)));
                }
            }
        }

        decodeNetwork(position, model); //декодування сім оператора

        if (position.getAttributes().containsKey("llc1FuelLevel")
            && position.getAttributes().containsKey("llc2FuelLevel")) {
        position.set("llcFuelTotal", 0);
        }

        if (model != null && model.matches("FM.6..")) {

            Long msb = getLongAttr(position, "io195");
            Long lsb = getLongAttr(position, "io196");

            if (msb != null && lsb != null && (msb != 0 || lsb != 0)) {
                String driverId = String.format("%016X%016X", msb, lsb);
                position.set(Position.KEY_DRIVER_UNIQUE_ID, driverId);
            }

            position.getAttributes().remove("io195");
            position.getAttributes().remove("io196");

            Long msb2 = getLongAttr(position, "io197");
            Long lsb2 = getLongAttr(position, "io198");

            if (msb2 != null && lsb2 != null && (msb2 != 0 || lsb2 != 0)) {
                String driverId2 = String.format("%016X%016X", msb2, lsb2);
                position.set(Position.KEY_DRIVER_UNIQUE_ID2, driverId2);
            }

            position.getAttributes().remove("io197");
            position.getAttributes().remove("io198");

            Long rnp1 = getLongAttr(position, "io231");
            Long rnp2 = getLongAttr(position, "io232");

            if (rnp1 != null && rnp2 != null && (rnp1 != 0 || rnp2 != 0)) {
                position.set("vehicleRnp",
                    String.format("%016X%016X", rnp1, rnp2));
            }

            position.getAttributes().remove("io231");
            position.getAttributes().remove("io232");

            Long vin1 = getLongAttr(position, "io233");
            Long vin2 = getLongAttr(position, "io234");

            if (vin1 != null && vin2 != null && (vin1 != 0 || vin2 != 0)) {
                position.set(Position.KEY_VIN,
                    String.format("%016X%016X", vin1, vin2));
            }

            position.getAttributes().remove("io233");
            position.getAttributes().remove("io234");
        }
    }

    private List<Position> parseData(
            Channel channel, SocketAddress remoteAddress, ByteBuf buf, int locationPacketId, String... imei) {
        List<Position> positions = new LinkedList<>();

        if (!connectionless) {
            buf.readUnsignedInt(); // data length
        }

        int codec = buf.readUnsignedByte();
        int count = buf.readUnsignedByte();

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
        if (deviceSession == null) {
            return null;
        }

        for (int i = 0; i < count; i++) {
            Position position = new Position(getProtocolName());

            position.setDeviceId(deviceSession.getDeviceId());
            position.setValid(true);

            if (codec == CODEC_13) {
                buf.readUnsignedByte(); // type
                int length = buf.readInt() - 4;
                getLastLocation(position, new Date(buf.readUnsignedInt() * 1000));
                if (BufferUtil.isPrintable(buf, length)) {
                    String data = buf.readCharSequence(length, StandardCharsets.US_ASCII).toString().trim();
                    if (data.startsWith("GTSL")) {
                        position.set(Position.KEY_DRIVER_UNIQUE_ID, data.split("\\|")[4]);
                    } else {
                        position.set(Position.KEY_RESULT, data);
                    }
                } else {
                    position.set(Position.KEY_RESULT,
                            ByteBufUtil.hexDump(buf.readSlice(length)));
                }
            } else if (codec == CODEC_12) {
                decodeSerial(channel, remoteAddress, deviceSession, position, buf);
            } else {
                decodeLocation(position, buf, codec, getDeviceModel(deviceSession));
            }

            if (!position.getOutdated() || !position.getAttributes().isEmpty()) {
                positions.add(position);
            }
        }

        if (channel != null && codec != CODEC_12 && codec != CODEC_13) {
            ByteBuf response = Unpooled.buffer();
            if (connectionless) {
                response.writeShort(5);
                response.writeShort(0);
                response.writeByte(0x01);
                response.writeByte(locationPacketId);
                response.writeByte(count);
            } else {
                response.writeInt(count);
            }
            channel.writeAndFlush(new NetworkMessage(response, remoteAddress));
        }

        return positions.isEmpty() ? null : positions;
    }

    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        ByteBuf buf = (ByteBuf) msg;

        if (connectionless) {
            return decodeUdp(channel, remoteAddress, buf);
        } else {
            return decodeTcp(channel, remoteAddress, buf);
        }
    }

    private Object decodeTcp(Channel channel, SocketAddress remoteAddress, ByteBuf buf) {

        if (buf.readableBytes() == 1 && buf.readUnsignedByte() == 0xff) {
            return null;
        } else if (buf.getUnsignedShort(0) > 0) {
            parseIdentification(channel, remoteAddress, buf);
        } else {
            buf.skipBytes(4);
            return parseData(channel, remoteAddress, buf, 0);
        }

        return null;
    }

    private Object decodeUdp(Channel channel, SocketAddress remoteAddress, ByteBuf buf) {

        buf.readUnsignedShort(); // length
        buf.readUnsignedShort(); // packet id
        buf.readUnsignedByte(); // packet type
        int locationPacketId = buf.readUnsignedByte();
        String imei = buf.readSlice(buf.readUnsignedShort()).toString(StandardCharsets.US_ASCII);

        return parseData(channel, remoteAddress, buf, locationPacketId, imei);

    }

}

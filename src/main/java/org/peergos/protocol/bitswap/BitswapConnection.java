package org.peergos.protocol.bitswap;

import io.libp2p.core.Stream;
import io.prometheus.client.Counter;
import kotlin.Unit;
import org.peergos.protocol.bitswap.pb.MessageOuterClass;

import java.util.concurrent.CompletableFuture;

public class BitswapConnection implements BitswapController {

    private final Stream conn;
    private final Counter sentBytes;

    public BitswapConnection(Stream conn, Counter sentBytes) {
        this.conn = conn;
        this.sentBytes = sentBytes;
    }

    @Override
    public void send(MessageOuterClass.Message msg) {
        conn.writeAndFlush(msg);
        sentBytes.inc(msg.getSerializedSize());
    }

    @Override
    public CompletableFuture<Unit> close() {
        return conn.close();
    }


    @Override
    public String toString() {
        return "BitswapConnection{" +
                "conn=" + conn +
                '}';
    }
}

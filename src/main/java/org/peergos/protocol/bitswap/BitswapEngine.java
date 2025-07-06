package org.peergos.protocol.bitswap;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.PeerId;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.Multiaddr;
import io.prometheus.client.Counter;
import org.peergos.BlockRequestAuthoriser;
import org.peergos.Hash;
import org.peergos.HashedBlock;
import org.peergos.Want;
import org.peergos.blockstore.Blockstore;
import org.peergos.protocol.bitswap.pb.MessageOuterClass;
import org.peergos.util.ArrayOps;
import org.peergos.util.LRUCache;
import org.peergos.util.Logging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class BitswapEngine {
    private static final Logger LOG = Logging.LOG();

    private final Blockstore store;
    private final int maxMessageSize;
    private final ConcurrentHashMap<Want, WantResult> localWants = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, Boolean> persistBlocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, PeerId> blockHaves = new ConcurrentHashMap<>();
    private final Map<Want, Boolean> deniedWants = Collections.synchronizedMap(new LRUCache<>(10_000));
    private final Map<PeerId, Map<Want, Boolean>> recentBlocksSent = Collections.synchronizedMap(new LRUCache<>(100));
    private final Map<PeerId, Map<Want, Long>> recentWantsSent = Collections.synchronizedMap(new LRUCache<>(100));
    private final Map<PeerId, Boolean> blockedPeers = Collections.synchronizedMap(new LRUCache<>(1_000));
    private final boolean blockAggressivePeers;
    private final Set<PeerId> connections = new HashSet<>();
    private final BlockRequestAuthoriser authoriser;
    private AddressBook addressBook;
    public BitswapEngineHandler beHandler;

    private static byte[] prefixBytes(Cid c) {
        ByteArrayOutputStream res = new ByteArrayOutputStream();
        try {
            Cid.putUvarint(res, c.version);
            Cid.putUvarint(res, c.codec.type);
            Cid.putUvarint(res, c.getType().index);
            Cid.putUvarint(res, c.getType().length);
            return res.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public BitswapEngine(Blockstore store, BlockRequestAuthoriser authoriser, int maxMessageSize, boolean blockAggressivePeers) {
        this.store = store;
        this.authoriser = authoriser;
        this.maxMessageSize = maxMessageSize;
        this.blockAggressivePeers = blockAggressivePeers;
    }

    public BitswapEngine(Blockstore store, BlockRequestAuthoriser authoriser, int maxMessageSize) {
        this(store, authoriser, maxMessageSize, false);
    }


    public int maxMessageSize() {
        return maxMessageSize;
    }

    public boolean allowConnection(PeerId peer) {
        return !blockAggressivePeers || !blockedPeers.containsKey(peer);
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public synchronized void addConnection(PeerId peer, Multiaddr addr) {
        connections.add(peer);
    }

    public CompletableFuture<HashedBlock> getWant(Want w, boolean addToBlockstore) {
        WantResult existing = localWants.get(w);
        if (existing != null)
            return existing.result;
        if (addToBlockstore)
            persistBlocks.put(w, true);
        WantResult res = new WantResult(System.currentTimeMillis());
        localWants.put(w, res);
        return res.result;
    }

    public boolean hasWants() {
        return !localWants.isEmpty();
    }

    public Set<PeerId> getConnected() {
        Set<PeerId> connected = new HashSet<>();
        synchronized (connections) {
            connected.addAll(connections);
        }
        return connected;
    }

    private static final class WantResult {
        public final CompletableFuture<HashedBlock> result = new CompletableFuture<>();
        public final long creationTime;

        public WantResult(long creationTime) {
            this.creationTime = creationTime;
        }
    }

    private Map<Want, Long> recentSentWants(PeerId peer) {
        Map<Want, Long> recent = recentWantsSent.get(peer);
        if (recent == null) {
            recent = Collections.synchronizedMap(new LRUCache<>(1000));
            recentWantsSent.put(peer, recent);
        }
        return recent;
    }

    public Set<Want> getWants(Set<PeerId> peers) {
        if (peers.size() == 1) {
            PeerId peer = peers.stream().findFirst().get();
            Map<Want, Long> recent = recentSentWants(peer);

            long now = System.currentTimeMillis();
            long minResendWait = 5_000;
            Set<Want> res = localWants.entrySet().stream()
                    .filter(e -> e.getValue().creationTime > now - 5 * 60 * 1000)
                    .map(e -> e.getKey())
                    .filter(w -> !recent.containsKey(w) || recent.get(w) < now - minResendWait)
                    .collect(Collectors.toSet());
            res.forEach(w -> recent.put(w, now));
            return res;
        }
        return localWants.keySet();
    }

    public Map<Want, PeerId> getHaves() {
        return blockHaves;
    }

    public void receiveMessage(MessageOuterClass.Message msg, Stream source, Counter sentBytes) {
        String peerId = source.remotePeerId().toBase58();

        LOG.info("BITSWAP " + "received message from " + peerId);
        try {
            String json = JsonFormat.printer().print(msg);
            LOG.info("BITSWAP json:\n" + json);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        List<MessageOuterClass.Message.BlockPresence> presences = new ArrayList<>();
        List<MessageOuterClass.Message.Block> blocks = new ArrayList<>();

        int messageSize = 0;
        Multihash peerM = Multihash.deserialize(source.remotePeerId().getBytes());
        Cid sourcePeerId = new Cid(1, Cid.Codec.Libp2pKey, peerM.getType(), peerM.getHash());
        Map<Want, Boolean> recent = recentBlocksSent.get(source.remotePeerId());
        if (recent == null) {
            recent = Collections.synchronizedMap(new LRUCache<>(1_000));
            recentBlocksSent.put(source.remotePeerId(), recent);
        }
        int absentBlocks = 0;
        int presentBlocks = 0;
        if (msg.hasWantlist()) {
            LOG.info("BITSWAP " + "message has list of wants");
            for (MessageOuterClass.Message.Wantlist.Entry e : msg.getWantlist().getEntriesList()) {
                if (e.getCancel()) continue;
                Cid c;
                try {
                    c = Cid.cast(e.getBlock().toByteArray());
                } catch (Exception ex) {
                    LOG.info("BITSWAP " + "unable to cast " + e);
                    continue;
                }
                Optional<String> auth = e.getAuth().isEmpty() ? Optional.empty() : Optional.of(ArrayOps.bytesToHex(e.getAuth().toByteArray()));

                boolean sendDontHave = e.getSendDontHave();
                boolean wantBlock = e.getWantType().getNumber() == 0;
                Want w = new Want(c, auth);


                if (wantBlock) {
                    LOG.info("BITSWAP " + "Entry WantType is BLOCK ");
                    boolean denied = deniedWants.containsKey(w);
                    if (denied) {
                        LOG.info("BITSWAP " + w.cid + " was denied");
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                        continue;
                    }
                    if (recent.containsKey(w)) {
                        LOG.info("BITSWAP" + w.cid + "in recently sent blocks. Resent any way");
                    }
                    boolean blockPresent = store.has(c).join();
                    if (!blockPresent)
                        absentBlocks++;
                    else
                        presentBlocks++;
                    if (blockPresent && authoriser.allowRead(c, sourcePeerId, auth.orElse("")).join()) {
                        LOG.info("BITSWAP block present and allowed to read by authoriser");
                        MessageOuterClass.Message.Block blockP = MessageOuterClass.Message.Block.newBuilder()
                                .setPrefix(ByteString.copyFrom(prefixBytes(c)))
                                .setAuth(ByteString.copyFrom(ArrayOps.hexToBytes(auth.orElse(""))))
                                .setData(ByteString.copyFrom(store.get(c).join().get()))
                                .build();
                        int blockSize = blockP.getSerializedSize();
                        if (blockSize + messageSize > maxMessageSize) {
                            LOG.info("BITSWAP We've exceeded the message size, so we're sending it now");
                            buildAndSendMessages(emptyList(), presences, blocks, source::writeAndFlush);
                            presences = new ArrayList<>();
                            blocks = new ArrayList<>();
                            messageSize = 0;
                        }
                        messageSize += blockSize;
                        LOG.info("BITSWAP Added a block to the message being sent." + blockP);
                        blocks.add(blockP);
                        recent.put(w, true);
                    } else if (sendDontHave) {
                        LOG.info("BITSWAP block don't present or not allowed to read by authoriser and we must report that there is no block.");
                        if (blockPresent) {
                            deniedWants.put(w, true);
                            LOG.info("BITSWAP The authorizer has denied access to the block we have. " + w.cid);
                        }
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else if (blockPresent) {
                        LOG.info("BITSWAP block present, but not allowed to read by authoriser and we don't must report that there is no block.");
                        deniedWants.put(w, true);
                        LOG.info("BITSWAP The authorizer has denied access to the block we have. " + w.cid);
                    }
                } else {
                    LOG.info("BITSWAP " + "Entry WantType is HAVE");
                    boolean hasBlock = store.has(c).join();
                    if (hasBlock) {
                        LOG.info("BITSWAP " + "we have block with cid:" + c);
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.Have)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else if (sendDontHave) {
                        LOG.info("BITSWAP " + " we don't have this block and must send reply about it " + c);
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else {
                        LOG.info("BITSWAP " + " we don't have this block and must not send reply about it " + c);
                    }
                }
            }
        }

        LOG.fine("BITSWAP Bitswap received " + msg.getWantlist().getEntriesCount() + " wants, " + msg.getPayloadCount() +
                " blocks and " + msg.getBlockPresencesCount() + " presences from " + sourcePeerId.toBase58());
        boolean receivedWantedBlock = false;
        for (MessageOuterClass.Message.Block block : msg.getPayloadList()) {
            byte[] cidPrefix = block.getPrefix().toByteArray();
            Optional<String> auth = block.getAuth().isEmpty() ?
                    Optional.empty() :
                    Optional.of(ArrayOps.bytesToHex(block.getAuth().toByteArray()));
            byte[] data = block.getData().toByteArray();
            ByteArrayInputStream bin = new ByteArrayInputStream(cidPrefix);
            try {
                long version = Cid.readVarint(bin);
                Cid.Codec codec = Cid.Codec.lookup(Cid.readVarint(bin));
                Multihash.Type type = Multihash.Type.lookup((int) Cid.readVarint(bin));
//                int hashSize = (int)Cid.readVarint(bin);
                if (type != Multihash.Type.sha2_256) {
                    LOG.info("BITSWAP Unsupported hash algorithm " + type.name());
                } else {
                    byte[] hash = Hash.sha256(data);
                    Cid c = new Cid(version, codec, type, hash);
                    Want w = new Want(c, auth);
                    WantResult waiter = localWants.get(w);
                    if (waiter != null) {
                        receivedWantedBlock = true;
                        if (persistBlocks.containsKey(w)) {
                            store.put(data, codec);
                            persistBlocks.remove(w);
                        }
                        waiter.result.complete(new HashedBlock(c, data));
                        localWants.remove(w);
                        LOG.info("BITSWAP We got the block we wanted. " + c);
                    } else
                        LOG.info("BITSWAP Received block we don't want: " + c + " from " + sourcePeerId.bareMultihash());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (!localWants.isEmpty()) {
            LOG.info("BITSWAP Local wishlist still not empty. ");
            LOG.fine("BITSWAP Remaining: " + localWants.size());
        }
        boolean receivedRequestedHave = false;
        for (MessageOuterClass.Message.BlockPresence blockPresence : msg.getBlockPresencesList()) {
            Cid c = Cid.cast(blockPresence.getCid().toByteArray());
            Optional<String> auth = blockPresence.getAuth().isEmpty() ?
                    Optional.empty() :
                    Optional.of(ArrayOps.bytesToHex(blockPresence.getAuth().toByteArray()));
            Want w = new Want(c, auth);
            boolean have = blockPresence.getType().getNumber() == 0;
            if (have && localWants.containsKey(w)) {
                receivedRequestedHave = true;
                blockHaves.put(w, source.remotePeerId());
            }
        }
        if (absentBlocks > 10 && presentBlocks == 0 && !receivedRequestedHave && !receivedWantedBlock) {
            // This peer is sending us lots of irrelevant requests, block them
            LOG.info("BITSWAP This peer is sending us lots of irrelevant requests, block them");
            blockedPeers.put(source.remotePeerId(), true);
            source.close();
        }

        if (presences.isEmpty() && blocks.isEmpty()) {
            LOG.info("BITSWAP presences and blocks is empty. return");
            return;
        }

        buildAndSendMessages(emptyList(), presences, blocks, reply -> {
                    try {
                        String json = JsonFormat.printer().print(reply);
                        LOG.info("BITSWAP reply to be sent: " + json);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }

                    Multiaddr addr = source.getConnection().remoteAddress().withP2P(source.remotePeerId());
                    source.writeAndFlush(reply); // don't work with kubo
                    CompletableFuture.runAsync(() -> this.beHandler.execute(reply, addr));
                }
        );
    }

    public void buildAndSendMessages(List<MessageOuterClass.Message.Wantlist.Entry> wants,
                                     List<MessageOuterClass.Message.BlockPresence> presences,
                                     List<MessageOuterClass.Message.Block> blocks,
                                     Consumer<MessageOuterClass.Message> sender) {
        LOG.info("BITSWAP start buildAndSendMessages");
        // make sure we stay within the message size limit
        MessageOuterClass.Message.Builder builder = MessageOuterClass.Message.newBuilder();
        int messageSize = 0;
        for (int i = 0; i < wants.size(); i++) {
            MessageOuterClass.Message.Wantlist.Entry want = wants.get(i);
            int wantSize = want.getSerializedSize();
            LOG.info("BITSWAP wantSize " + i + " " + wantSize);
            if (wantSize + messageSize > maxMessageSize) {
                LOG.info("BITSWAP wantSize messageSize > maxMessageSize. send message " + messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += wantSize;
            builder = builder.setWantlist(builder.getWantlist().toBuilder().addEntries(want).build());
        }

        for (int i = 0; i < presences.size(); i++) {
            MessageOuterClass.Message.BlockPresence presence = presences.get(i);
            int presenceSize = presence.getSerializedSize();
            if (presenceSize + messageSize > maxMessageSize) {
                LOG.info("BITSWAP presenceSize messageSize > maxMessageSize. send message" + messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += presenceSize;
            builder = builder.addBlockPresences(presence);
        }
        for (int i = 0; i < blocks.size(); i++) {
            MessageOuterClass.Message.Block block = blocks.get(i);
            int blockSize = block.getSerializedSize();
            if (blockSize + messageSize > maxMessageSize) {
                LOG.info("BITSWAP blockSize messageSize > maxMessageSize. send message" + messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += blockSize;
            builder = builder.addPayload(block);
        }
        if (messageSize > 0) {
            LOG.info("BITSWAP messageSize > 0. send message" + messageSize);
            if (builder.getWantlist().getEntriesCount() == 0) {
                MessageOuterClass.Message.Wantlist wantlist = MessageOuterClass.Message.Wantlist.newBuilder()
                        .addAllEntries(emptyList())
                        .setFull(true)
                        .build();
                builder.setWantlist(wantlist);
            }
            sender.accept(builder.build());
        }
    }

    public interface BitswapEngineHandler {
        void execute(MessageOuterClass.Message msg, Multiaddr peerId);
    }
}

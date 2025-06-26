package org.peergos.protocol.bitswap;

import com.google.protobuf.*;
import com.google.protobuf.util.JsonFormat;
import io.ipfs.cid.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import io.prometheus.client.*;
import org.peergos.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.bitswap.pb.*;
import org.peergos.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import java.util.stream.*;

public class BitswapEngine {
    private static final Logger LOG = Logging.LOG();

    private final Blockstore store;
    private final int maxMessageSize;
    private final ConcurrentHashMap<Want, WantResult> localWants = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, Boolean> persistBlocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Want, PeerId> blockHaves = new ConcurrentHashMap<>();
    private final Map<Want, Boolean> deniedWants = Collections.synchronizedMap(new LRUCache<>(10_000));
    private final Map<PeerId, Map<Want, Boolean>> recentBlocksSent = Collections.synchronizedMap(new LRUCache<>(100));
    private final Map<PeerId, Map<Want, Long>> recentWantsSent = Collections.synchronizedMap(new org.peergos.util.LRUCache<>(100));
    private final Map<PeerId, Boolean> blockedPeers = Collections.synchronizedMap(new LRUCache<>(1_000));
    private final boolean blockAggressivePeers;
    private final Set<PeerId> connections = new HashSet<>();
    private final BlockRequestAuthoriser authoriser;
    private AddressBook addressBook;
    public BitswapEngineHandler beHandler;

    public interface BitswapEngineHandler {
         void execute(MessageOuterClass.Message msg, Multiaddr peerId);
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
        return ! blockAggressivePeers || ! blockedPeers.containsKey(peer);
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public synchronized void addConnection(PeerId peer, Multiaddr addr) {
        LOG.info("BitswapEngine.addConnection - Peer - " + peer.toBase58() + " Multiaddr - " + addr);
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
        return ! localWants.isEmpty();
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
                    .filter(e -> e.getValue().creationTime > now - 5*60*1000)
                    .map(e -> e.getKey())
                    .filter(w -> ! recent.containsKey(w) || recent.get(w) < now - minResendWait)
                    .collect(Collectors.toSet());
            res.forEach(w -> recent.put(w, now));
            return res;
        }
        return localWants.keySet();
    }

    public Map<Want, PeerId> getHaves() {
        return blockHaves;
    }

    private static byte[] prefixBytes(Cid c) {
        ByteArrayOutputStream res = new ByteArrayOutputStream();
        try {
            Cid.putUvarint(res, c.version);
            Cid.putUvarint(res, c.codec.type);
            Cid.putUvarint(res, c.getType().index);
            Cid.putUvarint(res, c.getType().length);;
            return res.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void receiveMessage(MessageOuterClass.Message msg, Stream source, Counter sentBytes) {

        LOG.info("RECEIVE MESSAGE FROM - " + source.remotePeerId().toBase58());

        try {
            String json = JsonFormat.printer().print(msg);
            LOG.info("MESSAGE JSON  " + json);
        } catch (InvalidProtocolBufferException e) {
            LOG.info("MESSAGE reply  " + msg);
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
            for (MessageOuterClass.Message.Wantlist.Entry e : msg.getWantlist().getEntriesList()) {
                Cid c;
                try {
                    c = Cid.cast(e.getBlock().toByteArray());
                } catch (Exception ex) {
                    continue;
                }

                LOG.info("PEER - CID -- " + peerM.toBase58() + "====" + c.version);

                Optional<String> auth = e.getAuth().isEmpty() ? Optional.empty() : Optional.of(ArrayOps.bytesToHex(e.getAuth().toByteArray()));
                boolean isCancel = e.getCancel();
                boolean sendDontHave = e.getSendDontHave();
                boolean wantBlock = e.getWantType().getNumber() == 0;
                Want w = new Want(c, auth);
                LOG.info("WANT - " + w.cid);

                LOG.info("MessageOuterClass.Message.Wantlist.Entry - getWantType = " +  e.getWantType() + " - " + e.getWantType().getNumber());
                LOG.info("MessageOuterClass.Message.Wantlist.Entry - sendDontHave = " +  e.getSendDontHave());
                LOG.info("MessageOuterClass.Message.Wantlist.Entry - isCancel = " +  e.getCancel());

                if (wantBlock) {
                    boolean denied = deniedWants.containsKey(w);

                    LOG.info("WANT denied - " + denied);
                    if (denied) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                        continue;
                    }
                    LOG.info("IF we recently sent it to this peer  - " + recent.containsKey(w));

                    if (recent.containsKey(w))
                        continue; // don't re-send this block as we recently sent it to this peer
                    boolean blockPresent = store.has(c).join();
                    LOG.info("blockPresent HAS  - " + blockPresent);

                    if (! blockPresent)
                        absentBlocks++;
                    else
                        presentBlocks++;

                    if (blockPresent && authoriser.allowRead(c, sourcePeerId, auth.orElse("")).join()) {
                        LOG.info("authoriser allowRead ad blockPresent is true  - ");
                        MessageOuterClass.Message.Block blockP = MessageOuterClass.Message.Block.newBuilder()
                                .setPrefix(ByteString.copyFrom(prefixBytes(c)))
                                .setAuth(ByteString.copyFrom(ArrayOps.hexToBytes(auth.orElse(""))))
                                .setData(ByteString.copyFrom(store.get(c).join().get()))
                                .build();
                        int blockSize = blockP.getSerializedSize();
                        if (blockSize + messageSize > maxMessageSize) {
                            buildAndSendMessages(Collections.emptyList(), presences, blocks, source::writeAndFlush);
                            presences = new ArrayList<>();
                            blocks = new ArrayList<>();
                            messageSize = 0;
                        }
                        messageSize += blockSize;
                        LOG.info("added block  - " + blockP);
                        blocks.add(blockP);
                        recent.put(w, true);
                    } else if (sendDontHave) {
                        if (blockPresent) {
                            deniedWants.put(w, true);
                            LOG.info("Rejecting auth for block " + c + " from " + sourcePeerId.bareMultihash());
                        }
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else if (blockPresent) {
                        deniedWants.put(w, true);
                        LOG.info("Rejecting repeated invalid auth for block " + c + " from " + sourcePeerId.bareMultihash());
                    }
                } else {
                    LOG.info("HAVE has " + c.bareMultihash() + " == " + c);

                    boolean hasBlock = store.has(c).join();

                    LOG.info(" IF !wantBlock hasBlock HAS  - " + hasBlock);
                    if (hasBlock) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.Have)
                                .build();
                        LOG.info(" IF !wantBlock hasBlock HAS presence TYPE - " + presence.getTypeValue() + " - CID HEX - " + c.toHex());
                        LOG.info(" IF !wantBlock hasBlock HAS presence CID Base58 - " + c.toBase58());

                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    } else if (sendDontHave) {
                        MessageOuterClass.Message.BlockPresence presence = MessageOuterClass.Message.BlockPresence.newBuilder()
                                .setCid(ByteString.copyFrom(c.toBytes()))
                                .setType(MessageOuterClass.Message.BlockPresenceType.DontHave)
                                .build();
                        presences.add(presence);
                        messageSize += presence.getSerializedSize();
                    }
                }
            }
        }

        LOG.fine("Bitswap received " + msg.getWantlist().getEntriesCount() + " wants, " + msg.getPayloadCount() +
                " blocks and " + msg.getBlockPresencesCount() + " presences from " + sourcePeerId.toBase58());
        boolean receivedWantedBlock = false;

        LOG.info("Start CYCLE MessageOuterClass.Message.Block");
        for (MessageOuterClass.Message.Block block : msg.getPayloadList()) {
            byte[] cidPrefix = block.getPrefix().toByteArray();

            LOG.info("MessageOuterClass.Message.Block Prefix = " + cidPrefix);
            Optional<String> auth = block.getAuth().isEmpty() ?
                    Optional.empty() :
                    Optional.of(ArrayOps.bytesToHex(block.getAuth().toByteArray()));
            byte[] data = block.getData().toByteArray();
            LOG.info("MessageOuterClass.Message.Block auth IS Empty? = " + block.getAuth().isEmpty());

            LOG.info("MessageOuterClass.Message.Block Block data = " + data.toString());

            ByteArrayInputStream bin = new ByteArrayInputStream(cidPrefix);
            try {
                long version = Cid.readVarint(bin);
                LOG.info("MessageOuterClass.Message.Block Version = " + version);
                Cid.Codec codec = Cid.Codec.lookup(Cid.readVarint(bin));
                Multihash.Type type = Multihash.Type.lookup((int)Cid.readVarint(bin));
//                int hashSize = (int)Cid.readVarint(bin);
                LOG.info("MessageOuterClass.Message.Block type data = " + type);
                if (type != Multihash.Type.sha2_256) {
                    LOG.info("Unsupported hash algorithm " + type.name());
                } else {
                    byte[] hash = Hash.sha256(data);
                    Cid c = new Cid(version, codec, type, hash);
                    Want w = new Want(c, auth);
                    LOG.info("MessageOuterClass.Message.Block WantResult waiter = " + type);
                    WantResult waiter = localWants.get(w);
                    LOG.info("MessageOuterClass.Message.Block WantResult waiter = " + waiter);
                    if (waiter != null) {
                        receivedWantedBlock = true;
                        if (persistBlocks.containsKey(w)) {
                            store.put(data, codec);
                            persistBlocks.remove(w);
                        }
                        LOG.info("MessageOuterClass.Message.Block waiter result complete = " + data);
                        waiter.result.complete(new HashedBlock(c, data));
                        localWants.remove(w);
                    } else
                        LOG.info("Received block we don't want: " + c + " from " + sourcePeerId.bareMultihash());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        LOG.info("MessageOuterClass.Message.Block localWants = " + localWants);

        if (! localWants.isEmpty())
            LOG.fine("Remaining: " + localWants.size());
        boolean receivedRequestedHave = false;

        LOG.info("MessageOuterClass.Message.BlockPresence SIZE= " +  msg.getBlockPresencesList().size());

        for (MessageOuterClass.Message.BlockPresence blockPresence : msg.getBlockPresencesList()) {
            Cid c = Cid.cast(blockPresence.getCid().toByteArray());
            LOG.info("MessageOuterClass.Message.BlockPresence CID= " +  c.toBase58());

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

        LOG.info("absentBlocks-- " +  absentBlocks + " - presentBlocks -- " + presentBlocks+ " - receivedRequestedHave --"
                + receivedRequestedHave + " - receivedWantedBlock-- " + receivedWantedBlock);

        if (absentBlocks > 10 && presentBlocks == 0 && ! receivedRequestedHave && ! receivedWantedBlock) {
            // This peer is sending us lots of irrelevant requests, block them
            LOG.info("This peer is sending us lots of irrelevant requests, block them");
            blockedPeers.put(source.remotePeerId(), true);
            source.close();
        }

        if (presences.isEmpty() && blocks.isEmpty()) {
            LOG.info("presences and blocks IS EMPTY");
            return;
        }


        LOG.info("START BUILD AND SEND MESSAGES");
        buildAndSendMessages(Collections.emptyList(), presences, blocks, reply -> {
            sentBytes.inc(reply.getSerializedSize());
            LOG.info("Sent message to " + source.remotePeerId().toBase58() );
            LOG.info("message WantList ==  " + reply.getWantlist() );
            LOG.info("message Presenses List ==  " + reply.getBlockPresencesList() );


            reply.getBlockPresencesList().forEach(blockPresence -> {
                LOG.info("PRESENSE IN Reply message ==  TypeValue -  " + blockPresence.getTypeValue() );
                LOG.info("PRESENSE IN Reply message ==  CID -  " + blockPresence.getCid() );
                LOG.info("PRESENSE IN Reply message ==  AUTH -  " + blockPresence.getAuth() );
            });

            reply.getPayloadList().forEach(block -> {
                LOG.info("BLOCK IN Reply message ==  Prefix -  " + block.getPrefix() );
                LOG.info("BLOCK IN Reply message ==  DATA -  " + block.getData() );
                LOG.info("BLOCK IN Reply message ==  AUTH -  " + block.getAuth() );
            });

            try {
                String json = JsonFormat.printer().print(reply);
                LOG.info("MESSAGE JSON  " + json);
            } catch (InvalidProtocolBufferException e) {
                LOG.info("MESSAGE reply  " + reply);
                throw new RuntimeException(e);
            }

            LOG.info("SOURCE PROTOCOL " + source.getProtocol().join() );

            Multiaddr addr =  source.getConnection().remoteAddress().withP2P(source.remotePeerId());
            source.writeAndFlush(reply);
            source.close().join();
            CompletableFuture.runAsync(() -> {
                this.beHandler.execute(reply, addr);
            });
        });
    }

    public void buildAndSendMessages(List<MessageOuterClass.Message.Wantlist.Entry> wants,
                                     List<MessageOuterClass.Message.BlockPresence> presences,
                                     List<MessageOuterClass.Message.Block> blocks,
                                     Consumer<MessageOuterClass.Message> sender) {
        LOG.info("START buildAndSendMessages INSIDE");
        // make sure we stay within the message size limit
        MessageOuterClass.Message.Builder builder = MessageOuterClass.Message.newBuilder();
        int messageSize = 0;
        LOG.info("START  wants.size() CYCLE");
        for (int i=0; i < wants.size(); i++) {
            MessageOuterClass.Message.Wantlist.Entry want = wants.get(i);
            int wantSize = want.getSerializedSize();
            LOG.info("wantSize - " + i  + " ==== " +wantSize );
            if (wantSize + messageSize > maxMessageSize) {
                LOG.info("wants.size() sender  accept  messageSize > maxMessageSize " +  messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += wantSize;
            builder = builder.setWantlist(builder.getWantlist().toBuilder().addEntries(want).build());
        }
        LOG.info("START  presences.size() CYCLE");
        for (int i=0; i < presences.size(); i++) {
            MessageOuterClass.Message.BlockPresence presence = presences.get(i);
            int presenceSize = presence.getSerializedSize();
            LOG.info("presenceSize - " + i  + " ==== " +presenceSize );
            if (presenceSize + messageSize > maxMessageSize) {
                LOG.info(" presences.size() sender  accept  messageSize > maxMessageSize " +  messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += presenceSize;
            builder = builder.addBlockPresences(presence);
        }
        LOG.info("START  blocks.size(); CYCLE");
        for (int i=0; i < blocks.size(); i++) {
            MessageOuterClass.Message.Block block = blocks.get(i);
            int blockSize = block.getSerializedSize();
            LOG.info("blockSize - " + i  + " ==== " +blockSize );
            if (blockSize + messageSize > maxMessageSize) {
                LOG.info(" blocks.size() sender  accept  messageSize > maxMessageSize " +  messageSize);
                sender.accept(builder.build());
                builder = MessageOuterClass.Message.newBuilder();
                messageSize = 0;
            }
            messageSize += blockSize;
            builder = builder.addPayload(block);
        }

        LOG.info("START  if (messageSize > 0) ");
        if (messageSize > 0) {
            MessageOuterClass.Message a = builder.build();
            for (int i=0; i < a.getBlocksList().size(); i++) {
                LOG.info("START BUILDER BUILD  messageSize > 0 " + a.getBlocksList().get(i) + " ---  "  +  a.getBlocksList().get(i).toStringUtf8());
            }
            sender.accept(a);
        }

        LOG.info("END buildAndSendMessages INSIDE");

    }
}

package org.peergos;

import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.ConnectionHandler;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.StreamPromise;
import io.libp2p.core.crypto.KeyKt;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multiformats.Protocol;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.crypto.keys.Ed25519Kt;
import io.libp2p.protocol.IdentifyBinding;
import io.libp2p.protocol.IdentifyController;
import io.libp2p.protocol.IdentifyProtocol;
import io.libp2p.protocol.Ping;
import io.libp2p.security.noise.NoiseXXSecureChannel;
import io.libp2p.transport.quic.QuicTransport;
import io.libp2p.transport.tcp.TcpTransport;
import org.peergos.blockstore.Blockstore;
import org.peergos.protocol.autonat.AutonatProtocol;
import org.peergos.protocol.bitswap.Bitswap;
import org.peergos.protocol.bitswap.BitswapEngine;
import org.peergos.protocol.circuit.CircuitHopProtocol;
import org.peergos.protocol.circuit.CircuitStopProtocol;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.KademliaEngine;
import org.peergos.protocol.dht.ProviderStore;
import org.peergos.protocol.dht.RecordStore;
import org.peergos.util.Logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class HostBuilder {
    private PrivKey privKey;
    private PeerId peerId;
    private final List<String> listenAddrs = new ArrayList<>();
    private final List<ProtocolBinding> protocols = new ArrayList<>();
    private final List<StreamMuxerProtocol> muxers = new ArrayList<>();

    private static final Logger LOG = Logging.LOG();

    public HostBuilder() {
    }

    public PrivKey getPrivateKey() {
        return privKey;
    }

    public PeerId getPeerId() {
        return peerId;
    }

    public List<ProtocolBinding> getProtocols() {
        return this.protocols;
    }

    public Optional<Kademlia> getWanDht() {
        return protocols.stream()
                .filter(p -> p instanceof Kademlia && p.getProtocolDescriptor().getAnnounceProtocols().contains("/ipfs/kad/1.0.0"))
                .map(p -> (Kademlia)p)
                .findFirst();
    }

    public Optional<Bitswap> getBitswap() {
        return protocols.stream()
                .filter(p -> p instanceof Bitswap)
                .map(p -> (Bitswap)p)
                .findFirst();
    }

    public Optional<CircuitHopProtocol.Binding> getRelayHop() {
        return protocols.stream()
                .filter(p -> p instanceof CircuitHopProtocol.Binding)
                .map(p -> (CircuitHopProtocol.Binding)p)
                .findFirst();
    }

    public HostBuilder addMuxers(List<StreamMuxerProtocol> muxers) {
        this.muxers.addAll(muxers);
        return this;
    }

    public HostBuilder addProtocols(List<ProtocolBinding> protocols) {
        this.protocols.addAll(protocols);
        return this;
    }

    public HostBuilder addProtocol(ProtocolBinding protocols) {
        this.protocols.add(protocols);
        return this;
    }

    public HostBuilder listen(List<MultiAddress> listenAddrs) {
        this.listenAddrs.addAll(listenAddrs.stream().map(MultiAddress::toString).collect(Collectors.toList()));
        return this;
    }

    public HostBuilder generateIdentity() {
        return setPrivKey(Ed25519Kt.generateEd25519KeyPair().getFirst());
    }

    public HostBuilder setIdentity(byte[] privKey) {
        return setPrivKey(KeyKt.unmarshalPrivateKey(privKey));
    }



    public HostBuilder setPrivKey(PrivKey privKey) {
        this.privKey = privKey;
        this.peerId = PeerId.fromPubKey(privKey.publicKey());
        return this;
    }

    public static HostBuilder create(int listenPort,
                                     ProviderStore providers,
                                     RecordStore records,
                                     Blockstore blocks,
                                     BlockRequestAuthoriser authoriser) {
        return create(listenPort, providers, records, blocks, authoriser, false);
    }

    public static HostBuilder create(int listenPort,
                                     ProviderStore providers,
                                     RecordStore records,
                                     Blockstore blocks,
                                     BlockRequestAuthoriser authoriser,
                                     boolean blockAggressivePeers) {
        HostBuilder builder = new HostBuilder()
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)));
        Multihash ourPeerId = Multihash.deserialize(builder.peerId.getBytes());
        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records, blocks), false);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.privKey, ourPeerId, 5);
        return builder.addProtocols(List.of(
                new Ping(),
                new AutonatProtocol.Binding(),
                new CircuitHopProtocol.Binding(relayManager, stop),
                new Bitswap(new BitswapEngine(blocks, authoriser, Bitswap.MAX_MESSAGE_SIZE, blockAggressivePeers)),
                dht));
    }

    public static Host build(int listenPort,
                             List<ProtocolBinding> protocols) {
        return new HostBuilder()
                .generateIdentity()
                .listen(List.of(new MultiAddress("/ip4/0.0.0.0/tcp/" + listenPort)))
                .addProtocols(protocols)
                .build();
    }

    public Host build() {
        if (muxers.isEmpty())
            muxers.addAll(List.of(StreamMuxerProtocol.Companion.getYamux(), StreamMuxerProtocol.Companion.getMplex()));
        return build(privKey, listenAddrs, protocols, muxers);
    }

    public static Host build(PrivKey privKey,
                             List<String> listenAddrs,
                             List<ProtocolBinding> protocols,
                             List<StreamMuxerProtocol> muxers) {
        Host host = BuilderJKt.hostJ(Builder.Defaults.None, b -> {
            b.getIdentity().setFactory(() -> privKey);
            List<Multiaddr> toListen = listenAddrs.stream().map(Multiaddr::new).collect(Collectors.toList());
            if (toListen.stream().anyMatch(a -> a.has(Protocol.QUICV1)))
                b.getSecureTransports().add(QuicTransport::Ecdsa);
            b.getTransports().add(TcpTransport::new);
            b.getSecureChannels().add(NoiseXXSecureChannel::new);
//            b.getSecureChannels().add(TlsSecureChannel::new);

            b.getMuxers().addAll(muxers);
            RamAddressBook addrs = new RamAddressBook();
            b.getAddressBook().setImpl(addrs);
            // Uncomment to add mux debug logging
//            b.getDebug().getMuxFramesHandler().addLogger(LogLevel.INFO, "MUX");
            for (ProtocolBinding<?> protocol : protocols) {
                b.getProtocols().add(protocol);
                if (protocol instanceof AddressBookConsumer)
                    ((AddressBookConsumer) protocol).setAddressBook(addrs);
                if (protocol instanceof ConnectionHandler)
                    b.getConnectionHandlers().add((ConnectionHandler) protocol);
            }

            Optional<Kademlia> wan = protocols.stream()
                    .filter(p -> p instanceof Kademlia && p.getProtocolDescriptor().getAnnounceProtocols().contains("/ipfs/kad/1.0.0"))
                    .map(p -> (Kademlia) p)
                    .findFirst();
            // Send an identify req on all new incoming connections
            b.getConnectionHandlers().add(connection -> {
                PeerId remotePeer = connection.secureSession().getRemoteId();
                LOG.info("HostBuilder.build: new incoming connection " + remotePeer.toBase58());

                Multiaddr remote = connection.remoteAddress().withP2P(remotePeer);
                addrs.addAddrs(remotePeer, 0, remote);
                if (connection.isInitiator())
                    return;
                addrs.getAddrs(remotePeer).thenAccept(existing -> {
                    if (! existing.isEmpty())
                        return;
                    StreamPromise<IdentifyController> stream = connection.muxerSession()
                            .createStream(new IdentifyBinding(new IdentifyProtocol()));
                    stream.getController()
                            .thenCompose(IdentifyController::id)
                            .thenAccept(remoteId -> {
                                Multiaddr[] remoteAddrs = remoteId.getListenAddrsList()
                                        .stream()
                                        .map(bytes -> Multiaddr.deserialize(bytes.toByteArray()))
                                        .toArray(Multiaddr[]::new);

                                addrs.addAddrs(remotePeer, 0, remoteAddrs);
                                List<String> protocolIds = remoteId.getProtocolsList().stream().collect(Collectors.toList());
                                if (protocolIds.contains(Kademlia.WAN_DHT_ID) && wan.isPresent()) {
                                    // add to kademlia routing table iffi
                                    // 1) we haven't already dialled them
                                    // 2) they accept a new kademlia stream
                                    if (existing.isEmpty())
                                        connection.muxerSession().createStream(wan.get());
                                }
                            });
                });
            });

            for (String listenAddr : listenAddrs) {
                b.getNetwork().listen(listenAddr);
            }

            b.getConnectionHandlers().add(conn -> System.out.println(conn.localAddress() +
                    " received connection from " + conn.remoteAddress() + " on transport " + conn.transport()));
        });
        for (ProtocolBinding protocol : protocols) {
            if (protocol instanceof HostConsumer)
                ((HostConsumer)protocol).setHost(host);
        }
        return host;
    }
}

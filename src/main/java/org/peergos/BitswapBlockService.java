package org.peergos;

import io.ipfs.cid.*;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.dht.*;
import org.peergos.util.Logging;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.*;

public class BitswapBlockService implements BlockService {

    private final Host us;
    private final Bitswap bitswap;
    private final Kademlia dht;
    private static final Logger LOG = Logging.LOG();

    public BitswapBlockService(Host us, Bitswap bitswap, Kademlia dht) {
        this.us = us;
        this.bitswap = bitswap;
        this.dht = dht;
    }

    @Override
    public List<HashedBlock> get(List<Want> hashes, Set<PeerId> peers, boolean addToBlockstore) {
        if (peers.isEmpty()) {
            // if peers are not provided try connected peers and then fallback to finding peers from DHT
            Set<PeerId> connected = bitswap.getBroadcastAudience();
            LOG.info("BitswapBlockService.get - Connected Peers Count - " + connected.size());

            if(!connected.isEmpty()) {
                connected.forEach(peer -> {
                    LOG.info("BitswapBlockService.get - Connected Peer - " + peer.toBase58());
                });
            }

            Set<HashedBlock> fromConnected = bitswap.get(hashes, us, connected, addToBlockstore)
                    .stream()
                    .flatMap(f -> {
                        try {
                            return java.util.stream.Stream.of(f.orTimeout(10, TimeUnit.SECONDS).join());
                        } catch (Exception e) {
                            return java.util.stream.Stream.empty();
                        }
                    })
                    .collect(Collectors.toSet());

            LOG.info("BitswapBlockService.get - fromConnected Size - " +  fromConnected.size() + " hashes size -- " + hashes.size() );

            if (fromConnected.size() == hashes.size())
                return new ArrayList<>(fromConnected);

            Set<Cid> done = fromConnected.stream().map(b -> b.hash).collect(Collectors.toSet());
            List<Want> remaining = hashes.stream()
                    .filter(w -> !done.contains(w.cid))
                    .collect(Collectors.toList());
            List<PeerAddresses> providers = dht.findProviders(hashes.get(0).cid, us, 5).join();
            peers = providers.stream()
                    .map(p -> PeerId.fromBase58(p.peerId.toBase58()))
                    .collect(Collectors.toSet());
            return java.util.stream.Stream.concat(bitswap.get(remaining, us, peers, addToBlockstore)
                                    .stream()
                                    .map(f -> f.join()),
                            fromConnected.stream())
                    .collect(Collectors.toList());
        }
        return bitswap.get(hashes, us, peers, addToBlockstore)
                .stream()
                .map(f -> f.join())
                .collect(Collectors.toList());
    }
}

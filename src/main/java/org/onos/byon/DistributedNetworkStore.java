/*
 * Copyright 2015 Open Networking Laboratory
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

package org.onos.byon;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.net.HostId;
import org.onosproject.store.AbstractStore;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.ConsistentMap;
import org.onosproject.store.service.MapEvent;
import org.onosproject.store.service.MapEventListener;
import org.onosproject.store.service.Serializer;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onos.byon.NetworkEvent.Type.*;

/**
 * Network Store implementation backed by consistent map.
 */
@Component(immediate = true)
@Service
public class DistributedNetworkStore
        extends AbstractStore<NetworkEvent, NetworkStoreDelegate>
        implements NetworkStore {

    private static Logger log = LoggerFactory.getLogger(DistributedNetworkStore.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected StorageService storageService;

    private ConsistentMap<String, Set<HostId>> networks;
    private final InternalListener listener = new InternalListener();

    @Activate
    public void activate() {
        networks = storageService.<String, Set<HostId>>consistentMapBuilder()
                .withSerializer(Serializer.using(KryoNamespaces.API))
                .withName("byon-networks")
                .build();

        networks.addListener(listener);
        log.info("Started");
    }

    @Deactivate
    public void deactivate() {
        networks.removeListener(listener);
        log.info("Stopped");
    }

    @Override
    public void putNetwork(String network) {
        networks.putIfAbsent(network, Sets.<HostId>newHashSet());
    }

    @Override
    public void removeNetwork(String network) {
        networks.remove(network);
    }

    @Override
    public Set<String> getNetworks() {
        return ImmutableSet.copyOf(networks.keySet());
    }

    @Override
    public boolean addHost(String network, HostId hostId) {
        Versioned<Set<HostId>> existingHosts = checkNotNull(networks.get(network),
                                                            "Network %s does not exist", network);

        if (existingHosts.value().contains(hostId)) {
            return false;
        }

        networks.computeIfPresent(network,
                                  (k, v) -> {
                                      Set<HostId> result = Sets.newHashSet(v);
                                      result.add(hostId);
                                      return result;
                                  });
        return true;
    }

    @Override
    public void removeHost(String network, HostId hostId) {
        Versioned<Set<HostId>> hosts =
            networks.computeIf(network,
                               value -> value != null && value.contains(hostId),
                               (k, v) -> {
                                   Set<HostId> result = Sets.newHashSet(v);
                                   result.remove(hostId);
                                   return result;
                               });
        checkNotNull(hosts, "Network %s does not exist", network);
    }

    @Override
    public Set<HostId> getHosts(String network) {
        return checkNotNull(networks.get(network),
                            "Please create the network first").value();
    }

    /**
     * Listener for remote map events.
     */
    private class InternalListener implements MapEventListener<String, Set<HostId>> {
        @Override
        public void event(MapEvent<String, Set<HostId>> mapEvent) {
            final NetworkEvent.Type type;
            switch (mapEvent.type()) {
                case INSERT:
                    type = NETWORK_ADDED;
                    break;
                case UPDATE:
                    type = NETWORK_UPDATED;
                    break;
                case REMOVE:
                default:
                    type = NETWORK_REMOVED;
                    break;
            }
            notifyDelegate(new NetworkEvent(type, mapEvent.key()));
        }
    }
}

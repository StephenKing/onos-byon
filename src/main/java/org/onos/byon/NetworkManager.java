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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.event.EventDeliveryService;
import org.onosproject.event.ListenerRegistry;
import org.onosproject.net.HostId;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.Intent;
import org.onosproject.net.intent.IntentService;
import org.onosproject.net.intent.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * FIXME Skeletal ONOS application component.
 */
@Component(immediate = true)
@Service
public class NetworkManager implements NetworkService {

    public static final String HOST_FORMAT = "%s~%s";
    public static final String KEY_FORMAT = "%s,%s";
    private static Logger log = LoggerFactory.getLogger(NetworkManager.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected NetworkStore store;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected IntentService intentService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventDeliveryService eventDispatcher;

    protected ApplicationId appId;
    private final ListenerRegistry<NetworkEvent, NetworkListener>
            listenerRegistry = new ListenerRegistry<>();

    private final NetworkStoreDelegate delegate = new InternalStoreDelegate();

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.onos.byon");
        eventDispatcher.addSink(NetworkEvent.class, listenerRegistry);
        store.setDelegate(delegate);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        eventDispatcher.removeSink(NetworkEvent.class);
        store.unsetDelegate(delegate);
        log.info("Stopped");
    }

    @Override
    public void createNetwork(String network) {
        checkNotNull(network, "Network name cannot be null");
        checkState(!network.contains(","), "Network names cannot contain commas");
        store.putNetwork(network);
    }

    @Override
    public void deleteNetwork(String network) {
        checkNotNull(network, "Network name cannot be null");
        store.removeNetwork(network);
        removeIntents(network, Optional.empty());
    }

    @Override
    public Set<String> getNetworks() {
        return store.getNetworks();
    }

    @Override
    public void addHost(String network, HostId hostId) {
        checkNotNull(network, "Network name cannot be null");
        checkNotNull(hostId, "HostId cannot be null");
        if (store.addHost(network, hostId)) {
            addIntents(network, hostId, store.getHosts(network));
        }
    }

    @Override
    public void removeHost(String network, HostId hostId) {
        checkNotNull(network, "Network name cannot be null");
        checkNotNull(hostId, "HostId cannot be null");
        store.removeHost(network, hostId);
        removeIntents(network, Optional.of(hostId));
    }

    @Override
    public Set<HostId> getHosts(String network) {
        checkNotNull(network, "Network name cannot be null");
        return store.getHosts(network);
    }

    @Override
    public void addListener(NetworkListener listener) {
        listenerRegistry.addListener(listener);
    }

    @Override
    public void removeListener(NetworkListener listener) {
        listenerRegistry.removeListener(listener);
    }

    /**
     * Returns ordered intent key from network and two hosts.
     *
     * @param network network name
     * @param one host one
     * @param two host two
     * @return canonical intent string key
     */
    protected Key key(String network, HostId one, HostId two) {
        String hosts = one.toString().compareTo(two.toString()) < 0 ?
                format(HOST_FORMAT, one, two) : format(HOST_FORMAT, two, one);
        return Key.of(format(KEY_FORMAT, network, hosts), appId);
    }

    private void addIntents(String network, HostId src, Set<HostId> hostsInNet) {
        Set<Intent> createdIntents = Sets.newHashSet();
        hostsInNet.forEach(dst -> {
            if (!src.equals(dst)) {
                Key key = key(network, src, dst);
                Intent intent = HostToHostIntent.builder()
                        .appId(appId)
                        .key(key)
                        .one(src)
                        .two(dst)
                        .build();
                intentService.submit(intent);
                createdIntents.add(intent);
            }
        });
    }

    /**
     * Matches an intent to a network and optional host.
     *
     * @param network network name
     * @param id optional host id, wildcard if missing
     * @param intent intent to match
     * @return true if intent matches, false otherwise
     */
    protected boolean matches(String network, Optional<HostId> id, Intent intent) {
        if (!Objects.equals(appId, intent.appId())) {
            // different app ids
            return false;
        }

        String key = intent.key().toString();
        if (!key.startsWith(network)) {
            // different network
            return false;
        }

        if (!id.isPresent()) {
            // no host id specified; wildcard match
            return true;
        }

        HostId hostId = id.get();
        String[] fields = key.split(",");
        // return result of id match in host portion of key
        return fields.length > 1 && fields[1].contains(hostId.toString());
    }

    private void removeIntents(String network, Optional<HostId> hostId) {
        Iterables.filter(intentService.getIntents(), i -> matches(network, hostId, i))
                 .forEach(intentService::withdraw);
    }

    private class InternalStoreDelegate implements NetworkStoreDelegate {
        @Override
        public void notify(NetworkEvent event) {
            eventDispatcher.post(event);
        }
    }
}

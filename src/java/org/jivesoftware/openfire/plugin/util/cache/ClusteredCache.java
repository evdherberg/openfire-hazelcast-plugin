/*
 * Copyright (C) 1999-2009 Jive Software. All rights reserved.
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
package org.jivesoftware.openfire.plugin.util.cache;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.monitor.LocalMapStats;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.cluster.ClusteredCacheEntryListener;
import org.jivesoftware.openfire.cluster.NodeID;
import org.jivesoftware.openfire.container.Plugin;
import org.jivesoftware.openfire.container.PluginClassLoader;
import org.jivesoftware.util.cache.Cache;
import org.jivesoftware.util.cache.CacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Clustered implementation of the Cache interface using Hazelcast.
 *
 */
public class ClusteredCache<K extends Serializable, V extends Serializable> implements Cache<K, V> {

    private final Logger logger;

    private final Set<String> listeners = ConcurrentHashMap.newKeySet();

    // Maps for tracking information about locks
    // Not using ConcurrentHashMap on purpose - we do want to see errors occur if there are concurrent modifications - that may tell us something
    private static final int REGISTRATION_MAX_SIZE_PER_KEY = 20;
    private static final long HANGING_LOCK_DETECTION_THRESHOLD_SECONDS = 30L;
    private final Map<K, LinkedList<LockInfo>> lockRegistration = new HashMap<>();
    private final Map<K, LinkedList<LockInfo>> lockRegistrationArchive = new HashMap<>();

    /**
     * The map is used for distributed operations such as get, put, etc.
     */
    final IMap<K, V> map;
    private String name;
    private long numberOfGets = 0;

    /**
     * Used to limit the amount of duplicate warnings logged.
     */
    private Instant lastPluginClassLoaderWarning = Instant.EPOCH;
    private final Duration pluginClassLoaderWarningSupression = Duration.ofHours(1);

    /**
     * Create a new cache using the supplied named cache as the actual cache implementation
     *
     * @param name a name for the cache, which should be unique per vm.
     * @param cache the cache implementation
     */
    protected ClusteredCache(final String name, final IMap<K, V> cache) {
        this.map = cache;
        this.name = name;
        logger = LoggerFactory.getLogger(ClusteredCache.class.getName() + "[cache: "+name+"]");
    }

    void addEntryListener(final MapListener listener) {
        listeners.add(map.addEntryListener(listener, false));
    }

    @Override
    public String addClusteredCacheEntryListener(@Nonnull final ClusteredCacheEntryListener<K, V> clusteredCacheEntryListener, final boolean includeValues, final boolean includeEventsFromLocalNode)
    {
        final EntryListener<K, V> listener = new EntryListener<K, V>() {
            @Override
            public void mapEvicted(MapEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing map evicted event of node '{}'", eventNodeId);
                    clusteredCacheEntryListener.mapEvicted(eventNodeId);
                }
            }

            @Override
            public void mapCleared(MapEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing map cleared event of node '{}'", eventNodeId);
                    clusteredCacheEntryListener.mapCleared(eventNodeId);
                }
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing entry update event of node '{}' for key '{}'", eventNodeId, event.getKey());
                    clusteredCacheEntryListener.entryUpdated((K) event.getKey(), (V) event.getOldValue(), (V) event.getValue(), eventNodeId);
                }
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing entry removed event of node '{}' for key '{}'", eventNodeId, event.getKey());
                    clusteredCacheEntryListener.entryRemoved((K) event.getKey(), (V) event.getOldValue(), eventNodeId);
                }
            }

            @Override
            public void entryEvicted(EntryEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing entry evicted event of node '{}' for key '{}'", eventNodeId, event.getKey());
                    clusteredCacheEntryListener.entryEvicted((K) event.getKey(), (V) event.getOldValue(), eventNodeId);
                }
            }

            @Override
            public void entryAdded(EntryEvent event) {
                if (includeEventsFromLocalNode || !event.getMember().localMember()) {
                    final NodeID eventNodeId = ClusteredCacheFactory.getNodeID(event.getMember());
                    logger.trace("Processing entry added event of node '{}' for key '{}'", eventNodeId, event.getKey());
                    clusteredCacheEntryListener.entryAdded((K) event.getKey(), (V) event.getValue(), eventNodeId);
                }
            }
        };

        final String listenerId = map.addEntryListener(listener, includeValues);
        listeners.add(listenerId);
        logger.debug("Added new clustered cache entry listener (including values: {}, includeEventsFromLocalNode: {}) using ID: '{}'", includeValues, includeEventsFromLocalNode, listenerId);
        return listenerId;
    }

    @Override
    public void removeClusteredCacheEntryListener(@Nonnull final String listenerId) {
        logger.debug("Removing clustered cache entry listener: '{}'", listenerId);
        map.removeEntryListener(listenerId);
        listeners.remove(listenerId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public V put(final K key, final V object) {
        if (object == null) { return null; }
        checkForPluginClassLoader(key);
        checkForPluginClassLoader(object);
        return map.put(key, object);
    }

    @Override
    public V get(final Object key) {
        numberOfGets++;
        return map.get(key);
    }

    @Override
    public V remove(final Object key) {
        return map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int size() {
        final LocalMapStats stats = map.getLocalMapStats();
        return (int) (stats.getOwnedEntryCount() + stats.getBackupEntryCount());
    }

    @Override
    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> entries) {
        map.putAll(entries);

        // Instances are likely all loaded by the same class loader. For resource usage optimization, let's test just one, not all.
        entries.entrySet().stream().findAny().ifPresent(
            e -> {
                checkForPluginClassLoader(e.getKey());
                checkForPluginClassLoader(e.getValue());
            }
        );
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public long getCacheHits() {
        return map.getLocalMapStats().getHits();
    }

    @Override
    public long getCacheMisses() {
        final long hits = map.getLocalMapStats().getHits();
        return numberOfGets > hits ? numberOfGets - hits : 0;
    }

    @Override
    public int getCacheSize() {
        return (int) getLongCacheSize();
    }

    @Override
    public long getLongCacheSize() {
        final LocalMapStats stats = map.getLocalMapStats();
        return stats.getOwnedEntryMemoryCost() + stats.getBackupEntryMemoryCost();
    }

    @Override
    public long getMaxCacheSize() {
        return CacheFactory.getMaxCacheSize(getName());
    }

    @Override
    public void setMaxCacheSize(int i) {
        setMaxCacheSize((long) i);
    }

    @Override
    public void setMaxCacheSize(final long maxSize) {
        CacheFactory.setMaxSizeProperty(getName(), maxSize);
    }

    @Override
    public long getMaxLifetime() {
        return CacheFactory.getMaxCacheLifetime(getName());
    }

    @Override
    public void setMaxLifetime(final long maxLifetime) {
        CacheFactory.setMaxLifetimeProperty(getName(), maxLifetime);
    }

    void destroy() {
        listeners.forEach(map::removeEntryListener);
        map.destroy();
    }

    boolean lock(final K key, final long timeout) {
        boolean result = true;
        if (timeout < 0) {
            registerLockRequested(key);
            map.lock(key);
            registerLockAcquired(key, true);
        } else if (timeout == 0) {
            registerLockRequested(key);
            result = map.tryLock(key);
            registerLockAcquired(key, result);
        } else {
            try {
                registerLockRequested(key);
                result = map.tryLock(key, timeout, TimeUnit.MILLISECONDS);
                registerLockAcquired(key, result);
            } catch (final InterruptedException e) {
                logger.error("Failed to get cluster lock", e);
                result = false;
                registerLockAcquired(key, false);
            }
        }
        return result;
    }

    void unlock(final K key) {
        try {
            map.unlock(key);
            registerLockReleased(key);
        } catch (final IllegalMonitorStateException e) {
            logger.error("Failed to release cluster lock", e);
        }
    }

    /**
     * Clustered caches should not contain instances of classes that are provided by Openfire plugins. These will cause
     * issues related to class loading when the providing plugin is reloaded. This method verifies if an instance is
     * loaded by a plugin class loader, and logs a warning to the log files when it is. The amount of warnings logged is
     * limited by a time interval.
     *
     * @param o the instance for which to verify the class loader
     * @see <a href="https://github.com/igniterealtime/openfire-hazelcast-plugin/issues/74">Issue #74: Warn against usage of plugin-provided classes in Hazelcast</a>
     */
    protected void checkForPluginClassLoader(final Object o) {
        if (o != null && o.getClass().getClassLoader() instanceof PluginClassLoader
            && lastPluginClassLoaderWarning.isBefore(Instant.now().minus(pluginClassLoaderWarningSupression)) )
        {
            // Try to determine what plugin loaded the offending class.
            String pluginName = null;
            try {
                final Collection<Plugin> plugins = XMPPServer.getInstance().getPluginManager().getPlugins();
                for (final Plugin plugin : plugins) {
                    final PluginClassLoader pluginClassloader = XMPPServer.getInstance().getPluginManager().getPluginClassloader(plugin);
                    if (o.getClass().getClassLoader().equals(pluginClassloader)) {
                        pluginName = XMPPServer.getInstance().getPluginManager().getCanonicalName(plugin);
                        break;
                    }
                }
            } catch (Exception e) {
                logger.debug("An exception occurred while trying to determine the plugin class loader that loaded an instance of {}", o.getClass(), e);
            }
            logger.warn("An instance of {} that is loaded by {} has been added to the cache. " +
                    "This will cause issues when reloading the plugin that provides this class. The plugin implementation should be modified.",
                o.getClass(), pluginName != null ? pluginName : "a PluginClassLoader");
            lastPluginClassLoaderWarning = Instant.now();
        }
    }

    /**
     * Adds a LockInfo object to the lock registration for the specified key.
     * @param key The key for which the lock is requested.
     */
    private void registerLockRequested(K key) {
        logger.debug("LOCK TRACKING - Thread {} requested a lock on key {}.", Thread.currentThread().getName(), key);
        LockInfo lockInfo = new LockInfo(key);
        final LinkedList<LockInfo> lockInfosForThisKey = this.lockRegistration.computeIfAbsent(key, k -> new LinkedList<>());
        if (lockInfosForThisKey.size() >= ClusteredCache.REGISTRATION_MAX_SIZE_PER_KEY) {
            logger.warn("LOCK TRACKING - Not registering lock request by thread {} on key {} because the registration has reached capacity.", Thread.currentThread().getName(), key);
        } else {
            lockInfosForThisKey.addFirst(lockInfo);
        }
    }

    /**
     * Updates an existing LockInfo object in the registration to reflect that that lock as actually been applied.
     * @param key The key for which the lock is acquired.
     * @param acquireWasSuccessful Whether the lock could actually be acquired.
     */
    private void registerLockAcquired(K key, boolean acquireWasSuccessful) {
        if (!acquireWasSuccessful) {
            logger.warn("LOCK TRACKING - Thread {} tried to acquire a lock on key {}, but failed to get it.", Thread.currentThread().getName(), key);
            return;
        }

        logger.debug("LOCK TRACKING - Thread {} acquired a lock on key {}", Thread.currentThread().getName(), key);
        LinkedList<LockInfo> lockInfoListForThisKey = this.lockRegistration.get(key);
        if (lockInfoListForThisKey == null || lockInfoListForThisKey.isEmpty()) {
            logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but there is no lock registered for that key.", Thread.currentThread().getName(), key);
        } else {
            // Find the lock info object, hopefully at the beginning of the list
            final List<LockInfo> moreRecentlyRegisteredLocks = new ArrayList<>();
            LockInfo matchingLockInfo = null;
            for (LockInfo lockInfo : lockInfoListForThisKey) {
                if (lockInfo.isWaiting() && lockInfo.getThreadName().equals(Thread.currentThread().getName())) {
                    // Found it!
                    matchingLockInfo = lockInfo;
                    break;
                } else {
                    moreRecentlyRegisteredLocks.add(lockInfo);
                }
            }
            if (matchingLockInfo == null) {
                logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but there is no lock registered for that key.", Thread.currentThread().getName(), key);
            } else {
                if (!moreRecentlyRegisteredLocks.isEmpty()) {
                    logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but this is not the most recent thread to have requested a lock. This may still be a valid situation because threads sometimes need to queue in waiting for a lock. Other locks registered more recently: {}.", key, Thread.currentThread().getName(), moreRecentlyRegisteredLocks);
                }
                matchingLockInfo.registerAcquired();
            }
        }
    }

    /**
     * Updates an existing LockInfo object in the registration to reflect that that lock as been released.
     * This also moves the lock info from the lock registration to the archive.
     * @param key The key for which the lock is releases.
     */
    private void registerLockReleased(K key) {
        logger.debug("LOCK TRACKING - Thread {} released a lock on key {}.", Thread.currentThread().getName(), key);
        LinkedList<LockInfo> lockInfoListForThisKey = this.lockRegistration.get(key);
        if (lockInfoListForThisKey == null || lockInfoListForThisKey.isEmpty()) {
            logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but there is no lock registered for that key.", Thread.currentThread().getName(), key);
        } else {
            // Find the lock info object, hopefully at the beginning of the list
            final List<LockInfo> moreRecentlyRegisteredLocks = new ArrayList<>();
            LockInfo matchingLockInfo = null;
            for (LockInfo lockInfo : lockInfoListForThisKey) {
                if (lockInfo.isActive() && lockInfo.getThreadName().equals(Thread.currentThread().getName())) {
                    // Found it!
                    matchingLockInfo = lockInfo;
                    break;
                } else {
                    moreRecentlyRegisteredLocks.add(lockInfo);
                }
            }
            if (matchingLockInfo == null) {
                logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but there is no lock registered for that key.", Thread.currentThread().getName(), key);
            } else {
                if (!moreRecentlyRegisteredLocks.isEmpty()) {
                    logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired a lock on key {}, but this is not the most recent thread to have requested a lock. This may still be a valid situation because threads sometimes need to queue in waiting for a lock. Other locks registered more recently: {}.", key, Thread.currentThread().getName(), moreRecentlyRegisteredLocks);
                }
                final LockInfo currentLockHolder = findCurrentLockHolder(key).orElse(null);
                if (matchingLockInfo.equals(currentLockHolder)) {
                    matchingLockInfo.registerReleased();
                    moveToArchive(matchingLockInfo);
                } else {
                    logger.warn("LOCK TRACKING - Thread {} is supposed to have acquired lock {}, but currently lock {} is the active one.", Thread.currentThread().getName(), matchingLockInfo, currentLockHolder);
                }
            }
        }
    }

    /**
     * Removes the referenced LockInfo from the registration and adds it to the archive.
     * If the archive is at full capacity, the oldest entry is removed.
     * @param lockInfo The lock info to archive.
     */
    private void moveToArchive(@Nonnull LockInfo lockInfo) {
        final LinkedList<LockInfo> lockInfos = lockRegistration.get(lockInfo.getKey());
        if (lockInfos == null || !lockInfos.remove(lockInfo)) {
            logger.warn("LOCK TRACKING - Can't move {} to lock registration archive because it was not found in the lock registration.", lockInfo);
        } else {
            lockRegistrationArchive.computeIfAbsent(lockInfo.getKey(), k -> new LinkedList<>()).addFirst(lockInfo);
            if (lockRegistrationArchive.get(lockInfo.getKey()).size() > REGISTRATION_MAX_SIZE_PER_KEY) {
                lockRegistrationArchive.get(lockInfo.getKey()).removeLast();
            }
            logger.debug("LOCK TRACKING - Moved {} to lock registration archive. Current registration size is {} and archive size is {}.", lockInfo, lockInfos.size(), lockRegistrationArchive.get(lockInfo.getKey()).size());
        }
    }

    /**
     * Find the current holder of an active lock on the referenced key.
     * @param key The key for which to find the active lock holder.
     * @return The active lock holder, if any.
     */
    private Optional<LockInfo> findCurrentLockHolder(K key) {
        final List<LockInfo> activeLocksForKey = lockRegistration.computeIfAbsent(key, k -> new LinkedList<>()).stream().filter(LockInfo::isActive).collect(Collectors.toList());
        if (activeLocksForKey.isEmpty()) {
            logger.debug("LOCK TRACKING - There is no active lock holder for key {}", key);
            return Optional.empty();
        } else if (activeLocksForKey.size() == 1) {
            logger.debug("LOCK TRACKING - There is a single active lock holder for key {}: {}", key, activeLocksForKey.get(0));
            return Optional.of(activeLocksForKey.get(0));
        } else {
            logger.error("LOCK TRACKING - There is more than one active lock holder for key {}: {}", key, activeLocksForKey);
            return Optional.empty();
        }
    }

    /**
     * Returns information about all locks that have been active (acquired) for more than 30 seconds.
     * @return
     */
    public List<LockInfo> getPotentiallyHangingLocks() {
        LocalDateTime lockAcquiredThreshold = LocalDateTime.now().minusSeconds(HANGING_LOCK_DETECTION_THRESHOLD_SECONDS);
        return lockRegistration.values().stream()
            .flatMap(LinkedList::stream)
            .filter(LockInfo::isActive)
            .filter(lockInfo -> lockInfo.getLockAcquiredTime().equals(lockAcquiredThreshold))
            .collect(Collectors.toList());
    }

    /**
     * Returns information about all locks that appear to be waiting, while the referenced cache entry is currently not locked at all.
     * @return
     */
    public List<LinkedList<LockInfo>> getPotentiallyInconsistentLockEntries() {
        return lockRegistration.values().stream()
            .filter(lockInfos -> lockInfos.stream().noneMatch(LockInfo::isActive))
            .collect(Collectors.toList());
    }

    class LockInfo implements Serializable {
        private K key;
        private String threadName;
        private LocalDateTime lockRequestedTime;
        private LocalDateTime lockAcquiredTime;
        private LocalDateTime lockReleasedTime;
        private List<String> lockRequestedTrace;
        private List<String> lockReleasedTrace;

        public LockInfo(K key) {
            this.key = key;
            this.threadName = Thread.currentThread().getName();
            registerRequested();
        }

        private void registerRequested() {
            this.lockRequestedTime = LocalDateTime.now();
            this.lockRequestedTrace = determineStackTrace();
        }

        public void registerAcquired() {
            this.lockAcquiredTime = LocalDateTime.now();
        }

        public void registerReleased() {
            this.lockReleasedTime = LocalDateTime.now();
            this.lockReleasedTrace = determineStackTrace();
        }

        public boolean isWaiting() {
            return this.lockRequestedTime != null && this.lockAcquiredTime == null;
        }

        public boolean isActive() {
            return this.lockAcquiredTime != null && this.lockReleasedTime == null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockInfo lockInfo = (LockInfo) o;
            return key.equals(lockInfo.key) && threadName.equals(lockInfo.threadName) && lockRequestedTime.equals(lockInfo.lockRequestedTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, threadName, lockRequestedTime);
        }

        @Override
        public String toString() {
            return "LockInfo{" +
                "key=" + key +
                ", threadName='" + threadName + '\'' +
                ", lockRequestedTime=" + lockRequestedTime +
                '}';
        }

        private List<String> determineStackTrace() {
            StackTraceElement[] elements = Thread.currentThread().getStackTrace();
            return Arrays.stream(elements)
                .skip(2L) // First two lines of stack trace are uninteresting, because they reflect this place
                .limit(10L) // Limit number of lines to keep it manageable
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
        }

        public K getKey() {
            return key;
        }

        public String getThreadName() {
            return threadName;
        }

        public LocalDateTime getLockRequestedTime() {
            return lockRequestedTime;
        }

        public LocalDateTime getLockAcquiredTime() {
            return lockAcquiredTime;
        }

        public LocalDateTime getLockReleasedTime() {
            return lockReleasedTime;
        }

        public List<String> getLockRequestedTrace() {
            return lockRequestedTrace;
        }

        public List<String> getLockReleasedTrace() {
            return lockReleasedTrace;
        }
    }

}

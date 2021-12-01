package org.jivesoftware.openfire.plugin.util.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Thread.currentThread;

public class ClusteredCacheLockTracker {

    // Maps for tracking information about locks
    // Not using ConcurrentHashMap on purpose - we do want to see errors occur if there are concurrent modifications - that may tell us something
    private static final int REGISTRATION_MAX_SIZE_PER_KEY = 20;
    private static final long HANGING_LOCK_DETECTION_THRESHOLD_SECONDS = 30L;
    private static final Map<LockId, LinkedList<LockInfo>> lockRegistration = new HashMap<>();
    private static final Map<LockId, LinkedList<LockInfo>> lockRegistrationArchive = new HashMap<>();
    private final ThreadLocal<Map<LockId, LockInfo>> currentCacheLocksByThisThread = new ThreadLocal<>();

    private final Logger logger;

    public ClusteredCacheLockTracker() {
        this.logger = LoggerFactory.getLogger(ClusteredCacheLockTracker.class.getName());
    }

    /**
     * Adds a LockInfo object to the lock registration for the specified cache item.
     * @param cacheName The name of the cache that the lock is requested for.
     * @param cacheKey The key of the cache item that the lock is requested for.
     */
    void registerLockRequested(final String cacheName, final Object cacheKey) {
        final LockId lockId = new LockId(cacheName, cacheKey);
        final String thisThreadName = currentThread().getName();
        logger.debug("Thread {} requested a lock on {}.", thisThreadName, lockId);
        LockInfo lockInfo = new LockInfo(lockId, thisThreadName);
        final LinkedList<LockInfo> lockInfosForThisLockId = lockRegistration.computeIfAbsent(lockId, k -> new LinkedList<>());
        if (lockInfosForThisLockId.size() >= REGISTRATION_MAX_SIZE_PER_KEY) {
            logger.warn("Not registering lock request by thread {} on {} because the registration has reached capacity.", thisThreadName, lockId);
        } else {
            // Maybe the lock info is already in there, because of the reentrant nature of the locking mechanism. In
            // that case do not register again.
            boolean lockAlreadyRegistered = lockInfosForThisLockId.stream().anyMatch(li -> li.getLockId().equals(lockId));
            if (!lockAlreadyRegistered) {
                lockInfosForThisLockId.addFirst(lockInfo);
            }
        }
        
        // Also register in the threadlocal
        if (currentCacheLocksByThisThread.get() == null) {
            currentCacheLocksByThisThread.set(new HashMap<>());
        }
        if (currentCacheLocksByThisThread.get().containsKey(lockId)) {
            // This same thread is already holding or acquiring a lock on the same entry. That is perfectly reasonable
            // because of the reentrant nature of locks. That existing lock will be the one returned, so we don't need
            // to do anything here.
        } else {
            currentCacheLocksByThisThread.get().put(lockId, lockInfo);
        }
        if (currentCacheLocksByThisThread.get().size() > 1) {
            // This same thread is already holding or acquiring some other lock. This is fishy. Log a warning.
            logger.warn("Thread {} requested a lock for {}, but is also operating locks on {}.", thisThreadName, lockId, currentCacheLocksByThisThread.get().keySet());
        }
    }

    /**
     * Updates an existing LockInfo object in the registration to reflect that that lock as actually been applied.
     */
    void registerLockAcquired(final String cacheName, final Object cacheKey, final boolean acquireWasSuccessful) {
        final LockId lockId = new LockId(cacheName, cacheKey);
        final String thisThreadName = currentThread().getName();
        LockInfo theLockInfo = null;

        if (!acquireWasSuccessful) {
            logger.warn("Thread {} tried to acquire a lock on {}, but failed to get it.", thisThreadName, lockId);
            return;
        }

        logger.debug("Thread {} acquired a lock on {}", thisThreadName, lockId);
        LinkedList<LockInfo> lockInfoListForThisLockId = lockRegistration.get(lockId);
        if (lockInfoListForThisLockId == null || lockInfoListForThisLockId.isEmpty()) {
            logger.warn("Thread {} is supposed to have acquired a lock on {}, but there is no lock registered for that key.", thisThreadName, lockId);
        } else {
            // Find the lock info object, hopefully at the beginning of the list
            final List<LockInfo> moreRecentlyRegisteredLocks = new ArrayList<>();
            for (LockInfo lockInfo : lockInfoListForThisLockId) {
                if ((lockInfo.isWaiting() || lockInfo.isActive()) && lockInfo.getThreadName().equals(thisThreadName)) {
                    // Found it!
                    theLockInfo = lockInfo;
                    break;
                } else {
                    moreRecentlyRegisteredLocks.add(lockInfo);
                }
            }
            if (theLockInfo == null) {
                logger.warn("Thread {} is supposed to have acquired a lock on {}, but there is no lock registered for that.", thisThreadName, lockId);
            } else {
                if (!moreRecentlyRegisteredLocks.isEmpty()) {
                    logger.warn("Thread {} is supposed to have acquired a lock on {}, but this is not the most recent thread to have requested a lock. This may still be a valid situation because threads sometimes need to queue in waiting for a lock. Other locks registered more recently: {}.", thisThreadName, lockId, moreRecentlyRegisteredLocks);
                }
                theLockInfo.registerAcquired();
            }
        }

        // Also register in the threadlocal
        if (theLockInfo != null) {
            if (!currentCacheLocksByThisThread.get().containsKey(lockId)) {
                // This is strange, it should be in there.
                logger.warn("Thread {} acquired the lock for {}, but it has disappeared from the threadlocal registration.", thisThreadName, lockId);
            }
            currentCacheLocksByThisThread.get().put(lockId, theLockInfo); // Does not matter if it was already there, then this just replaces
        }
        if (currentCacheLocksByThisThread.get().size() > 1) {
            // This same thread is already holding or acquiring some other lock. This is fishy. Log a warning.
            logger.warn("Thread {} acquired a lock for {}, but is also operating locks on {}.", thisThreadName, lockId, currentCacheLocksByThisThread.get().keySet());
        }
    }

    /**
     * Updates an existing LockInfo object in the registration to reflect that that lock as been released.
     * This also moves the lock info from the lock registration to the archive.
     */
    void registerLockReleased(final String cacheName, final Object cacheKey) {
        final LockId lockId = new LockId(cacheName, cacheKey);
        final String thisThreadName = currentThread().getName();
        LockInfo theLockInfo = null;

        logger.debug("Thread {} released a lock on {}.", thisThreadName, lockId);
        LinkedList<LockInfo> lockInfoListForThisLockId = lockRegistration.get(lockId);
        if (lockInfoListForThisLockId == null || lockInfoListForThisLockId.isEmpty()) {
            logger.warn("Thread {} is supposed to have acquired a lock on {}, but there is no lock registered for that.", thisThreadName, lockId);
        } else {
            // Find the lock info object, hopefully at the beginning of the list
            final List<LockInfo> moreRecentlyRegisteredLocks = new ArrayList<>();
            for (LockInfo lockInfo : lockInfoListForThisLockId) {
                if (lockInfo.isActive() && lockInfo.getThreadName().equals(thisThreadName)) {
                    // Found it!
                    theLockInfo = lockInfo;
                    break;
                } else {
                    moreRecentlyRegisteredLocks.add(lockInfo);
                }
            }
            if (theLockInfo == null) {
                logger.warn("Thread {} is supposed to have acquired a lock on {}, but there is no lock registered for that.", thisThreadName, lockId);
            } else {
                if (!moreRecentlyRegisteredLocks.isEmpty()) {
                    logger.warn("Thread {} is supposed to have acquired a lock on {}, but this is not the most recent thread to have requested a lock. This may still be a valid situation because threads sometimes need to queue in waiting for a lock. Other locks registered more recently: {}.", thisThreadName, lockId, moreRecentlyRegisteredLocks);
                }
                final LockInfo currentLockHolder = findCurrentLockHolder(lockId).orElse(null);
                if (theLockInfo.equals(currentLockHolder)) {
                    if (theLockInfo.registerReleased()) {
                        moveToArchive(theLockInfo);
                    }
                } else {
                    logger.warn("Thread {} is supposed to have acquired lock {}, but currently lock {} is the active one.", thisThreadName, theLockInfo, currentLockHolder);
                }
            }
        }

        // Also register in the threadlocal
        if (theLockInfo != null) {
            if (!currentCacheLocksByThisThread.get().containsKey(lockId)) {
                // This is strange, it should be in there.
                logger.warn("Thread {} released the lock for {}, but it has disappeared from the threadlocal registration.", thisThreadName, lockId);
            }
            if (!theLockInfo.isActive()) {
                currentCacheLocksByThisThread.get().remove(lockId);
            }
        }
    }

    /**
     * Removes the referenced LockInfo from the registration and adds it to the archive.
     * If the archive is at full capacity, the oldest entry is removed.
     * @param lockInfo The lock info to archive.
     */
    private void moveToArchive(@Nonnull LockInfo lockInfo) {
        final LinkedList<LockInfo> lockInfos = lockRegistration.get(lockInfo.getLockId());
        if (lockInfos == null || !lockInfos.remove(lockInfo)) {
            logger.warn("Can't move {} to lock registration archive because it was not found in the lock registration.", lockInfo);
        } else {
            lockRegistrationArchive.computeIfAbsent(lockInfo.getLockId(), k -> new LinkedList<>()).addFirst(lockInfo);
            if (lockRegistrationArchive.get(lockInfo.getLockId()).size() > REGISTRATION_MAX_SIZE_PER_KEY) {
                lockRegistrationArchive.get(lockInfo.getLockId()).removeLast();
            }
            logger.debug("Moved {} to lock registration archive. Current registration size is {} and archive size is {}.", lockInfo, lockInfos.size(), lockRegistrationArchive.get(lockInfo.getLockId()).size());
        }
    }

    /**
     * Find the current holder of an active lock on the referenced key.
     * @return The active lock holder, if any.
     */
    private Optional<LockInfo> findCurrentLockHolder(final LockId lockId) {
        final List<LockInfo> activeLocksForLockId = lockRegistration.computeIfAbsent(lockId, k -> new LinkedList<>()).stream().filter(LockInfo::isActive).collect(Collectors.toList());
        if (activeLocksForLockId.isEmpty()) {
            logger.debug("There is no active lock holder for {}", lockId);
            return Optional.empty();
        } else if (activeLocksForLockId.size() == 1) {
            logger.trace("There is a single active lock holder for {}: {}", lockId, activeLocksForLockId.get(0));
            return Optional.of(activeLocksForLockId.get(0));
        } else {
            logger.error("There is more than one active lock holder for {}: {}", lockId, activeLocksForLockId);
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
            .filter(lockInfo -> lockInfo.getLockAcquiredTime().isBefore(lockAcquiredThreshold))
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

    /**
     * Returns names of all threads that are currently holding a lock on an entry in this cache, or waiting for one.
     * This can be used to build a 'global' set of all threads that are currently locking things.
     * @return
     */
    public Set<String> getAllThreadNamesHoldingOrWaitingForLocks() {
        return lockRegistration.values().stream()
            .flatMap(LinkedList::stream)
            .map(LockInfo::getThreadName)
            .collect(Collectors.toSet());
    }

    /**
     * Returns information about all locks in this cache that are currently held or waited for by the referenced thread.
     * @param threadName
     * @return
     */
    public Set<LockInfo> getAllLocksHeldByOrWaitingForByThread(final String threadName) {
        return lockRegistration.values().stream()
            .flatMap(LinkedList::stream)
            .filter(lockInfo -> lockInfo.getThreadName().equals(threadName))
            .collect(Collectors.toSet());
    }

    class LockId implements Serializable {
        private String cacheName;
        private Object cacheKey;

        public LockId(String cacheName, Object cacheKey) {
            this.cacheName = cacheName;
            this.cacheKey = cacheKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockId lockId = (LockId) o;
            return cacheName.equals(lockId.cacheName) && cacheKey.equals(lockId.cacheKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cacheName, cacheKey);
        }

        @Override
        public String toString() {
            return "LockId{" +
                "cacheName='" + cacheName + '\'' +
                ", cacheKey=" + cacheKey +
                '}';
        }

        public String getCacheName() {
            return cacheName;
        }

        public Object getCacheKey() {
            return cacheKey;
        }
    }

    class LockInfo implements Serializable {
        private LockId lockId;
        private String threadName;
        private LocalDateTime lockRequestedTime;
        private LocalDateTime lockAcquiredTime;
        private LocalDateTime lockReleasedTime;
        private List<String> lockRequestedTrace;
        private List<String> lockReleasedTrace;
        private int lockAcquiredCount;

        public LockInfo(final LockId lockId, final String threadName) {
            this.lockId = lockId;
            this.threadName = threadName;
            registerRequested();
        }

        private void registerRequested() {
            this.lockRequestedTime = LocalDateTime.now();
            this.lockRequestedTrace = determineStackTrace();
        }

        public void registerAcquired() {
            this.lockAcquiredTime = LocalDateTime.now();
            this.lockAcquiredCount++;
        }

        public boolean registerReleased() {
            this.lockReleasedTime = LocalDateTime.now();
            this.lockReleasedTrace = determineStackTrace();
            return --this.lockAcquiredCount == 0;
        }

        public boolean isWaiting() {
            return this.lockRequestedTime != null && this.lockAcquiredTime == null;
        }

        public boolean isActive() {
            return this.lockAcquiredCount > 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockInfo lockInfo = (LockInfo) o;
            return lockId.equals(lockInfo.lockId) && threadName.equals(lockInfo.threadName) && Objects.equals(lockRequestedTime, lockInfo.lockRequestedTime) && Objects.equals(lockAcquiredTime, lockInfo.lockAcquiredTime) && Objects.equals(lockReleasedTime, lockInfo.lockReleasedTime) && Objects.equals(lockRequestedTrace, lockInfo.lockRequestedTrace) && Objects.equals(lockReleasedTrace, lockInfo.lockReleasedTrace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lockId, threadName, lockRequestedTime, lockAcquiredTime, lockReleasedTime, lockRequestedTrace, lockReleasedTrace);
        }

        @Override
        public String toString() {
            return "LockInfo{" +
                "lockId=" + lockId +
                ", threadId='" + threadName + '\'' +
                ", lockRequestedTime=" + lockRequestedTime +
                ", lockAcquiredTime=" + lockAcquiredTime +
                ", lockReleasedTime=" + lockReleasedTime +
                '}';
        }

        private List<String> determineStackTrace() {
            StackTraceElement[] elements = currentThread().getStackTrace();
            return Arrays.stream(elements)
                .skip(2L) // First two lines of stack trace are uninteresting, because they reflect this place
                .limit(10L) // Limit number of lines to keep it manageable
                .map(StackTraceElement::toString)
                .collect(Collectors.toList());
        }

        public LockId getLockId() {
            return lockId;
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

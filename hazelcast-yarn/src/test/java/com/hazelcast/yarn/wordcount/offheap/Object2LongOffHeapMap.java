package com.hazelcast.yarn.wordcount.offheap;

import sun.misc.Unsafe;
import com.hazelcast.yarn.impl.YarnUtil;

import java.util.NoSuchElementException;

public class Object2LongOffHeapMap {
    private final Unsafe unsafe = YarnUtil.getUnsafe();

    private long itemCount;
    private final int addressSize;
    private final int partitionCount;
    private final long partitionAddress;

    public Object2LongOffHeapMap(int partitionCount) {
        this.partitionCount = partitionCount;
        this.addressSize = unsafe.addressSize();
        this.partitionAddress = allocate(this.partitionCount * addressSize, true);
    }

    private long allocate(long size, boolean init) {
        final long address = unsafe.allocateMemory(size);

        for (long offset = 0; init && offset < size; offset++) {
            unsafe.putByte(address + offset, (byte) 0);
        }

        return address;
    }


    public int size() {
        if (itemCount > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) itemCount;
    }

    public long get(long address, long keySize) {
        final int hash = Math.abs(this.keyHash(address, keySize));

        final long offset = hash % partitionCount;

        // This is the location of the partition on which the entry key belongs
        long locationAddress = unsafe.getAddress(partitionAddress + (offset * addressSize));

        // Skip if unallocated
        if (locationAddress == 0) {
            return -1;
        }

        // Read how many entries we expect in this partition
        int entryCount = unsafe.getInt(locationAddress);

        // Move pointer past size int
        locationAddress += Long.BYTES;

        for (long locationOffset = 0; locationOffset < entryCount; locationOffset++) {
            // Address of key within partition
            long keyAddress = unsafe.getAddress(locationAddress + (locationOffset * addressSize * 2));

            // Get size of key
            int size = unsafe.getInt(keyAddress);

            // If size of this key is different than the one
            // we're looking for, continue..
            if (size != keySize) {
                continue;
            }

            // Move pointer past size int
            keyAddress += Long.BYTES;

            // Scan each byte to check for differences
            boolean isEqual = true;
            for (int keyOffset = 0; keyOffset < keySize; keyOffset++) {
                if (unsafe.getByte(address + keyOffset) != unsafe.getByte(keyAddress + keyOffset)) {
                    isEqual = false;
                    break;
                }
            }

            // Check if we found the key
            if (isEqual) {
                long valueAddress = unsafe.getAddress(locationAddress + (locationOffset * addressSize * 2) + addressSize);

                // Check if this is a null value
                if (valueAddress == 0) {
                    return -1;
                }

                // Move pointer past size int
                valueAddress += Long.BYTES;

                return unsafe.getLong(valueAddress);
            }
        }

        return -1;
    }

    private int keyHash(long address, long keySize) {
        int result = 1;

        for (long pointer = 0; pointer < keySize; pointer++) {
            byte element = unsafe.getByte(address + pointer);
            int elementHash = (element ^ (element >>> 4));
            result = 31 * result + elementHash;
        }

        return result;
    }

    public void put(long address, long keySize, long value) {
        final int hash = Math.abs(this.keyHash(address, keySize));
        final long offset = hash % partitionCount;

        // This is the location of the partition on which the entry key belongs
        long locationAddress = unsafe.getAddress(this.partitionAddress + (offset * this.addressSize));

        // Read how many entries we expect in this partition
        int entryCount = locationAddress == 0 ? 0 : unsafe.getInt(locationAddress);

        // Move pointer past size int
        locationAddress += Long.BYTES;

        for (long locationOffset = 0; locationOffset < entryCount; locationOffset++) {
            // Address of key within partition
            long keyAddress = unsafe.getAddress(locationAddress + (locationOffset * this.addressSize * 2));

            // Get size of key
            int size = unsafe.getInt(keyAddress);

            // If size of this key is different than the one
            // we're looking for, continue..
            if (size != keySize) {
                continue;
            }

            // Move pointer past size int
            keyAddress += Long.BYTES;

            // Scan each byte to check for differences
            boolean isEqual = true;

            for (int keyOffset = 0; keyOffset < keySize; keyOffset++) {
                if (unsafe.getByte(address + keyOffset) != unsafe.getByte(keyAddress + keyOffset)) {
                    isEqual = false;
                    break;
                }
            }

            // Check if we found the key
            if (isEqual) {
                long valueAddress = unsafe.getAddress(locationAddress + (locationOffset * addressSize * 2) + addressSize);
                unsafe.putLong(valueAddress + Long.BYTES, value);

                // Update value address in partition
                unsafe.putAddress(locationAddress + (locationOffset * addressSize * 2) + addressSize, valueAddress);
                return;
            }
        }

        // Existing entry not found on key, insert new
        itemCount++;

        // Move partition pointer back to start
        locationAddress -= Long.BYTES;

        // Allocate and copy key
        long keyAddress = allocate(Long.BYTES + keySize, false);
        this.unsafe.putLong(keyAddress, keySize);

        unsafe.copyMemory(address, keyAddress + Long.BYTES, keySize);

        // Allocate and copy value
        int valueSize = Long.SIZE;
        long valueAddress = allocate(Long.BYTES + valueSize, false);
        unsafe.putInt(valueAddress, valueSize);
        unsafe.putLong(valueAddress + Long.BYTES, value);

        // Allocate or reallocate partition
        if (locationAddress == 0) {
            locationAddress = allocate(Long.BYTES + this.addressSize + this.addressSize, false);
        } else {
            //locationAddress = allocate(Long.BYTES + (this.addressSize * 2 * (entryCount + 1)), false);
            if (Integer.bitCount(entryCount) == 1) {
                locationAddress = unsafe.reallocateMemory(locationAddress, Long.BYTES + (this.addressSize * 2 * (entryCount * 2)));
            }
        }

        // Insert key and value pointers
        unsafe.putAddress(locationAddress + Long.BYTES + (this.addressSize * 2 * entryCount), keyAddress);
        unsafe.putAddress(locationAddress + Long.BYTES + (this.addressSize * 2 * entryCount) + this.addressSize, valueAddress);

        // Update entry count
        unsafe.putInt(locationAddress, entryCount + 1);

        // Update pointer to partition
        unsafe.putAddress(partitionAddress + (offset * this.addressSize), locationAddress);
    }

    public void clear() {
        // For each partition..
        for (long offset = 0; offset < partitionCount; offset++) {
            // ..get partition address
            long locationAddress = unsafe.getAddress(partitionAddress + (offset * addressSize));

            // Skip if unallocated
            if (locationAddress == 0)
                continue;

            // Read how many entries we expect in this partition
            int entryCount = unsafe.getInt(locationAddress);

            // Move pointer past size int
            locationAddress += Long.BYTES;

            for (long locationOffset = 0; locationOffset < entryCount; locationOffset++) {
                long keyAddress = unsafe.getAddress(locationAddress + (locationOffset * addressSize * 2));

                if (keyAddress != 0)
                    unsafe.freeMemory(keyAddress);

                long valueAddress = unsafe.getAddress(locationAddress + (locationOffset * addressSize * 2) + addressSize);

                if (valueAddress != 0)
                    unsafe.freeMemory(valueAddress);
            }

            locationAddress -= Long.BYTES;

            unsafe.freeMemory(locationAddress);

            unsafe.putAddress(partitionAddress + (offset * addressSize), 0);
        }

        // Reset item counter
        itemCount = 0;
    }

    protected void finalize() throws Throwable {
        // clear() will free all memory but the partition area itself
        this.clear();

        // Finally free the partition area itself
        unsafe.freeMemory(partitionAddress);
        super.finalize();
    }

    public static class KeySetIterator {
        private long keyDataSize;
        private long offset, locationOffset;
        private final Object2LongOffHeapMap map;

        public long getKeyDataSize() {
            return keyDataSize;
        }

        public KeySetIterator(Object2LongOffHeapMap map) {
            this.map = map;
        }

        public boolean hasNext() {
            while (offset < map.partitionCount) {
                long locationAddress = map.unsafe.getAddress(map.partitionAddress + (offset * map.addressSize));

                if (locationAddress == 0) {
                    offset++;
                    continue;
                }

                break;
            }

            return offset < map.partitionCount;
        }

        public long nextKeyAddress() {
            if (offset >= map.partitionCount)
                throw new NoSuchElementException();

            long locationAddress = map.unsafe.getAddress(map.partitionAddress + (offset * map.addressSize));

            while (locationAddress == 0) {
                offset++;

                if (offset >= map.partitionCount)
                    throw new NoSuchElementException();

                locationAddress = map.unsafe.getAddress(map.partitionAddress + (offset * map.addressSize));
            }

            // Read how many entries we expect in this partition
            int entryCount = map.unsafe.getInt(locationAddress);

            // Move pointer past size int
            locationAddress += Long.BYTES;

            long keyAddress = map.unsafe.getAddress(locationAddress + (locationOffset * map.addressSize * 2));

            // Get size of key
            int keyDataSize = map.unsafe.getInt(keyAddress);

            // Move pointer past size int
            keyAddress += Long.BYTES;

            locationOffset++;

            if (locationOffset >= entryCount) {
                locationOffset = 0;
                offset++;
            }

            this.keyDataSize = keyDataSize;
            return keyAddress;
        }
    }
}

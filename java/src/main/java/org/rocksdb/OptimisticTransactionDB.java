// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.List;

/**
 * Database with Transaction support.
 */
public class OptimisticTransactionDB extends RocksObject
    implements TransactionalDB<OptimisticTransactionOptions> {

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB baseDB;

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ OptimisticTransactionDB
   *     object
   */
  private OptimisticTransactionDB(final long nativeHandle) {
    super(nativeHandle);
    baseDB = new RocksDB(getBaseDB(nativeHandle_));
  }

  /**
   * Open an OptimisticTransactionDB similar to
   * {@link RocksDB#open(Options, String)}
   */
  public static OptimisticTransactionDB open(final Options options,
      final String path) throws RocksDBException {
    final OptimisticTransactionDB otdb = new OptimisticTransactionDB(open(
        options.nativeHandle_, path));

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    otdb.storeOptionsInstance(options);

    return otdb;
  }

  public static OptimisticTransactionDB open(final Options options,
                                             final String path,
                                             final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                                             final List<ColumnFamilyHandle> columnFamilyHandles)
          throws RocksDBException {

    final OptimisticTransactionDB otdb = open(options.nativeHandle_, path, columnFamilyDescriptors, columnFamilyHandles);

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    otdb.storeOptionsInstance(options);

    return otdb;
  }

  /**
   * Open an OptimisticTransactionDB similar to
   * {@link RocksDB#open(DBOptions, String, List, List)}
   */
  public static OptimisticTransactionDB open(final DBOptions dbOptions,
      final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {

    final OptimisticTransactionDB otdb = open(dbOptions.nativeHandle_, path, columnFamilyDescriptors, columnFamilyHandles);

    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    otdb.storeOptionsInstance(dbOptions);

    return otdb;
  }

  private static OptimisticTransactionDB open(final long dbOptionsNativeHandle,
                                             final String path,
                                             final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                                             final List<ColumnFamilyHandle> columnFamilyHandles)
          throws RocksDBException {

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = open(dbOptionsNativeHandle, path, cfNames,
        cfOptionHandles);
    final OptimisticTransactionDB otdb =
        new OptimisticTransactionDB(handles[0]);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(otdb.baseDB, handles[i]));
    }

    return otdb;
  }

  private void storeOptionsInstance(DBOptionsInterface options) {
    options_ = options;
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions) {
    return new Transaction(baseDB, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_));
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final OptimisticTransactionOptions optimisticTransactionOptions) {
    return new Transaction(baseDB, beginTransaction(nativeHandle_,
        writeOptions.nativeHandle_,
        optimisticTransactionOptions.nativeHandle_));
  }

  // TODO(AR) consider having beingTransaction(... oldTransaction) set a
  // reference count inside Transaction, so that we can always call
  // Transaction#close but the object is only disposed when there are as many
  // closes as beginTransaction. Makes the try-with-resources paradigm easier for
  // java developers

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final Transaction oldTransaction) {
    final long jtxn_handle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxn_handle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  @Override
  public Transaction beginTransaction(final WriteOptions writeOptions,
      final OptimisticTransactionOptions optimisticTransactionOptions,
      final Transaction oldTransaction) {
    final long jtxn_handle = beginTransaction_withOld(nativeHandle_,
        writeOptions.nativeHandle_, optimisticTransactionOptions.nativeHandle_,
        oldTransaction.nativeHandle_);

    // RocksJava relies on the assumption that
    // we do not allocate a new Transaction object
    // when providing an old_txn
    assert(jtxn_handle == oldTransaction.nativeHandle_);

    return oldTransaction;
  }

  public RocksDB getBaseDB() {
    return baseDB;
  }

  @Override
  protected void disposeInternal() {
    // this.nativeHandle owns the baseDB.nativeHandle.
    // And baseDB.nativeHandle will be destroyed when the this.nativeHandle is destroyed.
    baseDB.disOwnNativeHandle();
    super.disposeInternal();
  }

  protected static native long open(final long optionsHandle,
      final String path) throws RocksDBException;
  protected static native long[] open(final long handle, final String path,
      final byte[][] columnFamilyNames, final long[] columnFamilyOptions);
  private native long beginTransaction(final long handle,
      final long writeOptionsHandle);
  private native long beginTransaction(final long handle,
      final long writeOptionsHandle,
      final long optimisticTransactionOptionsHandle);
  private native long beginTransaction_withOld(final long handle,
      final long writeOptionsHandle, final long oldTransactionHandle);
  private native long beginTransaction_withOld(final long handle,
      final long writeOptionsHandle,
      final long optimisticTransactionOptionsHandle,
      final long oldTransactionHandle);
  protected native long getBaseDB(final long handle);
  @Override protected final native void disposeInternal(final long handle);
  private DBOptionsInterface options_;
}

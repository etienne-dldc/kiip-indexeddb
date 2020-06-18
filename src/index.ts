import {
  KiipDatabase,
  KiipFragment,
  KiipDocumentInternal,
  Timestamp,
  OnFragment
} from '@kiip/core';
import { openDB, DBSchema, IDBPTransaction } from 'idb';

export interface BackendDB extends DBSchema {
  fragments: {
    value: KiipFragment;
    key: string;
    indexes: {
      byDocument: string;
    };
  };
  documents: {
    key: string;
    value: KiipDocumentInternal;
  };
}

export type BackendTransaction = IDBPTransaction<BackendDB, ['documents', 'fragments']>;

export async function KiipIndexedDB(dbName: string): Promise<KiipDatabase<BackendTransaction>> {
  const db = await openDB<BackendDB>(dbName, 1, {
    upgrade(db) {
      db.createObjectStore('documents', {
        keyPath: 'id'
      });

      const fragmetsStore = db.createObjectStore('fragments', {
        keyPath: ['documentId', 'timestamp']
      });
      fragmetsStore.createIndex('byDocument', 'documentId');
    }
  });

  return {
    withTransaction,
    addDocument,
    addFragments,
    getDocument,
    getDocuments,
    getFragmentsSince,
    onEachFragment
  };

  async function onEachFragment(
    tx: BackendTransaction,
    documentId: string,
    onFragment: OnFragment
  ): Promise<void> {
    const fragmentsStore = tx.objectStore('fragments');
    let cursor = await fragmentsStore.index('byDocument').openCursor(documentId);
    if (!cursor) {
      return;
    }
    while (cursor) {
      const fragment = cursor.value;
      onFragment(fragment);
      cursor = await cursor.continue();
    }
  }

  async function getFragmentsSince(
    tx: BackendTransaction,
    documentId: string,
    timestamp: Timestamp,
    skipNodeId: string
  ): Promise<Array<KiipFragment>> {
    const fragmentsStore = tx.objectStore('fragments');
    // find all message after timestamp except the ones emitted by skipNodeId
    let cursor = await fragmentsStore.index('byDocument').openCursor(documentId);
    if (!cursor) {
      return [];
    }
    const fragments: Array<KiipFragment> = [];
    while (cursor) {
      const ts = Timestamp.parse(cursor.value.timestamp);
      if (timestamp <= ts && ts.node !== skipNodeId) {
        fragments.push(cursor.value);
      }
      cursor = await cursor.continue();
    }
    return fragments;
  }

  async function getDocuments(tx: BackendTransaction): Promise<Array<KiipDocumentInternal>> {
    const docsStore = tx.objectStore('documents');
    const docs = await docsStore.getAll();
    return docs;
  }

  async function getDocument(
    tx: BackendTransaction,
    documentId: string
  ): Promise<KiipDocumentInternal> {
    const doc = await tx.objectStore('documents').get(documentId);
    if (!doc) {
      throw new Error(`Cannot find document ${documentId}`);
    }
    return doc;
  }

  async function addFragments(
    tx: BackendTransaction,
    fragments: Array<KiipFragment>
  ): Promise<void> {
    const fragmentsStore = tx.objectStore('fragments');
    for await (const fragment of fragments) {
      await fragmentsStore.add(fragment);
    }
  }

  async function addDocument(
    tx: BackendTransaction,
    document: KiipDocumentInternal
  ): Promise<void> {
    await tx.objectStore('documents').add(document);
  }

  async function withTransaction<T>(exec: (tx: BackendTransaction) => Promise<T>): Promise<T> {
    const tx: BackendTransaction = db.transaction(['documents', 'fragments'], 'readwrite');
    let res;
    try {
      res = await exec(tx);
    } catch (error) {
      throw error;
    } finally {
      await tx.done;
    }
    return res;
  }
}

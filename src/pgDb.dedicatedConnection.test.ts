import { PgDb } from "./pgDb.js";

/**
 * A failing query on a dedicated connection (transaction / dedicatedConnectionBegin) must not release
 * that connection back to the pool: it is owned by the PgDb instance until transactionRollback() /
 * transactionCommit() / dedicatedConnectionEnd(). Releasing it early lets another caller check out the same
 * client, and the stale owner's later ROLLBACK / release() then hits the other caller's transaction.
 */
describe("dedicated connection - query errors", () => {
    let pgdb: PgDb;

    /** waits until the stream ends, returns the emitted error (if any) */
    function drain(stream: NodeJS.ReadableStream): Promise<Error | null> {
        return new Promise(resolve => {
            stream.on('data', () => { });
            stream.on('end', () => resolve(null));
            stream.on('error', (e: Error) => resolve(e));
        });
    }

    function withTimeout<T>(p: Promise<T>, ms = 2000): Promise<T | 'timeout'> {
        let timer: NodeJS.Timeout;
        return Promise.race([
            p,
            new Promise<'timeout'>(r => { timer = setTimeout(() => r('timeout'), ms); })
        ]).finally(() => clearTimeout(timer));
    }

    beforeAll(async () => {
        // pool max 1: a second checkout can only ever get the very same client
        pgdb = await PgDb.connect({ connectionString: "postgres://", max: 1 });
    });

    afterEach(() => {
        expect(pgdb.pool.totalCount - pgdb.pool.idleCount).toEqual(0); // nothing left checked out
        expect(pgdb.pool.waitingCount).toEqual(0);
    });

    afterAll(async () => {
        await pgdb.close();
    });

    it("query error keeps the transaction on the same connection, rollback succeeds", async () => {
        let tx = await pgdb.transactionBegin();
        let pid = await tx.queryOneField('SELECT pg_backend_pid()');

        await expect(tx.query('SELECT 1/0')).rejects.toThrow(/division by zero/);
        expect(tx.isTransactionActive()).toBe(true);
        expect(pgdb.pool.idleCount).toEqual(0); // still checked out by tx

        // postgres side the transaction is aborted, on the very same backend
        await expect(tx.query('SELECT 1')).rejects.toThrow(/current transaction is aborted/);

        await tx.transactionRollback();
        expect(tx.isTransactionActive()).toBe(false);
        expect(await pgdb.queryOneField('SELECT pg_backend_pid()')).toEqual(pid);
    });

    it("failed query in transaction A does not leak into transaction B (pool max 1)", async () => {
        let txA = await pgdb.transactionBegin();
        await expect(txA.query('SELECT 1/0')).rejects.toThrow();

        // B has to wait for A's connection, it must not get it before A ends
        let txBPromise = pgdb.transactionBegin();
        expect(await withTimeout(txBPromise, 300)).toEqual('timeout');

        await txA.transactionRollback();
        let txB = await txBPromise;

        await txB.query('CREATE TEMP TABLE dedicated_conn_test(x int)');
        await txB.query('INSERT INTO dedicated_conn_test VALUES (1)');
        expect(await txB.queryOneField('SELECT count(*)::int FROM dedicated_conn_test')).toEqual(1);
        await txB.transactionRollback(); // must not throw "already been released to the pool"
        expect(pgdb.pool.idleCount).toEqual(1);
    });

    it("savepoint: failing query, rollback to savepoint, then commit", async () => {
        await pgdb.query('CREATE TABLE IF NOT EXISTS dedicated_conn_sp(x int)');
        await pgdb.query('TRUNCATE dedicated_conn_sp');
        try {
            let tx = await pgdb.transactionBegin();
            await tx.query('INSERT INTO dedicated_conn_sp VALUES (1)');
            await tx.savePoint('sp');
            await expect(tx.query('SELECT 1/0')).rejects.toThrow();
            await tx.transactionRollback({ savePoint: 'sp' });
            await tx.query('INSERT INTO dedicated_conn_sp VALUES (2)');
            await tx.transactionCommit();

            expect(await pgdb.queryOneColumn('SELECT x FROM dedicated_conn_sp ORDER BY x')).toEqual([1, 2]);
        } finally {
            await pgdb.query('DROP TABLE dedicated_conn_sp');
        }
    });

    it("dedicatedConnectionBegin: query error keeps the connection until dedicatedConnectionEnd", async () => {
        let dc = await pgdb.dedicatedConnectionBegin();
        await expect(dc.query('SELECT * FROM not_existing_table_xyz')).rejects.toThrow();
        expect(pgdb.pool.idleCount).toEqual(0);
        expect(await dc.queryOneField('SELECT 1')).toEqual(1);
        await dc.dedicatedConnectionEnd();
        expect(pgdb.pool.idleCount).toEqual(1);
    });

    it("queryWithOnCursorCallback: error in transaction keeps the connection", async () => {
        let tx = await pgdb.transactionBegin();
        await expect(tx.queryWithOnCursorCallback('SELECT 1/0', null, null, () => false)).rejects.toThrow();
        expect(tx.isTransactionActive()).toBe(true);
        expect(pgdb.pool.idleCount).toEqual(0);
        expect(await withTimeout(tx.transactionRollback())).not.toEqual('timeout');
        expect(pgdb.pool.idleCount).toEqual(1);
    });

    it("queryAsStream: SQL error in transaction keeps the connection", async () => {
        let tx = await pgdb.transactionBegin();
        let stream = await tx.queryAsStream('SELECT 1/0');
        let err = await withTimeout(drain(stream));
        expect(err).toBeInstanceOf(Error);
        expect(tx.isTransactionActive()).toBe(true);
        expect(pgdb.pool.idleCount).toEqual(0);
        // the connection is still usable (got its ReadyForQuery), postgres reports the aborted transaction
        await expect(withTimeout(tx.query('SELECT 1'))).rejects.toThrow(/current transaction is aborted/);
        expect(await withTimeout(tx.transactionRollback())).not.toEqual('timeout');
        expect(pgdb.pool.idleCount).toEqual(1);
    });

    it("queryAsStream: conversion error in transaction keeps the connection usable", async () => {
        let tx = await pgdb.transactionBegin();
        // more rows than one cursor batch, so the cursor is still open when the conversion fails
        let stream = await tx.queryAsStream(`SELECT * FROM generate_series(${Number.MAX_SAFE_INTEGER - 2}, ${Number.MAX_SAFE_INTEGER + 1000}) num`);
        let err = await withTimeout(drain(stream));
        expect(err).toBeInstanceOf(Error); // JS side error: unknown oid / number can't be represented
        expect(pgdb.pool.idleCount).toEqual(0);
        expect(await withTimeout(tx.queryOneField('SELECT 42'))).toEqual(42);
        await tx.transactionRollback();
        expect(pgdb.pool.idleCount).toEqual(1);
    });

    it("without transaction: failing queries release their own connection exactly once", async () => {
        let idleBefore = pgdb.pool.idleCount;
        await expect(pgdb.query('SELECT 1/0')).rejects.toThrow();
        expect(pgdb.pool.idleCount).toEqual(idleBefore);

        await expect(pgdb.queryWithOnCursorCallback('SELECT 1/0', null, null, () => false)).rejects.toThrow();
        expect(pgdb.pool.idleCount).toEqual(idleBefore);

        let err = await withTimeout(drain(await pgdb.queryAsStream('SELECT 1/0')));
        expect(err).toBeInstanceOf(Error);
        await new Promise(r => setTimeout(r, 50));
        expect(pgdb.pool.idleCount).toEqual(idleBefore);

        // the pool (max 1) is still fully usable
        expect(await withTimeout(pgdb.queryOneField('SELECT 1'))).toEqual(1);
    });

    it("failed COMMIT ends the dedicated connection", async () => {
        let tx = await pgdb.transactionBegin();
        await tx.query('CREATE TEMP TABLE dedicated_conn_deferred(x int UNIQUE DEFERRABLE INITIALLY DEFERRED)');
        await tx.query('INSERT INTO dedicated_conn_deferred VALUES (1), (1)');
        await expect(tx.transactionCommit()).rejects.toThrow(/duplicate key/);
        expect(tx.isTransactionActive()).toBe(false);
        expect(pgdb.pool.idleCount).toEqual(1);
    });
});

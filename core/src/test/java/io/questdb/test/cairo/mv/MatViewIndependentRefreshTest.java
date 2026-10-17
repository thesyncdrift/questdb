/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MatViewIndependentRefreshTest extends AbstractCairoTest {
    private static volatile CountDownLatch slowEntered;
    private static volatile CountDownLatch slowRelease;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public WalWriter getWalWriter(TableToken token) {
                final CountDownLatch entered = slowEntered;
                if (entered != null && token.getTableName().equals("price_1m")) {
                    return new WalWriter(
                            getConfiguration(), token, getTableSequencerAPI(), getDdlListener(token),
                            getWalDirectoryPolicy(), getWalLocker(), getRecentWriteTracker(), getTelemetryWal()
                    ) {
                        @Override
                        public TableWriter.Row newRow(long timestamp) {
                            // Hold the refresh after it has opened its source snapshot.
                            entered.countDown();
                            try {
                                if (!slowRelease.await(30, TimeUnit.SECONDS)) {
                                    throw new AssertionError("slow refresh was not released");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new AssertionError(e);
                            }
                            return super.newRow(timestamp);
                        }
                    };
                }
                return super.getWalWriter(token);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testFastViewProgressesWhileSiblingRefreshIsBlocked() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE prices (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO prices VALUES (1.0, '2026-01-01T00:00:00.000000Z')");
            execute("CREATE MATERIALIZED VIEW price_1m AS (SELECT ts, sum(price) total FROM prices SAMPLE BY 1m) PARTITION BY DAY");
            execute("CREATE MATERIALIZED VIEW price_1s AS (SELECT ts, sum(price) total FROM prices SAMPLE BY 1s) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            slowRelease = new CountDownLatch(1);
            slowEntered = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (MatViewRefreshJob slowJob = new MatViewRefreshJob(0, engine, 2);
                 MatViewRefreshJob fastJob = new MatViewRefreshJob(1, engine, 2)) {
                execute("INSERT INTO prices VALUES (2.0, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                Thread worker = new Thread(() -> {
                    try {
                        drainMatViewQueue(slowJob);
                    } catch (Throwable th) {
                        failure.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                }, "slow-mv-refresh");
                worker.start();
                try {
                    Assert.assertTrue("slow view did not start", slowEntered.await(10, TimeUnit.SECONDS));
                    // This commit must reach the fast sibling while the slow view still owns its lock.
                    for (int i = 0; i < 20; i++) {
                        execute("INSERT INTO prices VALUES (3.0, '2026-01-01T00:00:02.000000Z')");
                        drainWalQueue();
                        drainMatViewQueue(fastJob);
                        drainWalQueue();
                        assertQuery("SELECT sum(total) total FROM price_1s")
                                .noLeakCheck().noRandomAccess().expectSize().returns("total\n" + (6 + 3 * i) + ".0\n");
                    }
                } finally {
                    slowEntered = null;
                    slowRelease.countDown();
                    worker.join(10_000);
                    Assert.assertFalse("refresh worker did not stop", worker.isAlive());
                }
                if (failure.get() != null) throw new AssertionError(failure.get());
            } finally {
                slowEntered = null;
                slowRelease.countDown();
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT sum(total) total FROM price_1m").noRandomAccess().expectSize().returns("total\n63.0\n");
            assertQuery("SELECT sum(total) total FROM price_1s").noRandomAccess().expectSize().returns("total\n63.0\n");
        });
    }
}

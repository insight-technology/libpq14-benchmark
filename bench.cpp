#include <cstdint>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <utility>
#include <libpq-fe.h>

using namespace std;


class StopWatch {
public:
    StopWatch() {
        this->reset();
    }
    void reset() {
        const auto now = chrono::system_clock::now();
        this->start = now;
        this->lastLap = now;
    }
    int64_t getLapTime() {
        const auto now = chrono::system_clock::now();
        const auto diff = chrono::duration_cast<chrono::milliseconds>(now - this->lastLap).count();
        this->lastLap = now;
        return diff;
    }
    int64_t getTime() {
        const auto now = chrono::system_clock::now();
        return chrono::duration_cast<chrono::milliseconds>(now - this->start).count();
    }
private:
    chrono::system_clock::time_point start;
    chrono::system_clock::time_point lastLap;
};


class DataGenerator {
public:
    DataGenerator(): mt(0), dist(0, 6) {};
    pair<const char*, const char*> fetch() {
        const auto idx = this->dist(this->mt);
        return make_pair(TITLES[idx], CONTENTS[idx]);
    }
    pair<const char*, const char*> fetchEscaped() {
        const auto idx = this->dist(this->mt);
        return make_pair(TITLES[idx], ESCAPED_CONTENTS[idx]);
    }
private:
    // from https://www.postgresql.org/docs/14/libpq-pipeline-mode.html
    const char* TITLES[7] = {
        "34.5. Pipeline Mode",
        "34.5.1. Using Pipeline Mode",
        "34.5.1.1. Issuing Queries",
        "34.5.1.2. Processing Results",
        "34.5.1.3. Error Handling",
        "34.5.1.4. Interleaving Result Processing And Query Dispatch",
        "34.5.3. When to Use Pipeline Mode"
    };
    const char* CONTENTS[7] = {
        "libpq pipeline mode allows applications to send a query without having to read the result of the previously sent query. Taking advantage of the pipeline mode, a client will wait less for the server, since multiple queries/results can be sent/received in a single network transaction.\n\nWhile pipeline mode provides a significant performance boost, writing clients using the pipeline mode is more complex because it involves managing a queue of pending queries and finding which result corresponds to which query in the queue.\n\nPipeline mode also generally consumes more memory on both the client and server, though careful and aggressive management of the send/receive queue can mitigate this. This applies whether or not the connection is in blocking or non-blocking mode.\n\nWhile the pipeline API was introduced in PostgreSQL 14, it is a client-side feature which doesn't require special server support and works on any server that supports the v3 extended query protocol.\n",
        "To issue pipelines, the application must switch the connection into pipeline mode, which is done with PQenterPipelineMode. PQpipelineStatus can be used to test whether pipeline mode is active. In pipeline mode, only asynchronous operations are permitted, command strings containing multiple SQL commands are disallowed, and so is COPY. Using synchronous command execution functions such as PQfn, PQexec, PQexecParams, PQprepare, PQexecPrepared, PQdescribePrepared, PQdescribePortal, is an error condition. Once all dispatched commands have had their results processed, and the end pipeline result has been consumed, the application may return to non-pipelined mode with PQexitPipelineMode.\n",
        "After entering pipeline mode, the application dispatches requests using PQsendQuery, PQsendQueryParams, or its prepared-query sibling PQsendQueryPrepared. These requests are queued on the client-side until flushed to the server; this occurs when PQpipelineSync is used to establish a synchronization point in the pipeline, or when PQflush is called. The functions PQsendPrepare, PQsendDescribePrepared, and PQsendDescribePortal also work in pipeline mode. Result processing is described below.\n\nThe server executes statements, and returns results, in the order the client sends them. The server will begin executing the commands in the pipeline immediately, not waiting for the end of the pipeline. Note that results are buffered on the server side; the server flushes that buffer when a synchronization point is established with PQpipelineSync, or when PQsendFlushRequest is called. If any statement encounters an error, the server aborts the current transaction and does not execute any subsequent command in the queue until the next synchronization point; a PGRES_PIPELINE_ABORTED result is produced for each such command. (This remains true even if the commands in the pipeline would rollback the transaction.) Query processing resumes after the synchronization point.\n\nIt's fine for one operation to depend on the results of a prior one; for example, one query may define a table that the next query in the same pipeline uses. Similarly, an application may create a named prepared statement and execute it with later statements in the same pipeline.\n",
        "To process the result of one query in a pipeline, the application calls PQgetResult repeatedly and handles each result until PQgetResult returns null. The result from the next query in the pipeline may then be retrieved using PQgetResult again and the cycle repeated. The application handles individual statement results as normal. When the results of all the queries in the pipeline have been returned, PQgetResult returns a result containing the status value PGRES_PIPELINE_SYNC\n\nThe client may choose to defer result processing until the complete pipeline has been sent, or interleave that with sending further queries in the pipeline; see Section 34.5.1.4.\n\nTo enter single-row mode, call PQsetSingleRowMode before retrieving results with PQgetResult. This mode selection is effective only for the query currently being processed. For more information on the use of PQsetSingleRowMode, refer to Section 34.6.\n\nPQgetResult behaves the same as for normal asynchronous processing except that it may contain the new PGresult types PGRES_PIPELINE_SYNC and PGRES_PIPELINE_ABORTED. PGRES_PIPELINE_SYNC is reported exactly once for each PQpipelineSync at the corresponding point in the pipeline. PGRES_PIPELINE_ABORTED is emitted in place of a normal query result for the first error and all subsequent results until the next PGRES_PIPELINE_SYNC; see Section 34.5.1.3.\n\nPQisBusy, PQconsumeInput, etc operate as normal when processing pipeline results. In particular, a call to PQisBusy in the middle of a pipeline returns 0 if the results for all the queries issued so far have been consumed.\n\nlibpq does not provide any information to the application about the query currently being processed (except that PQgetResult returns null to indicate that we start returning the results of next query). The application must keep track of the order in which it sent queries, to associate them with their corresponding results. Applications will typically use a state machine or a FIFO queue for this.\n",
        "From the client's perspective, after PQresultStatus returns PGRES_FATAL_ERROR, the pipeline is flagged as aborted. PQresultStatus will report a PGRES_PIPELINE_ABORTED result for each remaining queued operation in an aborted pipeline. The result for PQpipelineSync is reported as PGRES_PIPELINE_SYNC to signal the end of the aborted pipeline and resumption of normal result processing.\n\nThe client must process results with PQgetResult during error recovery.\n\nIf the pipeline used an implicit transaction, then operations that have already executed are rolled back and operations that were queued to follow the failed operation are skipped entirely. The same behavior holds if the pipeline starts and commits a single explicit transaction (i.e. the first statement is BEGIN and the last is COMMIT) except that the session remains in an aborted transaction state at the end of the pipeline. If a pipeline contains multiple explicit transactions, all transactions that committed prior to the error remain committed, the currently in-progress transaction is aborted, and all subsequent operations are skipped completely, including subsequent transactions. If a pipeline synchronization point occurs with an explicit transaction block in aborted state, the next pipeline will become aborted immediately unless the next command puts the transaction in normal mode with ROLLBACK.\n",
        "To avoid deadlocks on large pipelines the client should be structured around a non-blocking event loop using operating system facilities such as select, poll, WaitForMultipleObjectEx, etc.\n\nThe client application should generally maintain a queue of work remaining to be dispatched and a queue of work that has been dispatched but not yet had its results processed. When the socket is writable it should dispatch more work. When the socket is readable it should read results and process them, matching them up to the next entry in its corresponding results queue. Based on available memory, results from the socket should be read frequently: there's no need to wait until the pipeline end to read the results. Pipelines should be scoped to logical units of work, usually (but not necessarily) one transaction per pipeline. There's no need to exit pipeline mode and re-enter it between pipelines, or to wait for one pipeline to finish before sending the next.\n\nAn example using select() and a simple state machine to track sent and received work is in src/test/modules/libpq_pipeline/libpq_pipeline.c in the PostgreSQL source distribution.\n",
        "Much like asynchronous query mode, there is no meaningful performance overhead when using pipeline mode. It increases client application complexity, and extra caution is required to prevent client/server deadlocks, but pipeline mode can offer considerable performance improvements, in exchange for increased memory usage from leaving state around longer.\n\nPipeline mode is most useful when the server is distant, i.e., network latency (“ping time”) is high, and also when many small operations are being performed in rapid succession. There is usually less benefit in using pipelined commands when each query takes many multiples of the client/server round-trip time to execute. A 100-statement operation run on a server 300 ms round-trip-time away would take 30 seconds in network latency alone without pipelining; with pipelining it may spend as little as 0.3 s waiting for results from the server.\n\nUse pipelined commands when your application does lots of small INSERT, UPDATE and DELETE operations that can't easily be transformed into operations on sets, or into a COPY operation.\n\nPipeline mode is not useful when information from one operation is required by the client to produce the next operation. In such cases, the client would have to introduce a synchronization point and wait for a full client/server round-trip to get the results it needs. However, it's often possible to adjust the client design to exchange the required information server-side. Read-modify-write cycles are especially good candidates; for example:\n",
    };

    const char* ESCAPED_CONTENTS[7] = {
        "libpq pipeline mode allows applications to send a query without having to read the result of the previously sent query. Taking advantage of the pipeline mode, a client will wait less for the server, since multiple queries/results can be sent/received in a single network transaction.\n\nWhile pipeline mode provides a significant performance boost, writing clients using the pipeline mode is more complex because it involves managing a queue of pending queries and finding which result corresponds to which query in the queue.\n\nPipeline mode also generally consumes more memory on both the client and server, though careful and aggressive management of the send/receive queue can mitigate this. This applies whether or not the connection is in blocking or non-blocking mode.\n\nWhile the pipeline API was introduced in PostgreSQL 14, it is a client-side feature which doesn''t require special server support and works on any server that supports the v3 extended query protocol.\n",
        "To issue pipelines, the application must switch the connection into pipeline mode, which is done with PQenterPipelineMode. PQpipelineStatus can be used to test whether pipeline mode is active. In pipeline mode, only asynchronous operations are permitted, command strings containing multiple SQL commands are disallowed, and so is COPY. Using synchronous command execution functions such as PQfn, PQexec, PQexecParams, PQprepare, PQexecPrepared, PQdescribePrepared, PQdescribePortal, is an error condition. Once all dispatched commands have had their results processed, and the end pipeline result has been consumed, the application may return to non-pipelined mode with PQexitPipelineMode.\n",
        "After entering pipeline mode, the application dispatches requests using PQsendQuery, PQsendQueryParams, or its prepared-query sibling PQsendQueryPrepared. These requests are queued on the client-side until flushed to the server; this occurs when PQpipelineSync is used to establish a synchronization point in the pipeline, or when PQflush is called. The functions PQsendPrepare, PQsendDescribePrepared, and PQsendDescribePortal also work in pipeline mode. Result processing is described below.\n\nThe server executes statements, and returns results, in the order the client sends them. The server will begin executing the commands in the pipeline immediately, not waiting for the end of the pipeline. Note that results are buffered on the server side; the server flushes that buffer when a synchronization point is established with PQpipelineSync, or when PQsendFlushRequest is called. If any statement encounters an error, the server aborts the current transaction and does not execute any subsequent command in the queue until the next synchronization point; a PGRES_PIPELINE_ABORTED result is produced for each such command. (This remains true even if the commands in the pipeline would rollback the transaction.) Query processing resumes after the synchronization point.\n\nIt''s fine for one operation to depend on the results of a prior one; for example, one query may define a table that the next query in the same pipeline uses. Similarly, an application may create a named prepared statement and execute it with later statements in the same pipeline.\n",
        "To process the result of one query in a pipeline, the application calls PQgetResult repeatedly and handles each result until PQgetResult returns null. The result from the next query in the pipeline may then be retrieved using PQgetResult again and the cycle repeated. The application handles individual statement results as normal. When the results of all the queries in the pipeline have been returned, PQgetResult returns a result containing the status value PGRES_PIPELINE_SYNC\n\nThe client may choose to defer result processing until the complete pipeline has been sent, or interleave that with sending further queries in the pipeline; see Section 34.5.1.4.\n\nTo enter single-row mode, call PQsetSingleRowMode before retrieving results with PQgetResult. This mode selection is effective only for the query currently being processed. For more information on the use of PQsetSingleRowMode, refer to Section 34.6.\n\nPQgetResult behaves the same as for normal asynchronous processing except that it may contain the new PGresult types PGRES_PIPELINE_SYNC and PGRES_PIPELINE_ABORTED. PGRES_PIPELINE_SYNC is reported exactly once for each PQpipelineSync at the corresponding point in the pipeline. PGRES_PIPELINE_ABORTED is emitted in place of a normal query result for the first error and all subsequent results until the next PGRES_PIPELINE_SYNC; see Section 34.5.1.3.\n\nPQisBusy, PQconsumeInput, etc operate as normal when processing pipeline results. In particular, a call to PQisBusy in the middle of a pipeline returns 0 if the results for all the queries issued so far have been consumed.\n\nlibpq does not provide any information to the application about the query currently being processed (except that PQgetResult returns null to indicate that we start returning the results of next query). The application must keep track of the order in which it sent queries, to associate them with their corresponding results. Applications will typically use a state machine or a FIFO queue for this.\n",
        "From the client''s perspective, after PQresultStatus returns PGRES_FATAL_ERROR, the pipeline is flagged as aborted. PQresultStatus will report a PGRES_PIPELINE_ABORTED result for each remaining queued operation in an aborted pipeline. The result for PQpipelineSync is reported as PGRES_PIPELINE_SYNC to signal the end of the aborted pipeline and resumption of normal result processing.\n\nThe client must process results with PQgetResult during error recovery.\n\nIf the pipeline used an implicit transaction, then operations that have already executed are rolled back and operations that were queued to follow the failed operation are skipped entirely. The same behavior holds if the pipeline starts and commits a single explicit transaction (i.e. the first statement is BEGIN and the last is COMMIT) except that the session remains in an aborted transaction state at the end of the pipeline. If a pipeline contains multiple explicit transactions, all transactions that committed prior to the error remain committed, the currently in-progress transaction is aborted, and all subsequent operations are skipped completely, including subsequent transactions. If a pipeline synchronization point occurs with an explicit transaction block in aborted state, the next pipeline will become aborted immediately unless the next command puts the transaction in normal mode with ROLLBACK.\n",
        "To avoid deadlocks on large pipelines the client should be structured around a non-blocking event loop using operating system facilities such as select, poll, WaitForMultipleObjectEx, etc.\n\nThe client application should generally maintain a queue of work remaining to be dispatched and a queue of work that has been dispatched but not yet had its results processed. When the socket is writable it should dispatch more work. When the socket is readable it should read results and process them, matching them up to the next entry in its corresponding results queue. Based on available memory, results from the socket should be read frequently: there''s no need to wait until the pipeline end to read the results. Pipelines should be scoped to logical units of work, usually (but not necessarily) one transaction per pipeline. There''s no need to exit pipeline mode and re-enter it between pipelines, or to wait for one pipeline to finish before sending the next.\n\nAn example using select() and a simple state machine to track sent and received work is in src/test/modules/libpq_pipeline/libpq_pipeline.c in the PostgreSQL source distribution.\n",
        "Much like asynchronous query mode, there is no meaningful performance overhead when using pipeline mode. It increases client application complexity, and extra caution is required to prevent client/server deadlocks, but pipeline mode can offer considerable performance improvements, in exchange for increased memory usage from leaving state around longer.\n\nPipeline mode is most useful when the server is distant, i.e., network latency (“ping time”) is high, and also when many small operations are being performed in rapid succession. There is usually less benefit in using pipelined commands when each query takes many multiples of the client/server round-trip time to execute. A 100-statement operation run on a server 300 ms round-trip-time away would take 30 seconds in network latency alone without pipelining; with pipelining it may spend as little as 0.3 s waiting for results from the server.\n\nUse pipelined commands when your application does lots of small INSERT, UPDATE and DELETE operations that can''t easily be transformed into operations on sets, or into a COPY operation.\n\nPipeline mode is not useful when information from one operation is required by the client to produce the next operation. In such cases, the client would have to introduce a synchronization point and wait for a full client/server round-trip to get the results it needs. However, it''s often possible to adjust the client design to exchange the required information server-side. Read-modify-write cycles are especially good candidates; for example:\n",
    };

    std::mt19937 mt;
    std::uniform_int_distribution<int> dist;
};



void myExit(PGconn* cn) {
    cerr << PQerrorMessage(cn) << endl;
    exit(1);
}

void myExit(PGresult* res) {
    cerr << PQresultErrorMessage(res) << endl;
    exit(1);
}


void prepare(PGconn* cn) {
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);

    res.reset(PQexec(cn, "DROP TABLE IF EXISTS insert_performance_test"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    res.reset(PQexec(cn, "CREATE TABLE insert_performance_test (id SERIAL PRIMARY KEY, title VARCHAR(100), content TEXT, created_at timestamp DEFAULT CURRENT_TIMESTAMP)"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}

void verify(PGconn* cn, int num) {
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);

    res.reset(PQexec(cn, "SELECT COUNT(*) FROM insert_performance_test"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_TUPLES_OK) {
        myExit(res.get());
    }

    const char* cnt = PQgetvalue(res.get(), 0, 0);
    if (atoi(cnt) != num) {
        cout << "miss match number of rows. expected=" << num << " actual=" << cnt << endl;
    }
}

void wrap(const function<void(PGconn* cn, int num)>& f, const std::string& name, PGconn* cn, int num) {
    prepare(cn);

    cout << name << " start" << endl;
    StopWatch sw;

    f(cn, num);

    cout << name << " finish " << sw.getTime() << "ms" << endl << endl;

    verify(cn, num);
}


void simple(PGconn* cn, int num) {  // insecure
    DataGenerator dg;
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);

    res.reset(PQexec(cn, "BEGIN"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    for (int i=0; i<num; ++i) {
        const auto d = dg.fetchEscaped();
        const string query = string("INSERT INTO insert_performance_test (title, content) values('") + d.first + "', '" + d.second + "')";
        res.reset(PQexec(cn, query.c_str()));
        if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
            myExit(res.get());
        }
    }

    res.reset(PQexec(cn, "COMMIT"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}


void prepared(PGconn* cn, int num) {
    DataGenerator dg;
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);

    res.reset(PQprepare(cn, "test", "INSERT INTO insert_performance_test (title, content) values($1, $2)", 2, NULL));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    res.reset(PQexec(cn, "BEGIN"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    for (int i=0; i<num; ++i) {
        const auto d = dg.fetch();
        const char* params[2] = {d.first, d.second};
        res.reset(PQexecPrepared(cn, "test", 2, params, NULL, NULL, 0));
        if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
            myExit(res.get());
        }
    }

    res.reset(PQexec(cn, "COMMIT"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    res.reset(PQexec(cn, "DEALLOCATE \"test\""));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}

void pipeline(PGconn* cn, int num) {  // insecure
    StopWatch sw;
    DataGenerator dg;

    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);


    if (PQsetnonblocking(cn, 1) != 0) {
        myExit(cn);
    }
    if (PQenterPipelineMode(cn) != 1) {
        myExit(cn);
    }


    cout << "  start insert query " << sw.getLapTime() << " ms" << endl;
    for (int i=0; i<num; ++i) {
        const auto d = dg.fetchEscaped();
        const string query = string("INSERT INTO insert_performance_test (title, content) values('") + d.first + "', '" + d.second + "')";
        if (PQsendQuery(cn, query.c_str()) != 1) {
            myExit(cn);
        }
    }
    cout << "  finish insert query " << sw.getLapTime() << " ms" << endl;


    if (PQpipelineSync(cn) != 1) {
        myExit(cn);
    }
    cout << "  marks a synchronization point " << sw.getLapTime() << " ms" << endl;


    for (int i=0; i<num; ++i) {
        res.reset(PQgetResult(cn));
        if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
            myExit(res.get());
        }

        res.reset(PQgetResult(cn));
    }


    res.reset(PQgetResult(cn));
    if (PQresultStatus(res.get()) != PGRES_PIPELINE_SYNC) {
        myExit(res.get());
    }
    res.reset(PQgetResult(cn));
    cout << "  finish consuming results " << sw.getLapTime() << " ms" << endl;


    if (PQexitPipelineMode(cn) != 1) {
        myExit(cn);
    }
    if (PQsetnonblocking(cn, 0) != 0) {
        myExit(cn);
    }
}


void pipelineprepared(PGconn* cn, int num) {
    StopWatch sw;
    DataGenerator dg;

    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);


    // 非同期モードに入ってからPQsendPrepareでも可。その場合PQgetResultの回数も増やす
    res.reset(PQprepare(cn, "test", "INSERT INTO insert_performance_test (title, content) values($1, $2)", 2, NULL));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    if (PQsetnonblocking(cn, 1) != 0) {
        myExit(cn);
    }
    if (PQenterPipelineMode(cn) != 1) {
        myExit(cn);
    }

    cout << "  start insert query " << sw.getLapTime() << " ms" << endl;
    for (int i=0; i<num; ++i) {
        const auto d = dg.fetch();
        const char* params[2] = {d.first, d.second};
        if (PQsendQueryPrepared(cn, "test", 2, params, NULL, NULL, 0) != 1) {
            myExit(cn);
        }
    }
    cout << "  finish insert query " << sw.getLapTime() << " ms" << endl;


    if (PQpipelineSync(cn) != 1) {
        myExit(cn);
    }
    cout << "  marks a synchronization point " << sw.getLapTime() << " ms" << endl;


    for (int i=0; i<num; ++i) {
        res.reset(PQgetResult(cn));
        if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
            myExit(res.get());
        }

        res.reset(PQgetResult(cn));
    }


    res.reset(PQgetResult(cn));
    if (PQresultStatus(res.get()) != PGRES_PIPELINE_SYNC) {
        myExit(res.get());
    }
    res.reset(PQgetResult(cn));
    cout << "  finish consuming results " << sw.getLapTime() << " ms" << endl;


    if (PQexitPipelineMode(cn) != 1) {
        myExit(cn);
    }
    if (PQsetnonblocking(cn, 0) != 0) {
        myExit(cn);
    }

    res.reset(PQexec(cn, "DEALLOCATE \"test\""));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}


void bulk(PGconn* cn, int num) {  // insecure
    DataGenerator dg;
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);

    const int BULK_SIZE = 1000;

    res.reset(PQexec(cn, "BEGIN"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }

    for (int i=0; i<num; i+=BULK_SIZE) {
        int bulkSize = i + BULK_SIZE > num ? num - i : BULK_SIZE;
        
        stringstream ss;
        ss << "INSERT INTO insert_performance_test (title, content) values";
        for (int j=0; j<bulkSize; ++j) {
            const auto d = dg.fetchEscaped();    
            ss << "('" << d.first << "','" << d.second << "')";
            if (j != bulkSize - 1) {
                ss << ",";
            }
        }

        res.reset(PQexec(cn, ss.str().c_str()));
        if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
            myExit(res.get());
        }
    }

    res.reset(PQexec(cn, "COMMIT"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}


void copystdin(PGconn* cn, int num) {
    unique_ptr<PGresult, decltype(&PQclear)> res(nullptr, PQclear);
    DataGenerator dg;

    res.reset(PQexec(cn, "COPY insert_performance_test (title, content) FROM STDIN WITH CSV DELIMITER ',' QUOTE '\"'"));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COPY_IN) {
        myExit(res.get());
    }

    for (int i=0; i<num; ++i) {
        const auto d = dg.fetch();
        const string buf = string("\"") + d.first + "\",\"" + d.second + "\"\n";
        if (PQputCopyData(cn, buf.c_str(), buf.length()) != 1) {
            myExit(cn);
        }
    }
    if (PQputCopyEnd(cn, NULL) != 1) {
        myExit(cn);
    }
    res.reset(PQgetResult(cn));
    if (PQresultStatus(res.get()) != ExecStatusType::PGRES_COMMAND_OK) {
        myExit(res.get());
    }
}


int main(int argc, char** argv) {
    const char* conninfo = argc > 1 ? argv[1] : "";
    const int num = argc > 2 ? atoi(argv[2]) : 1000 * 1000;
    const string name = argc > 3 ? argv[3] : "";

    cout << "conninfo: " << conninfo << endl
         << "num rows: " << num << endl
         << endl;

    unique_ptr<PGconn, decltype(&PQfinish)> cn(nullptr, PQfinish);

    cn.reset(PQconnectdb(conninfo));
    if (PQstatus(cn.get()) != CONNECTION_OK) {
        cerr << PQerrorMessage(cn.get()) << endl;
        exit(1);
    }

    map<string, function<void(PGconn*, int)>> funcs;
    funcs["simple"] = simple;
    funcs["prepared"] = prepared;
    funcs["pipeline"] = pipeline;
    funcs["pipelineprepared"] = pipelineprepared;
    funcs["bulk"] = bulk;
    funcs["copystdin"] = copystdin;

    if (name == "") {
        for (const auto& elm : funcs) {
            wrap(elm.second, elm.first, cn.get(), num);
        }
    } else {
        wrap(funcs[name], name, cn.get(), num);
    }

    return 0;
}

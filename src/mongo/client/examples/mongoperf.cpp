/* 
   How to build and run:

   scons mongoperf
   ./mongoperf -h
*/

#define MONGO_EXPOSE_MACROS 1

#include <iostream>
#include <conio.h>
#include "../dbclient.h" // the mongo c++ driver
#include "../../util/mmap.h"
#include <assert.h>
#include "../../util/logfile.h"
#include "../../util/timer.h"
#include "../../util/time_support.h"
#include "../../bson/util/atomic_int.h"

#include <boost/filesystem/operations.hpp>

using namespace std;
using namespace mongo;
using namespace bson;

int dummy;
LogFile *lf = 0;
MemoryMappedFile *mmfFile;
char *mmf = 0;
bo options;
unsigned long long len; // file len
const unsigned PG = 4096;
unsigned nThreadsRunning = 0;
const unsigned long long GB = 1024ULL*1024*1024;
const unsigned MB = 1024 * 1024;
bool shuttingDown;


// as this is incremented A LOT, at some point this becomes a bottleneck if very high ops/second (in cache) things are happening.
AtomicUInt readOps;
AtomicUInt writeOps;

unsigned long long totReadOps;
unsigned long long totWriteOps;

SimpleMutex m("mperf");

int syncDelaySecs = 0;

void syncThread() {
    while( 1 ) {
        mongo::Timer t;
        mmfFile->flush(true);
        cout << "                                                     mmf sync took " << t.millis() << "ms" << endl;
        sleepsecs(syncDelaySecs);
    }
}

char* round(char* x) {
    size_t f = (size_t) x;
    char *p = (char *) ((f+PG-1)/PG*PG);
    return p;
}

struct Aligned {
    char x[8192];
    char* addr() { return round(x); }
}; 

unsigned long long rrand() { 
    // RAND_MAX is very small on windows
    return (static_cast<unsigned long long>(rand()) << 15) ^ rand();
}

int threads;

void workerThread() {
    bool r = options["r"].trueValue();
    bool w = options["w"].trueValue();
    //cout << "read:" << r << " write:" << w << endl;
    long long su = options["sleepMicros"].numberLong();
    Aligned a;
    while( !shuttingDown ) { 
        unsigned long long rofs = (rrand() * PG) % len;
        unsigned long long wofs = (rrand() * PG) % len;
        if( mmf ) { 
            if( r ) {
                dummy += mmf[rofs];
                readOps++;
            }
            if( w ) {
                mmf[wofs] = 3;
                writeOps++;
            }
        }
        else {
            if( r ) {
                lf->readAt(rofs, a.addr(), PG);
                readOps++;
            }
            if( w ) {
                lf->writeAt(wofs, a.addr(), PG);
                writeOps++;
            }
        }
        long long micros = su / nThreadsRunning;
        if( micros ) {
            sleepmicros(micros);
        }
    }
    threads--;
}

void go() {
    assert( options["r"].trueValue() || options["w"].trueValue() );
    MemoryMappedFile f;
    cout << "test file size:";
    len = options["fileSizeMB"].numberLong();
    if( len == 0 ) len = 1;
    cout << len << "MB ..." << endl;

    if( 0 && len > 2000 && !options["mmf"].trueValue() ) { 
        // todo make tests use 64 bit offsets in their i/o -- i.e. adjust LogFile::writeAt and such
        cout << "\nsizes > 2GB not yet supported with mmf:false" << endl; 
        return;
    }
    len *= MB;
    const char *fname = "./mongoperf__testfile__tmp";
  
    union {
        unsigned long long size;
        struct {
            DWORD lo;
            DWORD hi;
        };
    };
    size = 0;

#ifdef _WIN32
    ::HANDLE h = CreateFileA(fname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
    if (h) {
        lo = GetFileSize(h, &hi);
        CloseHandle(h);
        if (len > size) {
            size = 0;
            DeleteFileA(fname);
        }
    }
#else
    struct stat s;
    if (!stat(fname, &s)) {
        size = s.st_size;
        if (len > size) {
            size = 0;
            unlink(fname);
        }
    }
#endif

    const unsigned sz = 32 * MB; // needs to be big as we are using synchronousAppend.  if we used a regular MongoFile it wouldn't have to be
    char *buf = (char*) malloc(sz+4096);
    char *p = round(buf);
    lf = new LogFile(fname, true);
    if (size == 0) {
        cout << "creating ...\n";
        for( unsigned long long i = 0; i < len; i += sz ) { 
            lf->synchronousAppend(p, sz);
            if( i % GB == 0 && i ) {
                cout << i / GB << "GB..." << endl;
            }
        }
    }
/*
    char* str1 = "Just some string";
    strcpy(p, str1);
    lf->writeAt(1 * GB, p, PG);
    char* str2 = "This is a string";
    strcpy(p, str2);
    lf->writeAt(5 * GB, p, PG);
    *p = 0;
    lf->readAt(5 * GB, p, PG);
    if (strcmp(str2, p) != 0)
        cout << "bad 64-bit read\n";
    lf->readAt(1 * GB, p, PG);
    if (strcmp(p, str1) != 0)
        cout << "bad 64-bit write\n";
*/

    BSONObj& o = options;

    if( o["mmf"].trueValue() ) { 
        delete lf;
        lf = 0;
        mmfFile = new MemoryMappedFile();
        mmf = (char *) mmfFile->map(fname);
        assert( mmf );

        syncDelaySecs = options["syncDelay"].numberInt();
        if( syncDelaySecs ) {
            boost::thread t(syncThread);
        }
    }

    cout << "testing..."<< endl;

    unsigned wthr = (unsigned) o["nThreads"].numberInt();
    if (wthr == 0) wthr = 1;
    unsigned i = 0;
    unsigned d = 1;
    unsigned &nthr = nThreadsRunning;
    unsigned long long totalOps = 0;
    Timer t;
    while( i == 0 || threads > 0) {
        if( i++ % 8 == 0 ) {
            if( nthr < wthr ) {
                while( nthr < wthr && nthr < d ) {
                    nthr++;
                    boost::thread w(workerThread);
                    threads++;
                }
                cout << "new thread, total running : " << nthr << endl;
                d *= 2;
            }
        }
        sleepsecs(1);
        unsigned long long r = readOps.get();
        readOps.zero();
        totReadOps += r;
        unsigned long long w = writeOps.get();
        writeOps.zero();
        totWriteOps += w;
        w += r;
        // w /= 1; // 1 secs
        totalOps += w;
        cout << w << " ops/sec ";
        if( mmf == 0 ) 
            // only writing 4 bytes with mmf so we don't say this
            cout << (w * PG / MB) << " MB/sec";
        cout << endl;
    }
    cout << "read ops     = " << totReadOps << "\n" <<
            "write ops    = " << totWriteOps << endl <<
            "total ops    = " << totalOps << "\n" <<
            "total time   = " << t.seconds() << " seconds\n";
    if (mmf == 0)
        cout << "total MB/sec = " << (totalOps * PG / MB);

 }
            

char* validOptions[] = {
    "nThreads", "fileSizeMB", "sleepMicros", "mmf", "r", "w", "syncDelay", 0
};


#ifdef _WIN32
BOOL CtrlHandler(DWORD fdwCtrlType) {
    shuttingDown = true;
    return TRUE;
}

#else
sigset_t asyncSignals;
// The above signals will be processed by this thread only, in order to
// ensure the db and log mutexes aren't held.
void interruptThread() {
    int x;
    sigwait( &asyncSignals, &x );
    shuttingDown = true;
}


#endif


int runner(int argc, char *argv[]) {
    // try
    {
#ifdef _WIN32
        SetConsoleCtrlHandler((PHANDLER_ROUTINE) CtrlHandler, TRUE);
#else
        sigemptyset( &asyncSignals );
        sigaddset( &asyncSignals, SIGHUP );
        sigaddset( &asyncSignals, SIGINT );
        sigaddset( &asyncSignals, SIGTERM );
        assert( pthread_sigmask( SIG_SETMASK, &asyncSignals, 0 ) == 0 );
        boost::thread it( interruptThread );
#endif
        cout << "mongoperf" << endl;

        const char* fname = "stdin";
        std::istream *in = &cin;
        if( argc > 1 ) {
            fname = argv[1];
            if (strcmp(fname, "-h") == 0) {
                cout <<
                    "\n"
                    "usage:\n"
                    "\n"
                    "  mongoperf < myjsonconfigfile\n"
                    "  mongoperf myjsconfigfile\n"
                    "  mongoperf -h"
                    "\n"
                    "  {\n"
                    "    nThreads:<n>,     // number of threads (default 1)\n"
                    "    fileSizeMB:<n>,   // test file size (default 1MB)\n"
                    "    sleepMicros:<n>,  // pause for sleepMicros/nThreads between each operation (default 0)\n"
                    "    mmf:<bool>,       // if true do i/o's via memory mapped files (default false)\n"
                    "    r:<bool>,         // do reads (default false)\n"
                    "    w:<bool>,         // do writes (default false)\n"
                    "    syncDelay:<n>     // secs between fsyncs, like --syncdelay in mongod. (default 0/never)\n"
                    "  }\n"
                    "\n"
                    "mongoperf is a performance testing tool. the initial tests are of disk subsystem performance; \n"
                    "  tests of mongos and mongod will be added later.\n"
                    "most fields are optional.\n"
                    "non-mmf io is direct io (no caching). use a large file size to test making the heads\n"
                    "  move significantly and to avoid i/o coalescing\n"
                    "mmf io uses caching (the file system cache).\n"
                    "\n"
                    << endl;
                return 0;
            }
            in = new ifstream(fname, ios_base::in);
            if (in->bad()) {
                cout << "Error: Unable to open " << argv[1];
                delete in;
                return 1;
            }
        }

        cout << "use -h for help" << endl;
        cout << "reading from " << fname << "...\n\n";
        char input[1024];
        memset(input, 0, sizeof(input));
        in->read(input, 1000);
        if( *input == 0 ) { 
            cout << "Error: No options found reading " << fname << endl;
            return 2;
        }

        string s = input;
        str::stripTrailing(s, "\n\r\0x1a");
        try { 
            options = fromjson(s);
        }
        catch(...) { 
            cout << s << endl;
            cout << "couldn't parse json options" << endl;
            return -1;
        }
        cout << "options:\n" << options.toString() << "\n\n" << endl;

        bool err = false;
        for (BSONObjIterator i = options.begin(); i.more(); ) {
            const char* name = i.next().fieldName();
            bool found = false;
            for (char** s = validOptions; !found && *s; s++)
                found = strcmp(*s, name) == 0;
            if (!found) {
                err = true;
                cout << "Error: Unrecognized field name (" << name << ") in the options.";
            }
        }
        if (err) return -1;

        go();

        if (in != &cin)
            delete in;
#if 0
        cout << "connecting to localhost..." << endl;
        DBClientConnection c;
        c.connect("localhost");
        cout << "connected ok" << endl;
        unsigned long long count = c.count("test.foo");
        cout << "count of exiting documents in collection test.foo : " << count << endl;

        bo o = BSON( "hello" << "world" );
        c.insert("test.foo", o);

        string e = c.getLastError();
        if( !e.empty() ) { 
            cout << "insert #1 failed: " << e << endl;
        }

        // make an index with a unique key constraint
        c.ensureIndex("test.foo", BSON("hello"<<1), /*unique*/true);

        c.insert("test.foo", o); // will cause a dup key error on "hello" field
        cout << "we expect a dup key error here:" << endl;
        cout << "  " << c.getLastErrorDetailed().toString() << endl;
#endif
    } 
/*
    catch(DBException& e) { 
        cout << "caught DBException " << e.toString() << endl;
        return 1;
    }
*/
    return 0;
}


int main(int argc, char *argv[]) {
    int ret = runner(argc, argv);
#ifdef _WIN32
    if (IsDebuggerPresent())
        _getch();
#endif
    return ret;
}
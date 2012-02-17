/* 
   How to build and run:

   scons mongoperf
   ./mongoperf -h
*/

#define MONGO_EXPOSE_MACROS 1

#include <iostream>
#include <conio.h>
#include <ctime>
#include "../dbclient.h" // the mongo c++ driver
#include "../../util/mmap.h"
#include <assert.h>
#include "../../util/logfile.h"
#include "../../util/timer.h"
#include "../../util/time_support.h"
#include "../../bson/util/atomic_int.h"

#include <boost/filesystem/operations.hpp>

#include "Rand.h"

using namespace std;
using namespace mongo;
using namespace bson;

const char *workname = "./mongoperf__testfile__tmp";
int dummy;
LogFile *lf = 0;
MemoryMappedFile *mmfFile;
char *mmf = 0;
unsigned long long len; // file len
const unsigned PG = 4096;
unsigned opSize = PG;
unsigned nThreadsRunning = 0;
const unsigned long long GB = 1024ULL*1024*1024;
const unsigned MB = 1024 * 1024;
bool shuttingDown;
bool random;
bool append;
bool r;
bool w;

bo options;


inline string format_time_t(time_t t = time(0) ) {
        char buf[64];
#if defined(_WIN32)
        ctime_s(buf, sizeof(buf), &t);
#else
        ctime_r(&t, buf);
#endif
        buf[19] = 0; // don't want the year
        return buf;
}

bool isNumber(const char* fieldname) {
    switch (options[fieldname].type()) {
        case NumberDouble: ;
        case NumberInt: ;
        case NumberLong:
        case EOO :
            return true;
        default:
            cout << "Error: expected a number for " << fieldname << "\n";
            return false;
    }
}


// as this is incremented A LOT, at some point this becomes a bottleneck if very high ops/second (in cache) things are happening.
AtomicUInt readOps;
AtomicUInt writeOps;
AtomicUInt threads;

unsigned long long totReadOps;
unsigned long long totWriteOps;

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


void randBuf(RandomGenerator* rnd, char* buf, int size)
{
    long x;
    int n = 0;
    for (int i = 0; i < size; i++) {
        if (n == 0)
            x = rnd->next();
        char c = (char)((x >> (n * 8)) & 0xFF);
        buf[i] = c;
        n = (n+1) % 3;
    }
}

void workerThread() {
    RandomGenerator* rnd = 0;

    if (w) rnd = new RandomGenerator(rand());

    char* buf = new char[opSize + PG];
    if (!buf) {
        cout << "Error: out of memory allocating buffers.  Try a smaller opSize\n";
        exit(1);
    }
    char* q = round(buf);
    //cout << "read:" << r << " write:" << w << endl;
    long long su = options["sleepMicros"].numberLong();
    long long micros = su / nThreadsRunning;
    if (append) {
        while (!shuttingDown) { 
            if (random)
                randBuf(rnd, q, opSize);
            lf->synchronousAppend(q, opSize);
            writeOps++;
            if (micros) sleepmicros(micros);
       }
    }
    else if (mmf) {
        while (!shuttingDown) { 
            if (r) {
                unsigned long long rofs = (rrand() * PG) % len;
                memcpy(q, &mmf[rofs], opSize);
                readOps++;
            }
            if (w) {
                unsigned long long wofs = (rrand() * PG) % len;
                if (random)
                    randBuf(rnd, mmf + wofs, opSize);
                else
                    memcpy(mmf + wofs, q, opSize);
                writeOps++;
            }
            if (micros) sleepmicros(micros);
       }
    }
    else {
        while (!shuttingDown) { 
            if (r) {
                unsigned long long rofs = (rrand() * PG) % len;
                lf->readAt(rofs, q, opSize);
                readOps++;
            }
            if( w ) {
                unsigned long long wofs = (rrand() * PG) % len;
                if (random)
                    randBuf(rnd, q, opSize);
                lf->writeAt(wofs, q, opSize);
                writeOps++;
            }
            if (micros) sleepmicros(micros);
        }
    }
    delete buf;
    delete rnd;
    threads--;
}

void go() {
    MemoryMappedFile f;

    len *= MB;
  
    union {
        unsigned long long size;
        struct {
            DWORD lo;
            DWORD hi;
        };
    };
    size = 0;

#ifdef _WIN32
    ::HANDLE h = CreateFileA(workname, GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
    if (h != (void*)-1) {
        lo = GetFileSize(h, &hi);
        CloseHandle(h);
        if (len > size) {
            size = 0;
            DeleteFileA(workname);
        }
    }
#else
    struct stat s;
    if (!stat(workname, &s)) {
        size = s.st_size;
        if (len > size) {
            size = 0;
            unlink(workname);
        }
    }
#endif
    
    if (append && size) {
#ifdef _WIN32
        DeleteFileA(workname);
#else
        unlink(workname);
#endif
    }
    lf = new LogFile(workname, true);
    if (size == 0 && !append) {
        cout << "creating ...\n";
        const unsigned sz = 32 * MB; // needs to be big as we are using synchronousAppend.  if we used a regular MongoFile it wouldn't have to be
        char *buf = (char*) malloc(sz+4096);
        char *p = round(buf);
        for( unsigned long long i = 0; i < len; i += sz ) { 
            lf->synchronousAppend(p, sz);
            if( i % GB == 0 && i ) {
                cout << i / GB << "GB..." << endl;
            }
        }
    }

    len -= opSize; // don't allow IO past EOF
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


    if( options["mmf"].trueValue() ) { 
        delete lf;
        lf = 0;
        mmfFile = new MemoryMappedFile();
        mmf = (char *) mmfFile->map(workname);
        if (!mmf) {
            cout << "Unable to memory map file";
            exit(1);
        }

        syncDelaySecs = options["syncDelay"].numberInt();
        if( syncDelaySecs ) {
            boost::thread t(syncThread);
        }
    }

    cout << "testing...  (Press Ctrl-C to break)"<< endl;

    unsigned wthr = (unsigned) options["nThreads"].numberInt();
    if (wthr == 0) wthr = 1;
    unsigned i = 0;
    unsigned d = 1;
    unsigned &nthr = nThreadsRunning;
    unsigned long long totalOps = 0;
    Timer t;
    while( i == 0 || threads.get() > 0) {
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
        cout << format_time_t() << ": " << w << " ops/sec " <<
            setiosflags(ios::fixed) << setprecision(2) << 
            ((double) w * opSize / MB) << " MB/sec\n";
    }
    cout << "\nSummary:\n";
    if (r)      cout << "read ops     = " << totReadOps << endl;
    if (w)      cout << "write ops    = " << totWriteOps << endl;
    if (r && w) cout << "total ops    = " << totalOps << endl;
    cout << "total time   = " << t.seconds() << " seconds\n" <<
            "total MB/sec = " << 
            setiosflags(ios::fixed) << setprecision(2) << 
            ((double)totalOps * opSize / MB / t.seconds());
 }
            

char* validOptions[] = {
    "nThreads", "fileSizeMB", "sleepMicros", "mmf", 
    "r", "w", "syncDelay", "fileName", "opSize", "random", "append", 0
};


#ifdef _WIN32
BOOL CtrlHandler(DWORD fdwCtrlType) {
    shuttingDown = true;
    return TRUE;
}

#else
sigset_t asyncSignals;
void interruptThread() {
    int x;
    sigwait( &asyncSignals, &x );
    shuttingDown = true;
}


#endif


int runner(int argc, char *argv[]) {
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
                "    random:<bool>     // randomize write data (default false)\n"
                "    append:<bool>     // run in journaling/append mode (default false)\n"
                "    syncDelay:<n>,    // secs between fsyncs, like --syncdelay in mongod. (default 0/never)\n"
                "    fileName:<string> // pathname of the work file (default mongoperf__testfile__tmp)\n"
                "    opSize:<n>        // size of reads and writes in bytes (default 4096)\n"
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
            cout << "Error: Unable to open " << fname;
            delete in;
            return 1;
        }
    }

    cout << "use -h for help" << endl;
    cout << "reading options from " << fname << "...\n\n";
    char input[1024];
    memset(input, 0, sizeof(input));
    in->read(input, 1000);
    if( *input == 0 ) { 
        cout << "Error: No options found reading " << fname << endl;
        return 2;
    }
    if (in != &cin)
        delete in;

    string s = input;
    str::stripTrailing(s, "\n\r\0x1a");
    try { 
        options = fromjson(s);
    }
    catch(...) { 
        cout << s << endl;
        cout << "couldn't parse json options" << endl;
        return 2;
    }
    cout << "options:\n" << options.toString() << "\n\n" << endl;

// disallow unrecognized names for option field names
    bool err = false;
    for (BSONObjIterator i = options.begin(); i.more(); ) {
        const char* name = i.next().fieldName();
        bool found = false;
        for (char** s = validOptions; !found && *s; s++)
            found = strcmp(*s, name) == 0;
        if (!found) {
            err = true;
            cout << "Error: Unrecognized field name (" << name << ") in the options.\n";
        }
    }
    if (err) return 2;

    r = options["r"].trueValue();
    w = options["w"].trueValue();
    if (!r && !w) {
        cout << "Error: Neither read nor write modes specified in the options.\n" 
                "Boolean value 'r' and/or 'w' must be present.\n";
        return 2;
    }

    if (!isNumber("fileSizeMB") || !isNumber("sleepMicros") || !isNumber("nThreads") || !isNumber("syncDelay") || !isNumber("opSize"))
        return 2;
    BSONElement el = options["fileSizeMB"];
    if (el.type() == EOO)
        len = 1;
    else {
        len = el.numberLong();
        if (len == 0) {
            cout << "Error: fileSizeMB must be at least 1\n";
            return 2;
        }
    }   
    cout << "test file size: " << len << "MB ..." << endl;

    if( 0 && len > 2048 && !options["mmf"].trueValue() ) { 
        // todo make tests use 64 bit offsets in their i/o -- i.e. adjust LogFile::writeAt and such
        cout << "\nsizes > 2GB not yet supported with mmf:false" << endl; 
        return 2;
    }

    el = options["fileName"];
    if (el.type() != EOO) {
        if (el.type() != String) {
            cout << "Expected a string for fileName";
            return 2;
        }
        workname = el.valuestr();
    }

    opSize = options["opSize"].numberInt();
    if (opSize == 0)
        opSize = PG;

    random = options["random"].trueValue();
    append = options["append"].trueValue();

    if (append && (!w || r || options["mmf"].trueValue())) {
        cout << "Error: When append is true, w must be true, but r and mmf must be false";
        return 2;
    }

    go();

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

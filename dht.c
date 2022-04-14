/**
 * dht.c
 *
 * CS 470 Project 4
 *
 * Implementation for distributed hash table (DHT).
 *
 * Name: Benjamin Huber
 *
 */

#include <mpi.h>
#include <pthread.h>

#include "dht.h"

/* ========================================================================== */
/* ==========================MPI RELEVANT VARIABLES========================== */
/* ========================================================================== */

/*
 * Private module variable: current process ID (MPI rank)
 */
static int rank;

/*
 * Private module variable: number of processes
 */
static int nprocs;

/* ========================================================================== */
/* ========================PTHREADS RELEVANT VARIABLES======================= */
/* ========================================================================== */

/*
 * An integer that keeps track of how many MPI processes are done.  Our server 
 * thread will quit when this number equals our nprocs.
 */
static int done_count;

/*
 * The PID of our processes' server thread.  We will use this to join on it at
 * the end, so we don't print our results prematurely, or exit our program 
 * before it does.
 */
pthread_t server_pid;

/*
 * An enum to identify the message type that I'm recieving.
 */
typedef enum {
    PUT = 0, GET, SIZE, SYNC, DONE, BADREQ
} dst_message_type;

/*
 * The struct that we will be using to send/receive MPI requests to other 
 * processes' server threads.  These contain very basic information.
 *   [mt]     -->  Contains an enum that tells us what type of request is 
 *                 being recieved.  We may ignore other fields if the 
 *                 request does not need them.
 *   [origin] -->  Contains the process number of the sending process, so 
 *                 that our process can send back an acknowledgement.
 *   [key]    -->  Contains the key to either request from or put our 
 *                 value into our hash-table.
 *   [value]  -->  Contains the value to put into our hash table.
 */
typedef struct dst_key_value_packet {
    dst_message_type mt;
    int              origin;
    char             key[MAX_KEYLEN];
    long             value;
} dst_message_p;

/*
 * given a key name, return the distributed hash table owner
 * (uses djb2 algorithm: http://www.cse.yorku.ca/~oz/hash.html)
 */
int hash(const char *name)
{
    unsigned hash = 5381;
    while (*name != '\0') {
        hash = ((hash << 5) + hash) + (unsigned)(*name++);
    }
    return hash % nprocs;
}

/*
 * Server thread work-loop.  It will await requests and then perform them.  
 * Upon reaching its nth done message where n is equivalent to nprocs, it will
 * terminate.
 */
void* server_loop (void* arg)
{
    dst_message_p *rcv_buffer = (dst_message_p*) malloc (sizeof (dst_message_p));
    while (done_count != nprocs) {
        MPI_Recv (rcv_buffer, sizeof(dst_message_p), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        switch (rcv_buffer->mt) {
            case DONE:
                done_count += 1;
                break;
            default:
                break;
        }
    }
    free (rcv_buffer);
    return NULL;
}

int dht_init()
{
    // This code is essentially copied from the Project Prompt.  It ensures
    // that MPI is initialized in MPI_THREAD_MULTIPLE mode, to enable 
    // multithreading with MPI.
    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
    if (provided != MPI_THREAD_MULTIPLE) {
        printf("ERROR: Cannot initialize MPI in THREAD_MULTIPLE mode.\n");
        exit(EXIT_FAILURE);
    }

    // Set our local done_count to 0
    done_count = 0;

    // This will assign each process their rank correctly.
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    // This will perform local initialization for the data structures
    local_init();

    // This will spawn our worker thread.
    pthread_create(&server_pid, NULL, server_loop, NULL);
    return rank;
}

void dht_put(const char *key, long value)
{
    local_put(key, value);
}

long dht_get(const char *key)
{
    return local_get(key);
}

size_t dht_size()
{
    return local_size();
}

void dht_sync()
{
    // nothing to do in the serial version
}

/*
 * Once our process reaches this point in its main thread, we know that it has 
 * completed all of its MPI requests, and just needs to wait for all of the 
 * other MPI processes to finish up theirs, so we send a notification to all 
 * other processes' server threads telling them that we are "done", and wait 
 * for our server to terminate.
 *
 * This does include a self-send, but since we have another thread handling 
 * recieves, this is fine, as even if we do block on our sends, we will still
 * terminate.
 */
void dht_destroy(FILE *output)
{
    // build our "done" message
    dst_message_p done_message;
    done_message.mt = DONE;
    for (int i = 0; i < nprocs; i++) {
        MPI_Send((void*) &done_message, sizeof(dst_message_p), MPI_BYTE, i, 0, MPI_COMM_WORLD);
    }

    pthread_join(server_pid, NULL);

    local_destroy(output);

    MPI_Finalize();
}


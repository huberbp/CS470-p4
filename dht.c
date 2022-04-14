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
#define ACK_TAG 1234
#define NORM_TAG 5213
#define SIZE_TAG 2981

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
    PUT = 0, GET, SIZE, SYNC, DONE, ACK, BADREQ
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
        MPI_Recv (rcv_buffer, sizeof(dst_message_p), MPI_BYTE, MPI_ANY_SOURCE, NORM_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        switch (rcv_buffer->mt) {
            case PUT:
            {
                // Put the key and value into our local copy of the table.
                local_put(rcv_buffer->key, rcv_buffer->value);

                // Send back confirmation that we have done that.
                dst_message_p ack_message;
                ack_message.mt = ACK;
                MPI_Send(&ack_message, sizeof(dst_message_p), MPI_BYTE, rcv_buffer->origin, ACK_TAG, MPI_COMM_WORLD);
                break;
            }
            case GET:
            {
                // Get the value from our local copy of the table.
                long get_result = local_get(rcv_buffer->key);

                // Send back the results of that get.
                dst_message_p response_message;
                response_message.mt = ACK;
                response_message.value = get_result;
                MPI_Send(&response_message, sizeof(dst_message_p), MPI_BYTE, rcv_buffer->origin, ACK_TAG, MPI_COMM_WORLD);
                break;
            }
            case SIZE:
            {
                long size = local_size();

                // Send the results of size.
                dst_message_p size_message;
                size_message.mt = ACK;
                size_message.value = size;
                MPI_Send (&size_message, sizeof(dst_message_p), MPI_BYTE, rcv_buffer->origin, SIZE_TAG, MPI_COMM_WORLD);
                break;
            }
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

/*
 * This is pretty self-explanatory except for the fact that I essentially use a 
 * MPI_Recv call as a wait for a go-ahead from the thread we pinged.
 */
void dht_put(const char *key, long value)
{
    // Build our message
    dst_message_p put_message;
    put_message.mt = PUT;
    put_message.value = value;
    put_message.origin = rank;
    // Load our key into our struct
    strncpy (put_message.key, key, MAX_KEYLEN - 1);
    
    // Calculate the process to send it to
    int dest = hash(key);
    MPI_Send(&put_message, sizeof (dst_message_p), MPI_BYTE, dest, NORM_TAG, MPI_COMM_WORLD);

    // We can just use the buffer we created to recieve our ack message.
    MPI_Recv(&put_message, sizeof (dst_message_p), MPI_BYTE, dest, ACK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

/*
 * Obviously if we are hashing our keys to determine where we put them, we can 
 * hash our keys to determine where to find them, I make use of this assumption 
 * throughout the program.
 */
long dht_get(const char *key)
{
    // Build our message
    dst_message_p get_message;
    get_message.mt = GET;
    get_message.origin = rank;
    // Load our key into our struct
    strncpy (get_message.key, key, MAX_KEYLEN - 1);

    // Calculate the process to find our key in
    int dest = hash(key);
    MPI_Send(&get_message, sizeof (dst_message_p), MPI_BYTE, dest, NORM_TAG, MPI_COMM_WORLD);

    // We recieve our message from the requested thread.
    MPI_Recv(&get_message, sizeof (dst_message_p), MPI_BYTE, dest, ACK_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    return get_message.value;
}

/* 
 * 
 */
size_t dht_size()
{
    // Build our message
    dst_message_p size_request;
    size_request.mt = SIZE;
    size_request.origin = rank;

    // Send requests to all processes
    for (int i = 0; i < nprocs; i++) {
        MPI_Send(&size_request, sizeof (dst_message_p), MPI_BYTE, i, NORM_TAG, MPI_COMM_WORLD);
    }

    size_t total = 0;
    // Recieve requests from all processes and aggregate them
    for (int i = 0; i < nprocs; i++) {
        MPI_Recv(&size_request, sizeof (dst_message_p), MPI_BYTE, i, SIZE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        total += size_request.value;
    }

    return total;
}

/*
 * In order to sync all we need to do is barrier.
 */
void dht_sync()
{
    MPI_Barrier(MPI_COMM_WORLD);
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
        MPI_Send((void*) &done_message, sizeof(dst_message_p), MPI_BYTE, i, NORM_TAG, MPI_COMM_WORLD);
    }

    // Wait for our server thread to terminate.
    pthread_join(server_pid, NULL);

    // Once our server thread has terminated, we can dump our output.
    local_destroy(output);

    MPI_Finalize();
}


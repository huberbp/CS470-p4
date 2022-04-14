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

#define MAX_LOCAL_PAIRS 65536

/*
 * Private module variable: current process ID (MPI rank)
 */
static int rank;

/*
 * Private module variable: number of processes
 */
static int nprocs;

/*
 * Local variable to keep track of the # of pairs on each node
 */
static size_t pair_count;

/*
 * Key-Value pair structure. For reference, MAX_KEYLEN is 64 and defined in 
 * "local.h".
 */
typedef struct key_value_pair {
    char key[MAX_KEYLEN];
    long value;
} kv_pair;

/*
 * Private module variable: array that stores all local key-value pairs
 *
 * NOTE: the pairs are stored lexicographically by key for cleaner output
 */
static kv_pair *kv_pairs;

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

    // Assign each process their rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Get the total number of processes
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    // Set the number of pairs present in each MPI Process to 0
    pair_count = 0;

    // Dynamically allocate our local kv_pair arrays in each MPI process
    kv_pairs = (kv_pair*) malloc(MAX_LOCAL_PAIRS * sizeof(kv_pair));
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

void dht_destroy(FILE *output)
{
    local_destroy(output);

    // Free our dynamically allocated MPI processes
    free(kv_pairs);

    // Finalize MPI
    MPI_Finalize();
}


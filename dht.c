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

/*
 * Private module variable: current process ID (MPI rank)
 */
static int rank;

/*
 * Private module variable: number of processes
 */
static int nprocs;

/*
 * Server thread work-loop.  It will await requests and then 
 * dispatch calls to 
 */
void* server_loop (void* arg)
{
    
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

    // This will assign each process their rank correctly.
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    // This will perform local initialization for the data structures
    local_init();
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

    MPI_Finalize();
}


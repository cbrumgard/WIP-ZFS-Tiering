
/**
 * TODO Need to decide on copyright and authorship
 */

#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/dsl_pool.h>
#include <sys/dsl_scan.h>
#include <sys/vdev_impl.h>
#include <sys/zio.h>
#include <sys/abd.h>
#include <sys/fs/zfs.h>

typedef struct data_range data_range_t;
typedef struct tiering_map tiering_map_t;


/* TODO Need to autodetect this */
#define PERF_TIER_ALLOC_TRACKER_RECORD_SIZE 1048576


struct block_alloc_dist {
    u_int64_t size;
    u_int32_t ratio;
};

static struct block_alloc_dist block_alloc_dist[] = {
        { .size = 512,  .ratio = 512  },
        { .size = 1024, .ratio = 256  },
        { .size = 2048, .ratio = 256  },
        { .size = PERF_TIER_ALLOC_TRACKER_RECORD_SIZE, .ratio = 1 }, };

#define NUM_BLOCK_SIZES (sizeof(block_alloc_dist)/sizeof(block_alloc_dist[0]))



/* TODO remove, for analysis and debugging */
static int bsize_by_txg[512][NUM_BLOCK_SIZES];


struct perf_tier_alloc_tracker {

    struct bucket {

        /* Total number of buckets */
        u_int64_t nblocks;

        /* Number of free buckets remaining */
        u_int64_t nfree_blocks;

        /* Pointer to the head of the free list */
        u_int64_t free_idx_head;

        /* Size of blocks */
        u_int64_t bsize;

        /* Base address of the blocks */
        u_int64_t addr_base;

        /* List of blocks (The index serves as the slot id.  The entry contains
         * the next number or -1 if occupied */
        u_int64_t blocks[0] __attribute__((aligned));

    } *buckets[NUM_BLOCK_SIZES];


    /* Locking and signaling mechanisms */
    kmutex_t perf_tier_allocs_lock;
    kcondvar_t perf_tier_allocs_cv;

    avl_tree_t address_tree;

    krwlock_t lock;
};



struct data_range {

    /* Pointer to the top private data structure */
    tiering_map_t *tiering_map;

    /* Offset of the data range on capacity tier */
    u_int64_t cap_offset;

    /* Size of the data range on capacity tier */
    u_int64_t cap_size;

    /* Offset of data range on performance tier */
    u_int64_t perf_offset;

    /* Size of data range on performance tier */
    u_int64_t perf_size;

    /* Data buffer */
    abd_t *databuf;

    /* Node pointers */
    list_node_t migration_queue_node;

    avl_node_t address_tree_link;

    zfs_refcount_t refcount;

    zio_t *orig_zio;

    /* TODO rewrite attempt */
    blkptr_t blkptr;

    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker;
};


struct tiering_map {

    /* My vdev */
    vdev_t *tiering_vdev;

    /* Pointer to the performance tier vdev */
    vdev_t *performance_vdev;

    /* Pointer to the capacity tier vdev */
    vdev_t *capacity_vdev;

    /* List of ranges (by offset and length) read to be read from
     * the performance tier (buffer unfilled) */
    list_t from_performance_tier_data_ranges;

    /* List of ranges (by offset and length) ready to go on the capacity tier
     * buffer filled */
    list_t to_capacity_tier_data_ranges;

    /* Number of free migration buffers left */
    u_int64_t num_of_free_bufs;

    /*   */
    kmutex_t tiering_migration_thr_lock;

    /*  */
    kcondvar_t tiering_migration_thr_cv;

    /* */
    kthread_t *tiering_thread;

    /* Flag for controlling the tiering thread */
    u_int8_t tiering_thread_exit;

    /* Allocator tracker for the performance tier */
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker;
};


static data_range_t *
data_range_create(tiering_map_t *tiering_map, u_int64_t cap_offset,
                  u_int64_t cap_size, u_int64_t perf_offset,
                  u_int64_t perf_size,
                  struct perf_tier_alloc_tracker *perf_tier_alloc_tracker){

    data_range_t *data_range = kmem_alloc(sizeof(data_range_t), KM_SLEEP);

    data_range->tiering_map = tiering_map;
    data_range->cap_offset = cap_offset;
    data_range->cap_size = cap_size;
    data_range->perf_offset = perf_offset;
    data_range->perf_size = perf_size;
    data_range->databuf = NULL;
    data_range->orig_zio = NULL;
//    memcpy(&data_range->blkptr, zio->io_bp, sizeof(blkptr_t));
    data_range->perf_tier_alloc_tracker = perf_tier_alloc_tracker;


    /* Increment the reference count */
    zfs_refcount_create(&data_range->refcount);
    zfs_refcount_add(&data_range->refcount, FTAG);

    return data_range;
}


static void
data_range_destroy(data_range_t *data_range) {

    ASSERT(zfs_refcount_count(&data_range->refcount) == 0);

    zfs_refcount_destroy(&data_range->refcount);

    kmem_free(data_range, sizeof(data_range_t));
}

static void
data_range_reference_change(data_range_t *data_range) {

    mutex_enter(&data_range->perf_tier_alloc_tracker->perf_tier_allocs_lock);

    cv_broadcast(&data_range->perf_tier_alloc_tracker->perf_tier_allocs_cv);

    mutex_exit(&data_range->perf_tier_alloc_tracker->perf_tier_allocs_lock);
}

static int
address_compare(const void *v1, const void *v2) {

    const data_range_t *dr1 = v1;
    const data_range_t *dr2 = v2;

    /* Compares the txg group and then if those match the offset on the
     * capacity tier */
    if(dr1->blkptr.blk_birth < dr2->blkptr.blk_birth) {
        return -1;

    }else if(dr1->blkptr.blk_birth > dr2->blkptr.blk_birth) {
        return 1;

    }else {
        if(dr1->cap_offset < dr2->cap_offset) {
            return -1;
        }else if(dr1->cap_offset > dr2->cap_offset) {
            return 1;
        }
    }

    return 0;
}



static struct perf_tier_alloc_tracker *
allocate_perf_tier_alloc_tracker(vdev_t *vd) {

    u_int64_t num_blocks[NUM_BLOCK_SIZES];


    num_blocks[NUM_BLOCK_SIZES - 1] = (vd->vdev_psize/
            block_alloc_dist[NUM_BLOCK_SIZES - 1].size) -
                    (block_alloc_dist[NUM_BLOCK_SIZES - 1].ratio);

    for(int i=0; i<NUM_BLOCK_SIZES-1; i++) {
        num_blocks[i] = block_alloc_dist[i].ratio *
                block_alloc_dist[NUM_BLOCK_SIZES - 1].ratio;
    }


    int64_t remaining_size = vd->vdev_psize;

    for(int i=0; i<NUM_BLOCK_SIZES; i++) {
        ASSERT(num_blocks[i] > 0);

        remaining_size -= num_blocks[i] * block_alloc_dist[i].size;

        ASSERT(remaining_size >= 0);
    }


    zfs_dbgmsg("Inside of %s: space available %lld remaining_space %lld",
               __FUNCTION__, vd->vdev_psize, remaining_size);

    for(int i=0; i<NUM_BLOCK_SIZES; i++) {
        zfs_dbgmsg("\tBucket size: %d num: %d", block_alloc_dist[i].size,
                   num_blocks[i]);
    }

    /* Allocate an instance of the perf tiers allocation tracker */
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker =
            kmem_zalloc(sizeof(struct perf_tier_alloc_tracker), KM_SLEEP);

    /* If the allocation was successful, initialize the struct */
    if(perf_tier_alloc_tracker != NULL) {

        /* Initialize the avl tree for holding the data range */
        avl_create(&perf_tier_alloc_tracker->address_tree, address_compare,
                   sizeof(data_range_t), offsetof(data_range_t, address_tree_link));


        /* Lock and CV initialization */
        mutex_init(&perf_tier_alloc_tracker->perf_tier_allocs_lock, NULL,
                   MUTEX_DEFAULT, NULL);
        cv_init(&perf_tier_alloc_tracker->perf_tier_allocs_cv, NULL, CV_DEFAULT, NULL);

        /* Initialize the rw lock for protecting the address map */
        rw_init(&perf_tier_alloc_tracker->lock, NULL, RW_DEFAULT, NULL);

        u_int64_t addr_base = 0;

        for(int i=NUM_BLOCK_SIZES-1; i>-1; i--) {

            struct bucket *bucket = kmem_zalloc(
                    sizeof(struct bucket) + num_blocks[i]*sizeof(u_int64_t), KM_SLEEP);

            if(perf_tier_alloc_tracker != NULL) {
                bucket->nblocks = num_blocks[i];
                bucket->nfree_blocks = num_blocks[i];
                bucket->bsize = block_alloc_dist[i].size;


                /* Each bucket entry points to the next free */
                for(u_int64_t idx=0; idx<num_blocks[i]-1; idx++) {
                    bucket->blocks[idx] = idx+1;
                }

                /* Point the free list to the start of the buckets */
                bucket->free_idx_head = 0;

                /* The last bucket points to the sentinel value */
                bucket->blocks[num_blocks[i]-1] = (u_int64_t) -1;

                /* Address of the start of the blocks on the vdev */
                bucket->addr_base = addr_base;
                addr_base += num_blocks[i] * block_alloc_dist[i].size;

                perf_tier_alloc_tracker->buckets[i] = bucket;

            } else {
                goto ERROR_HANDLER;
            }
        }
    }

    /* Return the perf tier allocs */
    return perf_tier_alloc_tracker;

    ERROR_HANDLER:


        if(perf_tier_alloc_tracker != NULL) {

            for(int i=0; i<NUM_BLOCK_SIZES; i++) {

                if(perf_tier_alloc_tracker->buckets[i] != NULL) {
                    kmem_free(perf_tier_alloc_tracker->buckets[i],
                              sizeof(struct bucket) + num_blocks[i] * sizeof(u_int64_t));
                }
            }

            /* Free the avl tree */
            avl_destroy(&perf_tier_alloc_tracker->address_tree);

            /* Free the rw lock */
            rw_destroy(&perf_tier_alloc_tracker->lock);


            mutex_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_lock));
            cv_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_cv));


            kmem_free(perf_tier_alloc_tracker, sizeof(struct perf_tier_alloc_tracker));
        }

    return NULL;
}


static void
free_performance_tier_alloc_tracker(struct perf_tier_alloc_tracker *perf_tier_alloc_tracker) {


    /* Iterate through the nodes of the avl address tree and release the
    * resources tied to the nodes */
    for(data_range_t *cookie=NULL, *data_range=avl_destroy_nodes(&perf_tier_alloc_tracker->address_tree, (void **)&cookie);
        data_range != NULL;
        data_range=avl_destroy_nodes(&perf_tier_alloc_tracker->address_tree, (void **)&cookie)) {

        if(data_range->databuf != NULL) {
            abd_free(data_range->databuf);
        }

        data_range->databuf = NULL;

        kmem_free(data_range, sizeof(*data_range));
    }

    /* Free the avl tree */
    avl_destroy(&perf_tier_alloc_tracker->address_tree);

    /* Free the rw lock */
    rw_destroy(&perf_tier_alloc_tracker->lock);


    mutex_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_lock));
    cv_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_cv));

    for(int i=0; i<NUM_BLOCK_SIZES; i++) {

        kmem_free(perf_tier_alloc_tracker->buckets[i],
                  sizeof(struct bucket) + perf_tier_alloc_tracker->buckets[i]->nblocks*sizeof(u_int64_t));
    }


    kmem_free(perf_tier_alloc_tracker,
              sizeof(struct perf_tier_alloc_tracker));

}


static struct bucket *
performance_tier_alloc_tracker_find_bucket(
        struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
        u_int64_t size) {

    struct bucket *bucket = NULL;

    /* Get the bucket index for the size requested */
    for(int i=0; i<NUM_BLOCK_SIZES && bucket == NULL; i++) {
        if(size <= block_alloc_dist[i].size){
            bucket = perf_tier_alloc_tracker->buckets[i];
        }
    }

    return bucket;
}

static struct data_range *
performance_tier_alloc_tracker_get_block(
        struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
        struct tiering_map *tiering_map,
        struct blkptr *bp,
        u_int64_t cap_offset,
        u_int64_t size) {

    ASSERT(size <= PERF_TIER_ALLOC_TRACKER_RECORD_SIZE);

    /* TODO transfer ownership to here */
    data_range_t *data_range = data_range_create(tiering_map, cap_offset,
                                                 size, -1, size,
                                                 perf_tier_alloc_tracker);


    zfs_refcount_transfer_ownership(&data_range->refcount, data_range_create, FTAG);


    struct bucket *bucket = performance_tier_alloc_tracker_find_bucket(
            perf_tier_alloc_tracker, size);

    ASSERT(bucket != NULL);

    mutex_enter(&perf_tier_alloc_tracker->perf_tier_allocs_lock);


    /* Wait until there are free buckets */
    while(bucket->nfree_blocks == 0) {
        zfs_dbgmsg("Inside of %s: Out of blocks for bucket size %lld", __FUNCTION__, bucket->bsize);

        int nevicts = 0;

        rw_enter(&perf_tier_alloc_tracker->lock, RW_WRITER);

        /* Get the first data range (sorted by txg, offset) */
        data_range_t *evict_candiate_dr = avl_first(&perf_tier_alloc_tracker->address_tree);

        if(evict_candiate_dr != NULL) {

            /* Get the transaction group to free */
            //u_int64_t txg = evict_candiate_dr->blkptr.blk_birth;

            do {

                data_range_t *next_dr = AVL_NEXT(&perf_tier_alloc_tracker->address_tree, evict_candiate_dr);

                /* Get the current reference count */
                int64_t refcount = zfs_refcount_count(&evict_candiate_dr->refcount);


                /* If there is only one reference count, then we can release this one */
                if (refcount == 1) {

                    struct bucket *evict_bucket = performance_tier_alloc_tracker_find_bucket(perf_tier_alloc_tracker, evict_candiate_dr->perf_size);

                    if(evict_bucket->bsize == bucket->bsize) {

                        avl_remove(&perf_tier_alloc_tracker->address_tree, evict_candiate_dr);

                        u_int64_t block_idx = (evict_candiate_dr->perf_offset - evict_bucket->addr_base) /
                                evict_bucket->bsize;


                        zfs_dbgmsg(
                                "Inside of %s: eviction bucket size = %lld eviction block idx = %lld",
                                __FUNCTION__,
                                evict_bucket->bsize, block_idx);

                        ASSERT(evict_bucket->bsize >= evict_candiate_dr->perf_size);
                        ASSERT(block_idx < evict_bucket->nblocks && block_idx >= 0);

                        /* Add the free block back to the list of free records in the buckets */
                        evict_bucket->blocks[block_idx] =
                                evict_bucket->free_idx_head;

                        evict_bucket->free_idx_head = block_idx;

                        /* Increment the number of free buckets */
                        evict_bucket->nfree_blocks++;

                        zfs_refcount_remove(&evict_candiate_dr->refcount,
                                            &perf_tier_alloc_tracker->address_tree);

                        data_range_destroy(evict_candiate_dr);

                        nevicts++;
                        break;
                    }
                }

                evict_candiate_dr = next_dr;

            }while(evict_candiate_dr != NULL /*&& evict_candiate_dr->blkptr.blk_birth == txg*/);
        }

        rw_exit(&perf_tier_alloc_tracker->lock);

        /* Need to sleep here until one is free, wait until signal
         * for data object change */
        if(bucket->nfree_blocks == 0 && nevicts == 0) {
            cv_wait(&perf_tier_alloc_tracker->perf_tier_allocs_cv,
                    &perf_tier_alloc_tracker->perf_tier_allocs_lock);
        }

        if(nevicts > 0) {
            cv_broadcast(&data_range->perf_tier_alloc_tracker->perf_tier_allocs_cv);
        }
    }


    /* TODO remove, for analysis and debugging */
    {
        int bucket_idx = 0;
        for(bucket_idx=0; bucket_idx<NUM_BLOCK_SIZES; bucket_idx++) {
            if(bucket == perf_tier_alloc_tracker->buckets[bucket_idx]) {
                break;
            }
        }

        bsize_by_txg[bp->blk_birth][bucket_idx]++;
    }


    /* Get the index for the free bucket */
    u_int64_t block_idx = bucket->free_idx_head;

    zfs_dbgmsg("Inside of %s: bucket size = %lld block idx = %lld", __FUNCTION__,
               bucket->bsize, block_idx);
    ASSERT(block_idx >= 0 && block_idx < bucket->nblocks);

    /* Update the free index to the next free bucket index */
    bucket->free_idx_head = bucket->blocks[block_idx];

    /* Decrement the number of free buckets */
    bucket->nfree_blocks--;

    mutex_exit(&perf_tier_alloc_tracker->perf_tier_allocs_lock);


    data_range->perf_offset = block_idx * bucket->bsize + bucket->addr_base;


    ASSERT(data_range->perf_offset >= bucket->addr_base &&
           data_range->perf_offset < bucket->addr_base+(bucket->nblocks*bucket->bsize));


    avl_index_t where = 0;
    int64_t old_data_range_refcount = 0;


    rw_enter(&perf_tier_alloc_tracker->lock, RW_WRITER);


    /* Test if the original data exists */
    data_range_t *old_data_range = avl_find(&perf_tier_alloc_tracker->address_tree,
                                            data_range,
                                            &where);

    /* TODO handle conflict and data range removal (need to reclaim bucket
     * and change free bucket count and signal */
    /* If it does, then remove it */
    if(old_data_range != NULL) {

        ASSERT(old_data_range->cap_size == data_range->cap_size);

        avl_remove(&perf_tier_alloc_tracker->address_tree, old_data_range);

        old_data_range_refcount = zfs_refcount_remove(&old_data_range->refcount, &perf_tier_alloc_tracker->address_tree);
    }

    /* Insert into the address tree */
    avl_add(&perf_tier_alloc_tracker->address_tree, data_range);

    zfs_refcount_add(&data_range->refcount, &perf_tier_alloc_tracker->address_tree);

    rw_exit(&perf_tier_alloc_tracker->lock);



    /* Return the address of the free bucket */
    return data_range;
}


static void
vdev_tiering_performance_read_migration_child_done(zio_t *zio) {

    zfs_dbgmsg("Inside of %s, error = %d", __FUNCTION__, zio->io_error);

    ASSERT(zio->io_error == 0);

    /* TODO check error conditions */


    /* Get access to the data range and transfer ownership to this function */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);


    kmutex_t *lock = &data_range->tiering_map->tiering_migration_thr_lock;
    kcondvar_t *cv = &data_range->tiering_map->tiering_migration_thr_cv;
    list_t *to_cap_data_ranges = &data_range->tiering_map->to_capacity_tier_data_ranges;

    mutex_enter(lock);

    /* Move the data range (io_private of child) that are done to the ready
     * to be written to the capacity tier list and signal the migration thread */
    list_insert_head(to_cap_data_ranges, data_range);

    zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, to_cap_data_ranges);

    cv_signal(cv);
    mutex_exit(lock);
}


static void
vdev_tiering_performance_write_migration_child_done(zio_t *zio) {

    zfs_dbgmsg("Inside of %s, error = %d", __FUNCTION__, zio->io_error);

    ASSERT(zio->io_error == 0);

    /* Get access to the data range and transferownership to this function */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);



    tiering_map_t *tiering_map = data_range->tiering_map;
    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;


    /* TODO Free should be moved to the parent at a later time */
    abd_free(data_range->databuf);
    data_range->databuf = NULL;

    mutex_enter(lock);
    tiering_map->num_of_free_bufs++;
    cv_signal(cv);
    mutex_exit(lock);


    /* Drop the reference count on the data range and if count is
     * zero release it */
    zfs_refcount_remove(&data_range->refcount, FTAG);
    data_range_reference_change(data_range);
}



//
//static void
//vdev_tiering_performance_write_migration_parent_done(zio_t *zio) {
//
//
//    vdev_dbgmsg(zio->io_vd, "Inside of %s, error = %d", __FUNCTION__, zio->io_error);
//
//    tiering_map_t *tiering_map = zio->io_private;
//
//    /* TODO check error conditions */
//
//    spa_vdev_state_exit(tiering_map->capacity_vdev->vdev_spa,
//                        tiering_map->capacity_vdev, 0);
//
//
//}

static zio_t *
migration_issue_reads(zio_t *parent_zio, tiering_map_t *tiering_map, list_t *list) {

    /* If the parent doesn't already exist then create it */
    if(parent_zio == NULL) {
        parent_zio = zio_root(tiering_map->performance_vdev->vdev_spa,
                              NULL, NULL, ZIO_FLAG_DONT_PROPAGATE|ZIO_FLAG_IO_REWRITE);
        parent_zio->io_txg = 0;
    }

    /* Process the data ranges that need to be migration */
    for(data_range_t *data_range = list_remove_tail(list);
        data_range != NULL;
        data_range = list_remove_tail(list)) {

        zfs_refcount_transfer_ownership(&data_range->refcount, list, FTAG);

        /* TODO improve the allocation of data buffers */
        /* Create the data buffer */
        data_range->databuf = abd_alloc(data_range->cap_size, B_FALSE);

        /* Read in the data from the performance tier */

        zio_t *zio = zio_read_phys(parent_zio,
                                   tiering_map->performance_vdev->vdev_child[0],
                                   data_range->perf_offset,
                                   data_range->perf_size,
                                   data_range->databuf,
                                   ZIO_CHECKSUM_OFF,
                                   vdev_tiering_performance_read_migration_child_done,
                                   data_range,
                                   ZIO_PRIORITY_ASYNC_READ,
                                   ZIO_FLAG_CANFAIL,
                                   B_FALSE);

        zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, zio);

        zio_nowait(zio);
    }


    return parent_zio;
}

static zio_t *
migration_issue_writes(zio_t *parent_zio, tiering_map_t *tiering_map, list_t *list) {

    /* If the parent doesn't already exist then create it */
    if(parent_zio == NULL) {
        parent_zio = zio_root(tiering_map->capacity_vdev->vdev_spa,
                              NULL, NULL, 0);
        parent_zio->io_txg = 0;
    }

    /* Process the data ranges that need to be migration */
    for(data_range_t *data_range = list_remove_tail(list);
        data_range != NULL;
        data_range = list_remove_tail(list)) {

        zfs_refcount_transfer_ownership(&data_range->refcount, list, FTAG);

        /* Read in the data from the performance tier */
        zio_t *zio = zio_vdev_child_io(parent_zio,
                                      &data_range->blkptr,
                                      tiering_map->capacity_vdev,
                                      data_range->cap_offset,
                                      data_range->databuf,
                                      data_range->cap_size,
                                      ZIO_TYPE_WRITE,
                                      ZIO_PRIORITY_ASYNC_WRITE,
                                      0,
                                      vdev_tiering_performance_write_migration_child_done,
                                      data_range);

        zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, zio);

        zio_nowait(zio);
    }

    return parent_zio;
}

#define migration_thread_sleep_interval 20
#define MAX_BUFS_PER_ROUND 2

static void migration_thread(void *arg) {

    tiering_map_t *tiering_map = arg;
    list_t from_performance_tier_data_ranges;
    list_t to_capacity_tier_data_ranges;
    zio_t *parent_zio = NULL;

    /* Create a working list for data ranges being processed */
    list_create(&from_performance_tier_data_ranges, sizeof(data_range_t),
                offsetof(data_range_t, migration_queue_node));

    list_create(&to_capacity_tier_data_ranges, sizeof(data_range_t),
                offsetof(data_range_t, migration_queue_node));

    /* TODO remove print statement */
    vdev_dbgmsg(tiering_map->tiering_vdev, "migration_thread is running");

    /* Run until awoken and check if exit thread is set*/
    mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    while(tiering_map->tiering_thread_exit == 0) {

        while(/*tiering_map->num_of_free_bufs != MAX_BUFS_PER_ROUND &&*/
              list_is_empty(&tiering_map->from_performance_tier_data_ranges) &&
              list_is_empty(&tiering_map->to_capacity_tier_data_ranges) &&
              tiering_map->tiering_thread_exit == 0) {

            zfs_dbgmsg("migration_thread sleep: num_of_free_bufs %d", tiering_map->num_of_free_bufs);
            zfs_dbgmsg("migration_thread sleep: from_perf_tier %d", list_is_empty(&tiering_map->from_performance_tier_data_ranges));
            zfs_dbgmsg("migration_thread sleep: from_tier %d", list_is_empty(&tiering_map->to_capacity_tier_data_ranges));

            cv_timedwait(&(tiering_map->tiering_migration_thr_cv),
                         &(tiering_map->tiering_migration_thr_lock),
                         ddi_get_lbolt() + SEC_TO_TICK(
                                 migration_thread_sleep_interval));
        }

        /* TODO test code */
//        mutex_exit(&(tiering_map->tiering_migration_thr_lock));
//        delay(SEC_TO_TICK(10));
//        mutex_enter(&(tiering_map->tiering_migration_thr_lock));

        zfs_dbgmsg("migration_thread is awake");
        zfs_dbgmsg("migration_thread awake: num_of_free_bufs %d", tiering_map->num_of_free_bufs);
        zfs_dbgmsg("migration_thread awake: from_perf_tier %d", list_is_empty(&tiering_map->from_performance_tier_data_ranges));
        zfs_dbgmsg("migration_thread awake: from_tier %d", list_is_empty(&tiering_map->to_capacity_tier_data_ranges));


        /* There are data ranges on the performance tier to be migrated, move
         * them to a non-contended list */
        while(tiering_map->num_of_free_bufs > 0 &&
              list_is_empty(&tiering_map->from_performance_tier_data_ranges) == B_FALSE) {

            data_range_t *data_range = list_remove_tail(
                    &tiering_map->from_performance_tier_data_ranges);

            list_insert_head(&from_performance_tier_data_ranges,
                             data_range);

            zfs_refcount_transfer_ownership(&data_range->refcount,
                                            &tiering_map->from_performance_tier_data_ranges,
                                            &from_performance_tier_data_ranges);

            tiering_map->num_of_free_bufs--;
        }


        /* There are data ranges that are ready to be transferred to the
        * capacity tier */
        for (data_range_t *data_range = list_remove_tail(
                &tiering_map->to_capacity_tier_data_ranges);
             data_range!=NULL;
             data_range = list_remove_tail(
                     &tiering_map->to_capacity_tier_data_ranges)) {

            list_insert_head(&to_capacity_tier_data_ranges, data_range);

            zfs_refcount_transfer_ownership(&data_range->refcount,
                                            &tiering_map->to_capacity_tier_data_ranges,
                                            &to_capacity_tier_data_ranges);

        }

        /* Release the mutex since the working list doesn't need protection. */
        mutex_exit(&(tiering_map->tiering_migration_thr_lock));


        /* Lock the spa and create a parent zio that will unlock it on
         * completion of the io operations */
        spa_config_enter(tiering_map->performance_vdev->vdev_spa, SCL_ALL, FTAG, RW_READER);
        spa_config_enter(tiering_map->capacity_vdev->vdev_spa, SCL_ALL, FTAG, RW_READER);


        if(list_is_empty(&from_performance_tier_data_ranges) == B_FALSE) {

            parent_zio = migration_issue_reads(parent_zio,
                                               tiering_map,
                                               &from_performance_tier_data_ranges);
        }

        if(list_is_empty(&to_capacity_tier_data_ranges) == B_FALSE) {

            parent_zio = migration_issue_writes(parent_zio,
                                                tiering_map,
                                                &to_capacity_tier_data_ranges);
        }


        zio_wait(parent_zio);

        //delay(SEC_TO_TICK(10));

        spa_config_exit(tiering_map->performance_vdev->vdev_spa, SCL_ALL, FTAG);
        spa_config_exit(tiering_map->capacity_vdev->vdev_spa, SCL_ALL, FTAG);

        /* TODO need to know when to free the parent and children zios */
        parent_zio = NULL;

        /* TODO remember to free the data ranges when ios are complete */

        /* TODO check on the state of the root io */


        /* Retake the mutex before continuing */
        mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    }

    /* TODO add assert that I should always have the tiering_migration_thr_lock
     * mutex at this point */


    /* Signal that the thread is stopped */
    tiering_map->tiering_thread_exit = 0;
    tiering_map->tiering_thread = NULL;
    cv_signal(&(tiering_map->tiering_migration_thr_cv));
    mutex_exit(&(tiering_map->tiering_migration_thr_lock));

    /* Destroy the working lists */
    list_destroy(&from_performance_tier_data_ranges);
    list_destroy(&to_capacity_tier_data_ranges);


    zfs_dbgmsg("migration_thread is stopped");

    thread_exit();
}


static data_range_t *
performance_tier_alloc_tracker_find_mapping(struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
        blkptr_t *bp, u_int64_t io_offset, u_int64_t io_size) {

    data_range_t search = {
            .blkptr = *bp,
            .cap_offset = io_offset
    };

    avl_index_t where;

    rw_enter(&perf_tier_alloc_tracker->lock, RW_READER);

    /* Find the mapping for the blkptr and offset */
    data_range_t *data_range = avl_find(&perf_tier_alloc_tracker->address_tree, &search, &where);

    /* Not found so find the next lowest entry */
    if(data_range == NULL) {
        data_range = avl_nearest(&perf_tier_alloc_tracker->address_tree, where, AVL_BEFORE);
    }

    /* TODO this check on the range may not be necessary or it may be necessary
     * to adjust the offsets on if they don't match so that the caller
     * will know how to adjust the I/O call offset correctly */
    /* Check the returned entry is valid and if not reset to NULL */
    if(data_range != NULL &&
            (data_range->cap_offset <= io_offset &&
                    (data_range->cap_offset+data_range->cap_size) >= (io_offset+io_size))) {

        /* Increment the reference count on the data range before returning it */
        zfs_refcount_add(&data_range->refcount, FTAG);

    } else {
        data_range = NULL;
    }


    rw_exit(&perf_tier_alloc_tracker->lock);

    return data_range;
}



static tiering_map_t *
vdev_tiering_map_init(vdev_t *my_vdev, vdev_t *fast_tier, vdev_t *slow_tier) {

    /* Allocate and initialize the perf tier allocation tracker */
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker =
            allocate_perf_tier_alloc_tracker(fast_tier->vdev_child[0]);

    /* Error so return */
    if(perf_tier_alloc_tracker == NULL) {
        return NULL;
    }


    /* Allocate the central main data structure for the tiering vdev instance */
    tiering_map_t *tiering_map = kmem_zalloc(sizeof(tiering_map_t), KM_SLEEP);

    /* This should always happen since sleep is specified until memory is
     * available but just in case */
    if(tiering_map != NULL) {
        tiering_map->tiering_vdev = my_vdev;
        tiering_map->performance_vdev = fast_tier;
        tiering_map->capacity_vdev = slow_tier;
        tiering_map->num_of_free_bufs = MAX_BUFS_PER_ROUND;

        list_create(&tiering_map->from_performance_tier_data_ranges, sizeof(data_range_t),
                    offsetof(data_range_t, migration_queue_node));
        list_create(&tiering_map->to_capacity_tier_data_ranges, sizeof(data_range_t),
                    offsetof(data_range_t, migration_queue_node));

        mutex_init(&tiering_map->tiering_migration_thr_lock, NULL,
                   MUTEX_DEFAULT, NULL);
        cv_init(&tiering_map->tiering_migration_thr_cv, NULL, CV_DEFAULT, NULL);

        tiering_map->tiering_thread = thread_create(NULL,
                                                    0,
                                                    migration_thread,
                                                    tiering_map,
                                                    0,
                                                    &p0,
                                                    TS_RUN,
                                                    minclsyspri);


        tiering_map->perf_tier_alloc_tracker = perf_tier_alloc_tracker;

    } else {
        free_performance_tier_alloc_tracker(perf_tier_alloc_tracker);
    }

    return tiering_map;
}



static void
vdev_tiering_map_free(tiering_map_t *tiering_map) {


    free_performance_tier_alloc_tracker(tiering_map->perf_tier_alloc_tracker);

    mutex_destroy(&(tiering_map->tiering_migration_thr_lock));
    cv_destroy(&(tiering_map->tiering_migration_thr_cv));

    list_destroy(&tiering_map->from_performance_tier_data_ranges);
    list_destroy(&tiering_map->to_capacity_tier_data_ranges);


    kmem_free(tiering_map, sizeof(tiering_map_t));
}



static int
vdev_tiering_open(vdev_t *vd, u_int64_t *asize, u_int64_t *max_asize,
                  uint64_t *logical_ashift, uint64_t *physical_ashift)
{
    tiering_map_t *tiering_map = NULL;

    /* TODO remove print statement */
    zfs_dbgmsg("Inside of vdev_tiering_open");

    /* TODO remove, for analysis and debugging */
    memset(bsize_by_txg, 0, sizeof(bsize_by_txg));

    /* Check that there is one vdev for tiering */
    if(vd->vdev_children != 1) {
        vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
        return (SET_ERROR(EINVAL));
    }

    vd->vdev_tsd = NULL;



    char name[ZFS_MAX_DATASET_NAME_LEN];
    spa_t *performance_spa = NULL;

    /* Create the name for the pool of the new tier */
    snprintf(name, sizeof(name), "%s-tier0", spa_name(vd->vdev_spa));

    zfs_dbgmsg("Inside of %s@%d looking for spa %s", __FUNCTION__, __LINE__, name);




    ASSERT(MUTEX_HELD(&spa_namespace_lock));

    /* Find the performance spa by name */
    performance_spa = spa_lookup(name);

    //mutex_exit(&spa_namespace_lock);

    /* Spa not found, so report and error */
    if (performance_spa == NULL) {
        zfs_dbgmsg("Inside of %s@%d performance_spa = %p", __FUNCTION__, __LINE__, performance_spa);
        return (SET_ERROR(EINVAL));
    }

    zfs_dbgmsg("Inside of %s@%d performance_spa = %p", __FUNCTION__, __LINE__, performance_spa);



    /* TODO new code for splitting the spa, this is based off of code from
    * spa_vdev_split_mirror */
//    {
//        char name[ZFS_MAX_DATASET_NAME_LEN];
////        spa_t *newspa = NULL;
//
//        /* Create the name for the pool of the new tier */
//        snprintf(name, sizeof(name), "%s-tier0", spa_name(vd->vdev_spa));
//
//
//        int error = spa_vdev_split_tier(vd->vdev_spa, name, vd->vdev_child[0]);
//
//        if(error) {
//            return (SET_ERROR(error));
//        }
//
//
//        zfs_dbgmsg("Inside of vdev_tiering_open@%d", __LINE__);
//    }


    /* Open all of the child vdevs */
    vdev_open_children(vd);

    /* Iterate over the children checking for open errors and
     * computing the record sizes */
    for (int c = 0; c < vd->vdev_children; c++) {
        vdev_t *child_vdev = vd->vdev_child[c];

        /* A child vdev experienced an error so fail and return */
        /* TODO Currently a failure of any child vdev will cause an error, in
         * the future might consider still working as long as one vdev is still
         * good. */
        if(child_vdev->vdev_open_error != 0) {
            /* TODO might want to propose an new VDEV_AUX_ERR for this state */
            vd->vdev_stat.vs_aux = VDEV_AUX_ERR_EXCEEDED;
            return child_vdev->vdev_open_error;
        }



        /* Find the mininum asize and ashift settings that will be compatible
         * for the child vdevs and  */
        *asize = MIN(*asize - 1, child_vdev->vdev_asize - 1) + 1;
        *max_asize = MIN(*max_asize - 1, child_vdev->vdev_max_asize - 1) + 1;
        *logical_ashift = MAX(*logical_ashift, child_vdev->vdev_ashift);
        *physical_ashift = MAX(*physical_ashift,
                               child_vdev->vdev_physical_ashift);
    }

    /* Create an initialize tiering map */
    tiering_map = vdev_tiering_map_init(vd,
                                        performance_spa->spa_root_vdev,
                                        vd->vdev_child[0]);

    /* Store inside of the vdev private data */
    vd->vdev_tsd = tiering_map;


    /* TODO remove print statement */
    zfs_dbgmsg("vd = %s fast vd = %s slow vd = %s spa = %p fast spa = %p slow spa = %p",
            vd->vdev_path, performance_spa->spa_root_vdev->vdev_path, vd->vdev_child[0]->vdev_path, vd->vdev_spa, performance_spa->spa_root_vdev->vdev_spa, vd->vdev_child[0]->vdev_spa);

    zfs_dbgmsg("Inside of %s@%d tiering_map = %p", __FUNCTION__, __LINE__, tiering_map);

    /* Success if tiering map was created successfully */
    return tiering_map == NULL;
}


static void
vdev_tiering_close(vdev_t *vd)
{
    tiering_map_t *tiering_map = vd->vdev_tsd;

    /* TODO remove print statement */
    zfs_dbgmsg("Inside of %s", __FUNCTION__ );

    /* TODO remove, for analysis and debugging */
    {
        for(int txg=0; txg<(sizeof(bsize_by_txg)/sizeof(bsize_by_txg[0])); txg++) {
            for(int bucket_idx = 0; bucket_idx<NUM_BLOCK_SIZES; bucket_idx++) {
                if(bsize_by_txg[txg][bucket_idx] != 0) {
                    zfs_dbgmsg("txg: %d bsize: %lld count: %d", txg, block_alloc_dist[bucket_idx].size, bsize_by_txg[txg][bucket_idx]);
                }
            }
        }
    }

    /* Iterate over the child vdevs and close them */
    for (int c = 0; c < vd->vdev_children; c++) {
        vdev_close(vd->vdev_child[c]);
    }

    /* Stop the vdev tiering thread */
    if(tiering_map != NULL) {
        mutex_enter(&(tiering_map->tiering_migration_thr_lock));
        tiering_map->tiering_thread_exit = 1;
        cv_signal(&(tiering_map->tiering_migration_thr_cv));
        while (tiering_map->tiering_thread_exit!=0) {
            cv_wait(&(tiering_map->tiering_migration_thr_cv),
                    &(tiering_map->tiering_migration_thr_lock));
        }
        mutex_exit(&(tiering_map->tiering_migration_thr_lock));


        /* TODO need to decide when to free tiering map, may need to hold if
         * ops are still underway */
        vdev_tiering_map_free(tiering_map);
    }
}

static void
vdev_tiering_performance_write_child_done(zio_t *zio) {

    /* TODO remove print statement */
    vdev_dbgmsg(zio->io_spa->spa_root_vdev, "Inside of %s", __FUNCTION__);

    /* TODO handle errors */
    ASSERT(zio->io_error == 0);



    /* Get access to the data range entry and transfer ownership of the
     * data_range */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);

    //tiering_map_t *tiering_map = data_range->tiering_map;

    /* TODO we need to handle reads before write is complete? */
    //address_map_add_mapping(tiering_map->address_map, data_range);

    /* TODO remove mapping if it fails */

    /* Remove the local reference to the data range */
    zfs_refcount_remove(&data_range->refcount, FTAG);

    data_range_reference_change(data_range);

//    zio_execute(zio->io_private);



//    tiering_map_t *tiering_map = data_range->tiering_map;
//    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
//    //kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;
//
//    /* TODO check for errors on write to performance tier */
//
//
//
//    /* TODO see how zio->io_priority works, might be able to modify it to make
//     * this less important */
//
//
//    mutex_enter(lock);
//    list_insert_head(&tiering_map->from_performance_tier_data_ranges, data_range);
//
//
//
//
//    /* TODO temporarily disable signal for demo */
//    //cv_signal(cv);
//
//    mutex_exit(lock);
}


static void
vdev_tiering_performance_read_child_done(zio_t *zio) {

    //vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_performance_read_child_done");

    /* TODO handle error */
    ASSERT(zio->io_error == 0);

    /* Get access to the data range entry and transfer ownership to this function */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);

    /* Remove the local reference to the data range */
    zfs_refcount_remove(&data_range->refcount, FTAG);

    data_range_reference_change(data_range);
}



static void
vdev_tiering_capacity_allocate_child_done(zio_t *zio) {
    zfs_dbgmsg("Inside of %s", __FUNCTION__);

    ASSERT(zio->io_error == 0);

    /* Get access to the data range entry and transfer ownership to this function */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);

    tiering_map_t *tiering_map = data_range->tiering_map;
    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    //kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;


    /* TODO check for errors on write to performance tier */

    /* TODO see how zio->io_priority works, might be able to modify it to make
     * this less important */


    /* Add to the list of data ranges ready for migration and transfer
     * ownership */
    mutex_enter(lock);
    list_insert_head(&tiering_map->from_performance_tier_data_ranges, data_range);

    zfs_refcount_transfer_ownership(&data_range->refcount, FTAG,
                                    &tiering_map->from_performance_tier_data_ranges);

    /* TODO temporarily disable signal for demo */
    //cv_signal(cv);

    mutex_exit(lock);
}

static void
vdev_tiering_capacity_read_child_done(zio_t *zio) {
    vdev_dbgmsg(zio->io_vd, "Inside of %s", __FUNCTION__);

    ASSERT(zio->io_error == 0);
}



static void
vdev_tiering_io_start(zio_t *zio) {

    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_vd->vdev_tsd;
//    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    //vdev_t *vd;
    data_range_t *data_range = NULL;

    /* TODO remove print statement */
    vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_io_start, tiering_map = %p", tiering_map);


    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ: {
            vdev_dbgmsg(zio->io_vd,
                        "vdev_tiering_io_start read op offset: %llu length %llu",
                        zio->io_offset, zio->io_size);


            data_range = performance_tier_alloc_tracker_find_mapping(
                    tiering_map->perf_tier_alloc_tracker,
                    zio->io_bp,
                    zio->io_offset,
                    zio->io_size);


            /* On the performance tier */
            if(data_range != NULL) {

                zfs_refcount_transfer_ownership(&data_range->refcount,
                                    performance_tier_alloc_tracker_find_mapping,
                                    FTAG);

                spa_t *perf_spa = tiering_map->performance_vdev->vdev_spa;

                spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);

                /* Do the read off the physical tier now and fill in the parents
                 * abd buffer */
                zio_t *perf_read = zio_read_phys(NULL,
                                                 tiering_map->performance_vdev->vdev_child[0],
                                                 data_range->perf_offset,
                                                 data_range->perf_size,
                                                 zio->io_abd,
                                                 ZIO_CHECKSUM_OFF,
                                                 vdev_tiering_performance_read_child_done,
                                                 data_range,
                                                 zio->io_priority,
                                                 ZIO_FLAG_CANFAIL,
                                                 B_FALSE);

                zfs_refcount_add(&data_range->refcount, perf_read);

                zio_wait(perf_read);


                spa_config_exit(perf_spa, SCL_ALL, FTAG);

                /* Create a child nop and use that to signal the parent that it is
                 * done */
                zio_t * nop_zio = zio_null(zio,
                                           zio->io_spa,
                                           tiering_map->capacity_vdev,
                                           NULL,
                                           NULL,
                                           ZIO_FLAG_CANFAIL);

                zio_add_child(zio, nop_zio);

                zio_nowait(nop_zio);

                /* Remove the local reference to the data range */
                zfs_refcount_remove(&data_range->refcount, FTAG);

                data_range_reference_change(data_range);

            /* On the capacity tier */
            } else {

                /* Schedule a read on the capacity tier */
                zio_nowait(
                        zio_vdev_child_io(zio,
                                          zio->io_bp,
                                          tiering_map->capacity_vdev,
                                          zio->io_offset,
                                          zio->io_abd,
                                          zio->io_size,
                                          zio->io_type,
                                          zio->io_priority,
                                          0,
                                          vdev_tiering_capacity_read_child_done,
                                          tiering_map));

            }

            /* Execute zio */
            zio_execute(zio);

        }
            break;

        /* Write operation */
        /* TODO Implement write op */
        case ZIO_TYPE_WRITE: {

           vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_start write op txg: %d offset: %llu length: %llu flags: %d",
                    zio->io_txg, zio->io_offset, zio->io_size, zio->io_flags);

           /* TODO this is only the base case of a write, need to handle more
            * complex cases like reslivering and scrubs */
           data_range = performance_tier_alloc_tracker_get_block(
                   tiering_map->perf_tier_alloc_tracker, tiering_map,
                   zio->io_bp, zio->io_offset, zio->io_size);


           zfs_refcount_transfer_ownership(&data_range->refcount,
                                           performance_tier_alloc_tracker_get_block,
                                           FTAG);

           data_range->orig_zio = zio;
           data_range->databuf = zio->io_abd;
           data_range->blkptr = *zio->io_bp;

           zio->io_prop.zp_copies = 1;

           spa_t *perf_spa = tiering_map->performance_vdev->vdev_spa;
           zio_prop_t *zp = &zio->io_prop;

           vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_start zp_checksum %d",
                        zp->zp_checksum);

           /* TODO this is a workaround because zp_checksum is set to inherit
            * but needs to be higher, need to create a new zio_prop with
            * the correct settings */
           zp->zp_checksum = ZIO_CHECKSUM_OFF;
           zp->zp_compress = ZIO_COMPRESS_OFF;

           ASSERT(zp->zp_checksum >= ZIO_CHECKSUM_OFF);
           ASSERT(zp->zp_checksum < ZIO_CHECKSUM_FUNCTIONS);
           ASSERT(zp->zp_compress >= ZIO_COMPRESS_OFF);
           ASSERT(zp->zp_compress < ZIO_COMPRESS_FUNCTIONS);
           ASSERT(DMU_OT_IS_VALID(zp->zp_type));
           ASSERT(zp->zp_level < 32);
           ASSERT(zp->zp_copies > 0);
           ASSERT(zp->zp_copies <= spa_max_replication(perf_spa));




/* Code for doing writes */
//           spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);
//
//           /* Schedule a write to the fast tier */
//           zio_wait(
//                   zio_write(NULL,
//                             perf_spa,
//                             spa_syncing_txg(perf_spa),// zio->io_txg,
//                             zio->io_bp,
//                             zio->io_abd,
//                             zio->io_lsize,
//                             zio->io_size,
//                             &zio->io_prop,
//                             NULL,
//                             NULL,
//                             NULL,
//                             vdev_tiering_performance_write_child_done,
//                             data_range,
//                             zio->io_priority,
//                             zio->io_flags,
//                             NULL));

//           spa_config_exit(perf_spa, SCL_ALL, FTAG);


            /* Write the data to the physical location on the performance tier */
            spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);

            zio_t *perf_zio = zio_write_phys(NULL,
                                           tiering_map->performance_vdev->vdev_child[0],
                                           data_range->perf_offset, //zio->io_offset,
                                           data_range->perf_size, //io->io_size,
                                           zio->io_abd,
                                           ZIO_CHECKSUM_OFF,
                                           vdev_tiering_performance_write_child_done,
                                           data_range,
                                           zio->io_priority,
                                           ZIO_FLAG_CANFAIL,
                                           B_FALSE);

            zfs_refcount_add(&data_range->refcount, perf_zio);

            zio_wait(perf_zio);

            spa_config_exit(perf_spa, SCL_ALL, FTAG);


            vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_io_start after zio_wait");


            /* Create a child vdev io that only allocates on the capacity tier */
            zio_t *cap_zio = zio_vdev_child_io(zio,
                                               zio->io_bp,
                                               tiering_map->capacity_vdev,
                                               zio->io_offset,
                                               NULL, //zio->io_abd,
                                               zio->io_size,
                                               zio->io_type,
                                               zio->io_priority,
                                               ZIO_FLAG_NODATA,
                                               vdev_tiering_capacity_allocate_child_done,
                                               data_range);


            zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, cap_zio);


            zio_nowait(cap_zio);

            /* Execute zio */
            zio_execute(zio);

            break;
        }

        /* Unsupported operation */
        /* TODO figure out which of these to support or pass along to lower
         * vdevs */
        case ZIO_TYPE_NULL:
        case ZIO_TYPE_FREE:
        case ZIO_TYPE_CLAIM:
        case ZIO_TYPE_IOCTL:
        case ZIO_TYPE_TRIM:
        case ZIO_TYPES:
        default:
            vdev_dbgmsg(zio->io_vd,"vdev_tiering_io_start unsupported operation %d",
                    zio->io_type);
            zio->io_error = SET_ERROR(ENOTSUP);
            zio_interrupt(zio);
            break;
    }
}


static void
vdev_tiering_io_done(zio_t *zio) {

    /* TODO remove print statement */
    //vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_io_done");

    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ:
            //vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_done read op offset: %llu length %llu", zio->io_offset, zio->io_size);
            break;

            /* Write operation */
            /* TODO Implement write op */
        case ZIO_TYPE_WRITE:
            //vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_done write op offset: %llu length %llu", zio->io_offset, zio->io_size);
            break;

            /* Unsupported operation */
            /* TODO figure out which of these to support or pass along to lower
             * vdevs */
        case ZIO_TYPE_NULL:
        case ZIO_TYPE_FREE:
        case ZIO_TYPE_CLAIM:
        case ZIO_TYPE_IOCTL:
        case ZIO_TYPE_TRIM:
        case ZIO_TYPES:
        default:
            vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_done unsupported operation %d",
                    zio->io_type);
            break;
    }
}


/* Provides the tiering operation functions. */
vdev_ops_t vdev_tiering_ops = {
    .vdev_op_init = NULL,   /* TODO find out what init does */
    .vdev_op_fini = NULL,   /* TODO find out what fini does */
    .vdev_op_open = vdev_tiering_open,  /* TODO study the change in open signature */
    .vdev_op_close = vdev_tiering_close,
    .vdev_op_asize = vdev_default_asize,    /* Use the default method since
 *                                             the tiering vdev does none of
 *                                             it's own allocations */
    .vdev_op_min_asize = vdev_default_min_asize,  /* TODO find out min_asize does */
    .vdev_op_min_alloc = NULL,  /* TODO find out min alloc does */

    .vdev_op_io_start = vdev_tiering_io_start,
    .vdev_op_io_done = vdev_tiering_io_done,
    .vdev_op_state_change = NULL,
    .vdev_op_need_resilver = NULL,
    .vdev_op_hold = NULL,
    .vdev_op_rele = NULL,
    .vdev_op_remap = NULL,
    .vdev_op_xlate = NULL,

    .vdev_op_rebuild_asize = NULL,     /* TODO find out what this function does */
    .vdev_op_metaslab_init = NULL,     /* TODO find out what this function does */
    .vdev_op_config_generate = NULL,   /* TODO find out what this function does */
    .vdev_op_nparity = NULL,           /* TODO find out what this function does */
    .vdev_op_ndisks = NULL,           /* TODO find out what this function does */

    .vdev_op_type = VDEV_TYPE_TIERING,  /* Name of vdev type */
    .vdev_op_leaf = B_FALSE             /* Not a leaf vdev */
};

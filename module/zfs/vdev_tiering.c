
/**
 * TODO Need to decide on copyright and authorship
 */

#include <sys/types.h>
#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/dsl_pool.h>
#include <sys/dsl_scan.h>
#include <sys/vdev_impl.h>
#include <sys/zio.h>
#include <sys/abd.h>
#include <sys/fs/zfs.h>
#include "../../include/sys/zio.h"
#include "sys/dmu.h"
#include "sys/zap.h"
#include "sys/zfs_znode.h"

typedef struct data_range data_range_t;
typedef struct tiering_map tiering_map_t;
typedef struct tier tier_t;

/* TODO Need to autodetect this */
#define PERF_TIER_ALLOC_TRACKER_RECORD_SIZE 1048576


struct block_alloc_dist {
    uint64_t size;
    u_int32_t ratio;
};

static struct block_alloc_dist block_alloc_dist[] __attribute__((unused)) = {
        { .size = 512,  .ratio = 512  },
        { .size = 1024, .ratio = 256  },
        { .size = 2048, .ratio = 256  },
        { .size = PERF_TIER_ALLOC_TRACKER_RECORD_SIZE, .ratio = 1 }, };

#define NUM_BLOCK_SIZES (sizeof(block_alloc_dist)/sizeof(block_alloc_dist[0]))



/* TODO remove, for analysis and debugging */
//static int bsize_by_txg[512][NUM_BLOCK_SIZES];
//

struct perf_tier_alloc_tracker {

//    struct bucket {
//
//        /* Total number of buckets */
//        uint64_t nblocks;
//
//        /* Number of free buckets remaining */
//        uint64_t nfree_blocks;
//
//        /* Pointer to the head of the free list */
//        uint64_t free_idx_head;
//
//        /* Size of blocks */
//        uint64_t bsize;
//
//        /* Base address of the blocks */
//        uint64_t addr_base;
//
//        /* List of blocks (The index serves as the slot id.  The entry contains
//         * the next number or -1 if occupied */
//        uint64_t blocks[0] __attribute__((aligned));
//
//    } *buckets[NUM_BLOCK_SIZES];


    /* Locking and signaling mechanisms */
    kmutex_t perf_tier_allocs_lock;
    kcondvar_t perf_tier_allocs_cv;
    uint64_t ndata_range_changes;

    avl_tree_t address_tree;

    krwlock_t lock;
};



struct data_range {

    /* Pointer to the top private data structure */
    tiering_map_t *tiering_map;

    /* Offset of the data range on capacity tier */
    uint64_t cap_offset;

    /* Size of the data range on capacity tier */
    uint64_t cap_size;

    /* Offset of data range on performance tier */
    uint64_t perf_offset;

    /* Size of data range on performance tier */
    uint64_t perf_size;

    /* Data buffer */
    //abd_t *databuf;

    zio_t *orig_zio;


    /* Node pointers */
    list_node_t migration_queue_node;

    /* TODO this is a temporary address resolution until something
     * better is done */
    uint64_t addr_collision;
    avl_node_t address_tree_link;


    zfs_refcount_t refcount;

    //zio_t *orig_zio;

    blkptr_t curr_blkptr;
    blkptr_t dest_blkptr;



    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker;
    uint64_t num_evict_rounds;
};


struct tier {
    int (*init)(tier_t *, void *);
    void (*fini)(tier_t *);
    void (*allocate_space)(tier_t *);
    void (*deallocate_space)(tier_t *, u_int64_t offset, u_int64_t size);
    int  (*write)(tier_t *tier, void *, uint64_t, uint64_t, dmu_tx_callback_func_t, void *);
    int  (*read)(tier_t *, void *, uint64_t, uint64_t);
    void (*stats)(tier_t *);
};

struct spa_tier {

    /* Composition inheritance of methods */
    struct tier t;

    /** Private members **/
    
    /* Object set for the tier */
    objset_t *objset; 

    /* Root dnode */
    dnode_t *root_dnode;

    uint64_t tiering_cache_id;

    /* SPA for the tier */
    /* TODO remove SPA */
    spa_t *spa;

    /* last time (txg) tier was evicted */
    uint64_t num_evict_rounds;

    uint64_t base_txg;

    // zio_t *root;
};



static int
spa_tier_init(tier_t *tier, void *arg) {

    struct spa_tier *spa_tier = (struct spa_tier *) tier;
    char *dataset_name = (char *) arg;
    objset_t *objset = NULL;
    uint64_t version;
    uint64_t root_id;
    uint64_t tiering_cache_id;
    dnode_t *root_dn = NULL;
    int rc = 0;

    /* Open the tier as a ZFS DMU objectset (TODO Might try another ZVOL or something else later) */
    rc = dmu_objset_own(dataset_name, DMU_OST_ZFS, B_FALSE, B_TRUE, spa_tier, &objset);
    
    /* Error opening the object set */
    if(rc != 0) {
        zfs_dbgmsg("Error opening dataset %s at %s@%d", dataset_name, __FUNCTION__, __LINE__);
        goto ERROR;
    }

    /* Check that the objset is okay by getting the ZPL version */
    rc = zap_lookup(objset, MASTER_NODE_OBJ, ZPL_VERSION_STR, sizeof(version), 1, &version);

    if(rc != 0) {
        zfs_dbgmsg("Error opening dataset %s at %s@%d", dataset_name, __FUNCTION__, __LINE__);
        goto ERROR;
    }

    /* Get the root object for the objset */
    rc = zap_lookup(objset, MASTER_NODE_OBJ, ZFS_ROOT_OBJ, sizeof(root_id), 1, &root_id);

    if(rc != 0) {
        zfs_dbgmsg("Error opening dataset %s at %s@%d", dataset_name, __FUNCTION__, __LINE__);
        goto ERROR;
    }

    /* Get the root dnode */
    rc =  dnode_hold(objset, root_id, spa_tier, &root_dn);

    if(rc != 0) {
        zfs_dbgmsg("Error opening dataset %s at %s@%d", dataset_name, __FUNCTION__, __LINE__);
        goto ERROR;
    }



    /* Open the data caching file */
    

    /* Search fo the object id of the tiering cache */
    rc = zap_lookup(objset, root_id, "TIERING_CACHE", sizeof(tiering_cache_id), 1, &tiering_cache_id);


    switch(rc) {

        /* Found, so it already exists */
        case 0:
            break;

        /* Not found so need to create it, by creating a transaction, holding the zap for the root, 
         * creating the file cache dmu object, add the it's id to the zap and then commit */
        case ENOENT: {

                 
                zfs_dbgmsg("Creating cache entry at %s@%d", __FUNCTION__, __LINE__);
        
                /* New transaction for this tier */
                dmu_tx_t *tx = dmu_tx_create(objset);
                
                /* Hold the root's zap for the transaction */
                dmu_tx_hold_zap(tx, root_id, B_TRUE, NULL);

                /* Assign the transaction to a group */
                rc = dmu_tx_assign(tx, TXG_NOWAIT);

                if(rc != 0) {
                    zfs_dbgmsg("Error creating tiering cache file, error = %d at %s@%d", rc, __FUNCTION__, __LINE__);
                    dmu_tx_abort(tx);
                    goto ERROR;
                }
                
                /* TODO Replace this with a programmtical and settable approach */
                #define TIERING_BLOCK_SIZE 512

                /* Create a dmu file object. TODO: this function appears to never be checked in the code for
                   return value */
                tiering_cache_id = dmu_object_alloc(objset, DMU_OT_PLAIN_FILE_CONTENTS, TIERING_BLOCK_SIZE, DMU_OT_SA,  0, tx);

                zfs_dbgmsg("Tiering cache id is %d at %s@%d", tiering_cache_id, __FUNCTION__, __LINE__);
                
                /* Add the tiering cache id file to the zap for later retrieval */
                rc = zap_add(objset, root_id, "TIERING_CACHE", sizeof(tiering_cache_id), 1, &tiering_cache_id, tx);

                if(rc != 0) {
                    zfs_dbgmsg("Error creating tiering cache file, error = %d at %s@%d", rc, __FUNCTION__, __LINE__);
                    dmu_tx_abort(tx);
                    goto ERROR;
                }


                /* Get the transaction handle */
                uint64_t txg = dmu_tx_get_txg(tx);

                zfs_dbgmsg("Txg is %d at %s@%d", txg, __FUNCTION__, __LINE__);

                /* Commit */
                dmu_tx_commit(tx);                

                /* Wait for commit to finish */
                txg_wait_synced(dmu_objset_pool(objset), txg);


                
                zfs_dbgmsg("Creating cache entry at %s@%d", __FUNCTION__, __LINE__);
            }

            break;

        /* Error */
        default:
            zfs_dbgmsg("Error opening dataset %s, error = %d at %s@%d", dataset_name, rc, __FUNCTION__, __LINE__);
            goto ERROR;
    }


    ASSERT(objset != NULL);
    ASSERT(root_dn != NULL);


    
    /* Assign the objset and root dnode (I have holds on both of them */
    spa_tier->objset = objset;
    spa_tier->root_dnode = root_dn;
    spa_tier->tiering_cache_id = tiering_cache_id;

    return 0;

    /* Error handler */
    ERROR:

        if(objset != NULL) {

            if(root_dn != NULL) {
                dnode_rele(root_dn, spa_tier);
            }

            dmu_objset_disown(objset, B_TRUE, spa_tier);
        }

        return SET_ERROR(rc);
}   

static void
spa_tier_fini(tier_t *tier) {

    struct spa_tier *spa_tier = (struct spa_tier *) tier;

    if(spa_tier->objset != NULL) {
        dmu_objset_disown(spa_tier->objset, B_FALSE, spa_tier);
    }

    if(spa_tier->root_dnode != NULL) {
        dnode_rele(spa_tier->root_dnode, spa_tier);
    }
}


struct spa_tier_private {
    struct spa_tier *spa_tier;
    blkptr_t bp;
    zio_done_func_t *caller_cb;
    void *caller_private;
};


static void 
spa_tier_deallocate_space(tier_t *tier, u_int64_t offset, u_int64_t size) {

    struct spa_tier *spa_tier = (struct spa_tier *) tier;

    uint64_t refdbytesp, availbytesp, usedobjsp,availobjsp;

    dmu_objset_space(spa_tier->objset, &refdbytesp, &availbytesp, &usedobjsp, 
                     &availobjsp);

    zfs_dbgmsg("Tier free at %ld of size %ld at %s@%d, bytes used = %ld space avaliable = %ld objs used = %ld avail objects = %ld", 
    offset, size, __FUNCTION__, __LINE__, refdbytesp, availbytesp, usedobjsp,availobjsp);
     

    /* New transaction for this tier */
    dmu_tx_t *tx = dmu_tx_create(spa_tier->objset);

    /* Hold this section of the file in the transaction */
    dmu_tx_hold_free(tx, spa_tier->tiering_cache_id, offset, size);

    /* Mark this transaction as freeing space */
    dmu_tx_mark_netfree(tx);

    /* Assign the transaction to a group */
    int rc = dmu_tx_assign(tx, TXG_WAIT);
    
    zfs_dbgmsg("Tier free at %ld of size %ld at %s@%d, rc == %d", 
                offset, size, __FUNCTION__, __LINE__, rc);
    ASSERT(rc == 0);

    /* Frees this section of the object */
    rc = dmu_free_range(spa_tier->objset, spa_tier->tiering_cache_id, 
                            offset, size, tx);

    ASSERT(rc == 0);

    /* Get the transaction handle */
    uint64_t txg = dmu_tx_get_txg(tx);

    /* Commit */
    dmu_tx_commit(tx);   


    /* TODO This forces the transaction group to proceed but creates
     * a lot of transaction groups.  Probably need to change this
     * so this isn't required or move it to other calls when this needs to be 
     * done */
    txg_wait_synced(dmu_objset_pool(spa_tier->objset), txg);

    dmu_objset_space(spa_tier->objset, &refdbytesp, &availbytesp, &usedobjsp, 
                     &availobjsp);

    zfs_dbgmsg("Done Tier free at %ld of size %ld at %s@%d, bytes used = %ld space avaliable = %ld objs used = %ld avail objects = %ld", 
    offset, size, __FUNCTION__, __LINE__, refdbytesp, availbytesp, usedobjsp,availobjsp);
     
}

// static void
// //spa_tier_write_done(zio_t *zio) {
// spa_tier_write_done(void *data, int error) {

//     /* TODO remove print statement */
//     zfs_dbgmsg("Inside of %s io_error = %d", __FUNCTION__, error);

//     struct spa_tier_private *private = data;

//     zio->io_private = private->caller_private;
//     private->caller_cb(zio);

//     //spa_config_exit(private->spa_tier->spa, SCL_ALL, private->spa_tier);

//     kmem_free(private, sizeof(*private));
// }



//static zio_t *
static int
spa_tier_write(tier_t *tier, void *buf, uint64_t offset, uint64_t size, 
               dmu_tx_callback_func_t cb, void *cb_data) {

    struct spa_tier *spa_tier = (struct spa_tier *) tier;

    // uint64_t adjusted_txg = spa_tier->spa->spa_dsl_pool->dp_tx.tx_open_txg; //spa_tier->base_txg + txg;
    // zio_prop_t zp;

    /* Create and initialize the private data */
    // struct spa_tier_private *private = kmem_alloc(sizeof(struct spa_tier_private), KM_SLEEP);

    // private->spa_tier = spa_tier;
    // private->caller_cb = cb;
    // private->caller_private = caller_private;
    // BP_ZERO(&private->bp);


    /* TODO May want to cache this somewhere */
    /* Setup the properties for the zio */
    // zp.zp_checksum = ZIO_CHECKSUM_OFF;  /* TODO set checksums if checksums are wanted */
    // zp.zp_compress = ZIO_COMPRESS_OFF;  /* TODO set compress if compress is wanted */
    // zp.zp_complevel = ZIO_COMPLEVEL_DEFAULT;
    // zp.zp_type = DMU_OT_NONE;
    // zp.zp_level = 0;    /* TODO find out what zp_level specifies, raid maybe? */
    // zp.zp_copies = 1;  /* TODO Only one data copy, might what to match what the SPA does */
    // zp.zp_dedup = B_FALSE;
    // zp.zp_dedup_verify = B_FALSE;
    // zp.zp_nopwrite = B_FALSE;
    // zp.zp_encrypt = B_FALSE; /* TODO set encryption if encryption is wanted */
    // zp.zp_byteorder = ZFS_HOST_BYTEORDER;
    // bzero(zp.zp_salt, ZIO_DATA_SALT_LEN);
    // bzero(zp.zp_iv, ZIO_DATA_IV_LEN);
    // bzero(zp.zp_mac, ZIO_DATA_MAC_LEN);

    /* Lock the spa config, unlock will happen in the callback */
    // spa_config_enter(spa_tier->spa, SCL_ALL, spa_tier, RW_READER);

    
    /* Create the ZIO */
    // zio_t *zio = zio_write(NULL, 
    //                        spa_tier->spa,
    //                        adjusted_txg,
    //                        &private->bp,
    //                        data,
    //                        size,
    //                        size,
    //                        &zp,
    //                        NULL, NULL, NULL,
    //                        spa_tier_write_done,
    //                        private,
    //                        ZIO_PRIORITY_ASYNC_WRITE,
    //                        ZIO_FLAG_IO_ALLOCATING|ZIO_FLAG_CANFAIL,
    //                        NULL);


    // ASSERT(zio != NULL);


    uint64_t refdbytesp, availbytesp, usedobjsp,availobjsp;

    dmu_objset_space(spa_tier->objset, &refdbytesp, &availbytesp, &usedobjsp, 
                     &availobjsp);
    zfs_dbgmsg("Tier write at %ld of size %ld at %s@%d, bytes used = %ld space avaliable = %ld objs used = %ld avail objects = %ld", 
    offset, size, __FUNCTION__, __LINE__, refdbytesp, availbytesp, usedobjsp,availobjsp);
     
    /* New transaction for this tier */
    dmu_tx_t *tx = dmu_tx_create(spa_tier->objset);

    /* Hold this section of the tier file */
    dmu_tx_hold_write(tx, spa_tier->tiering_cache_id, offset, size);

    /* Assign the transaction to a group */
    int rc = dmu_tx_assign(tx, TXG_WAIT);

    if(rc != 0) {
        zfs_dbgmsg("Error creating transaction, error = %d at %s@%d", rc, __FUNCTION__, __LINE__);
        dmu_tx_abort(tx);
        return SET_ERROR(rc);
    }
 
    /* Issue the write via the dmu */
    dmu_write(spa_tier->objset, spa_tier->tiering_cache_id, offset, size, buf, tx);
    
    /* Register the finished call back */
    dmu_tx_callback_register(tx, cb, cb_data);

    /* Get the transaction handle */
    uint64_t txg = dmu_tx_get_txg(tx);

    /* Commit */
    dmu_tx_commit(tx);   

    /* TODO This forces the transaction group to proceed but creates
     * a lot of transaction groups.  Probably need to change this
     * so this isn't required or move it to other calls when this needs to be 
     * done */
    txg_wait_synced(dmu_objset_pool(spa_tier->objset), txg);


    dmu_objset_space(spa_tier->objset, &refdbytesp, &availbytesp, &usedobjsp, 
                     &availobjsp);
    zfs_dbgmsg("Done Tier write at %ld of size %ld at %s@%d, bytes used = %ld space avaliable = %ld objs used = %ld avail objects = %ld", 
    offset, size, __FUNCTION__, __LINE__, refdbytesp, availbytesp, usedobjsp,availobjsp);
     
    //return zio;

    return 0;
}

// static void
// spa_tier_read_done(zio_t *zio) {

//     /* TODO remove print statement */
//     zfs_dbgmsg("%s inside", __FUNCTION__);

//     struct spa_tier_private *private = zio->io_private;

//     /* TODO handle errors */
//     ASSERT(zio->io_error == 0);

//     /* Issue the original callback */
//     zio->io_private = private->caller_private;
//     private->caller_cb(zio);

//     spa_config_exit(private->spa_tier->spa, SCL_ALL, private->spa_tier);

//     kmem_free(private, sizeof(*private));
// }

static int
spa_tier_read(tier_t *tier, void *buf, 
              uint64_t offset, uint64_t size) {

    struct spa_tier *spa_tier = (struct spa_tier *) tier;

    // /* Create and initialize the private data */
    // struct spa_tier_private *private = kmem_alloc(sizeof(struct spa_tier_private), KM_SLEEP);

    // private->spa_tier = spa_tier;
    // private->caller_cb = cb;
    // private->caller_private = caller_private;
    // private->bp = *bp;

    // /* Lock the spa config, unlock will happen in the callback */
    // spa_config_enter(spa_tier->spa, SCL_ALL, spa_tier, RW_READER);

    // /* Create the ZIO */
    // zio_t *zio = zio_read(NULL,
    //                       spa_tier->spa,
    //                       bp,
    //                       data,
    //                       size,
    //                       spa_tier_read_done,
    //                       private,
    //                       ZIO_PRIORITY_SYNC_READ,
    //                       ZIO_FLAG_CANFAIL,
    //                       NULL);

    // ASSERT(zio != NULL);

    // return zio;


    zfs_dbgmsg("Tier read at %ld of size %ld at %s@%d", offset, size, __FUNCTION__, __LINE__);
        

    /* New transaction for this tier */
    //dmu_tx_t *tx = dmu_tx_create(spa_tier->objset);

    int rc = dmu_read(spa_tier->objset, spa_tier->tiering_cache_id, offset, size, 
              buf, DMU_READ_NO_PREFETCH);

    if(rc != 0) {
        zfs_dbgmsg("Error reading data, error = %d at %s@%d", rc, __FUNCTION__, __LINE__);
        return SET_ERROR(rc);
    }

    zfs_dbgmsg("Done Tier read at %ld of size %ld at %s@%d", offset, size, __FUNCTION__, __LINE__);
        

    return 0;
}



static tier_t *
allocate_spa_tier(void) {

    struct spa_tier *tier = kmem_alloc(sizeof(struct spa_tier), KM_SLEEP);

    tier->t.init = spa_tier_init;
    tier->t.fini = spa_tier_fini;
    tier->t.allocate_space = NULL;
    tier->t.deallocate_space = spa_tier_deallocate_space;
    tier->t.write = spa_tier_write;
    tier->t.read = spa_tier_read;
    tier->t.stats = NULL;

    tier->spa = NULL;
    tier->objset = NULL;
    tier->root_dnode = NULL;
    tier->num_evict_rounds = 0;
    
    // tier->base_txg = spa_syncing_txg(spa);
    // tier->root = zio_root(spa, NULL, NULL, 0);

    // ASSERT(tier->root != NULL);

    return (tier_t *) tier;
}




struct tiering_map {

    /* My vdev */
    vdev_t *tiering_vdev;

    /* Pointer to the performance tier vdev */
    //vdev_t *performance_vdev;

    tier_t *perf_tier;

    /* Pointer to the capacity tier vdev */
    vdev_t *capacity_vdev;

    /* List of ranges (by offset and length) read to be read from
     * the performance tier (buffer unfilled) */
    list_t from_performance_tier_data_ranges;

    /* List of ranges (by offset and length) ready to go on the capacity tier
     * buffer filled */
    list_t to_capacity_tier_data_ranges;

    /* Number of free migration buffers left */
    uint64_t num_of_free_bufs;

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
data_range_create(tiering_map_t *tiering_map, uint64_t cap_offset,
                  uint64_t cap_size, uint64_t perf_offset,
                  uint64_t perf_size,
                  struct perf_tier_alloc_tracker *perf_tier_alloc_tracker){

    data_range_t *data_range = kmem_alloc(sizeof(data_range_t), KM_SLEEP);

    data_range->tiering_map = tiering_map;
    data_range->cap_offset = cap_offset;
    data_range->cap_size = cap_size;
    data_range->perf_offset = perf_offset;
    data_range->perf_size = perf_size;
    // data_range->databuf = NULL;
    data_range->orig_zio = NULL;
//    memcpy(&data_range->blkptr, zio->io_bp, sizeof(blkptr_t));
    data_range->perf_tier_alloc_tracker = perf_tier_alloc_tracker;

    BP_ZERO(&data_range->curr_blkptr);
    BP_ZERO(&data_range->dest_blkptr);

    /* TODO need to rewrite code to avoid needing the struct spa_tier instead
     * of tier_t */
    struct spa_tier *spa_tier = (struct spa_tier *)tiering_map->perf_tier;
    data_range->num_evict_rounds = spa_tier->num_evict_rounds;

    /* Increment the reference count */
    zfs_refcount_create(&data_range->refcount);
    zfs_refcount_add(&data_range->refcount, FTAG);

    return data_range;
}


static void
data_range_destroy(data_range_t *data_range) __attribute__((unused));
static void
data_range_destroy(data_range_t *data_range) {

    ASSERT(zfs_refcount_count(&data_range->refcount) == 0);

    zfs_refcount_destroy(&data_range->refcount);

    memset(data_range, 0, sizeof(*data_range));
    kmem_free(data_range, sizeof(data_range_t));
}

static void
data_range_reference_change(
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker) {

    mutex_enter(&perf_tier_alloc_tracker->perf_tier_allocs_lock);

    perf_tier_alloc_tracker->ndata_range_changes++;
    cv_broadcast(&perf_tier_alloc_tracker->perf_tier_allocs_cv);

    mutex_exit(&perf_tier_alloc_tracker->perf_tier_allocs_lock);
}

static int
address_compare(const void *v1, const void *v2) {

    const data_range_t *dr1 = v1;
    const data_range_t *dr2 = v2;

    /* Compares the txg group and then if those match the offset on the
     * capacity tier */
    if(dr1->dest_blkptr.blk_birth < dr2->dest_blkptr.blk_birth) {
        return -1;

    }else if(dr1->dest_blkptr.blk_birth > dr2->dest_blkptr.blk_birth) {
        return 1;

    }else {
        if(dr1->cap_offset < dr2->cap_offset) {
            return -1;
        }else if(dr1->cap_offset > dr2->cap_offset) {
            return 1;

        }else {
            zfs_dbgmsg("\t%s collision at dr1 offset: %ld dr2 offset: %ld dr1 id: %ld  dr2 id: %ld", __FUNCTION__,
                       dr1->cap_offset, dr2->cap_offset,
                       dr1->addr_collision, dr2->addr_collision);
            if(dr1->addr_collision < dr2->addr_collision) {
                zfs_dbgmsg("\t%s collision at return -1", __FUNCTION__);
                return -1;
            }else {
                zfs_dbgmsg("\t%s collision at return 1", __FUNCTION__);
                return 1;
            }
        }
    }

    return 0;
}

#if 0
static void
metaslab_test(spa_t *spa) {

    blkptr_t new_bp;
    uint64_t size = 4096;
    int flags = METASLAB_FASTWRITE;
    zio_alloc_list_t io_alloc_list;

    BP_ZERO(&new_bp);
    BP_SET_TYPE(&new_bp, DMU_OTN_UINT8_DATA);
    BP_SET_PSIZE(&new_bp, size);
    BP_SET_LEVEL(&new_bp, 0);



    zfs_dbgmsg("\t%s spa txg = %d", __FUNCTION__, spa_syncing_txg(spa)+1);

    metaslab_trace_init(&io_alloc_list);

    int error = metaslab_alloc(spa, spa_normal_class(spa), size, &new_bp, 1, spa_syncing_txg(spa)+1,
                               NULL, flags, &io_alloc_list, NULL,  0);

    metaslab_trace_fini(&io_alloc_list);



    zfs_dbgmsg("\t%s error = %d", __FUNCTION__, error);


}


static void
vdev_tiering_zio_test_done(zio_t *zio) {
    zfs_dbgmsg("\t%s error = %d", __FUNCTION__, zio->io_error);
}


static void zio_test(spa_t *spa) {

#define DATA "Hello Word"

    blkptr_t new_bp;
    uint64_t psize = SPA_MINBLOCKSIZE;
    uint64_t lsize = SPA_MINBLOCKSIZE;
    uint64_t txg = spa_syncing_txg(spa)+1;
    void *private = NULL;
    zio_prop_t zp;
    abd_t *data;

    zfs_dbgmsg("\t%s size = %d", __FUNCTION__, lsize);

    data = abd_alloc(psize, B_FALSE);
    abd_copy_from_buf(data, data, sizeof(DATA));

    BP_ZERO(&new_bp);

    zp.zp_checksum = ZIO_CHECKSUM_OFF; /* TODO set checksums if checksums are wanted */
    zp.zp_compress = ZIO_COMPRESS_OFF;  /* TODO set compress if compress is wanted */
    zp.zp_complevel = ZIO_COMPLEVEL_DEFAULT;
    zp.zp_type = DMU_OT_NONE;
    zp.zp_level = 0;    /* TODO find out what zp_level specifies, raid maybe? */
    zp.zp_copies = 1;  /* TODO Only one data copy, might what to match what the SPA does */
    zp.zp_dedup = B_FALSE;
    zp.zp_dedup_verify = B_FALSE;
    zp.zp_nopwrite = B_FALSE;
    zp.zp_encrypt = B_FALSE; /* TODO set encryption if encryption is wanted */
    zp.zp_byteorder = ZFS_HOST_BYTEORDER;
    bzero(zp.zp_salt, ZIO_DATA_SALT_LEN);
    bzero(zp.zp_iv, ZIO_DATA_IV_LEN);
    bzero(zp.zp_mac, ZIO_DATA_MAC_LEN);


    zio_t *zio = zio_write(NULL, spa, txg, &new_bp, data,
                           lsize, psize, &zp,
                           NULL, NULL, NULL,
                           vdev_tiering_zio_test_done, private,
                           ZIO_PRIORITY_SYNC_WRITE,
                           ZIO_FLAG_CANFAIL,
                           NULL);


    zio_wait(zio);
}
#endif

static struct perf_tier_alloc_tracker *
allocate_perf_tier_alloc_tracker(tier_t *tier) {

//    uint64_t num_blocks[NUM_BLOCK_SIZES];
//
//
//    num_blocks[NUM_BLOCK_SIZES - 1] = (vd->vdev_psize/
//            block_alloc_dist[NUM_BLOCK_SIZES - 1].size) -
//                    (block_alloc_dist[NUM_BLOCK_SIZES - 1].ratio);
//
//    for(int i=0; i<NUM_BLOCK_SIZES-1; i++) {
//        num_blocks[i] = block_alloc_dist[i].ratio *
//                block_alloc_dist[NUM_BLOCK_SIZES - 1].ratio;
//    }


//    int64_t remaining_size = vd->vdev_psize;
//
//    for(int i=0; i<NUM_BLOCK_SIZES; i++) {
//        ASSERT(num_blocks[i] > 0);
//
//        remaining_size -= num_blocks[i] * block_alloc_dist[i].size;
//
//        ASSERT(remaining_size >= 0);
//    }


//    zfs_dbgmsg("Inside of %s: space available %lld remaining_space %lld",
//               __FUNCTION__, vd->vdev_psize, remaining_size);
//
//    for(int i=0; i<NUM_BLOCK_SIZES; i++) {
//        zfs_dbgmsg("\tBucket size: %d num: %d", block_alloc_dist[i].size,
//                   num_blocks[i]);
//    }

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

        perf_tier_alloc_tracker->ndata_range_changes = 0;

//        uint64_t addr_base = 0;

//        for(int i=NUM_BLOCK_SIZES-1; i>-1; i--) {
//
//            struct bucket *bucket = kmem_zalloc(
//                    sizeof(struct bucket) + num_blocks[i]*sizeof(uint64_t), KM_SLEEP);
//
//            if(perf_tier_alloc_tracker != NULL) {
//                bucket->nblocks = num_blocks[i];
//                bucket->nfree_blocks = num_blocks[i];
//                bucket->bsize = block_alloc_dist[i].size;
//
//
//                /* Each bucket entry points to the next free */
//                for(uint64_t idx=0; idx<num_blocks[i]-1; idx++) {
//                    bucket->blocks[idx] = idx+1;
//                }
//
//                /* Point the free list to the start of the buckets */
//                bucket->free_idx_head = 0;
//
//                /* The last bucket points to the sentinel value */
//                bucket->blocks[num_blocks[i]-1] = (uint64_t) -1;
//
//                /* Address of the start of the blocks on the vdev */
//                bucket->addr_base = addr_base;
//                addr_base += num_blocks[i] * block_alloc_dist[i].size;
//
//                perf_tier_alloc_tracker->buckets[i] = bucket;
//
//            } else {
//                goto ERROR_HANDLER;
//            }
//        }
    }

    /* Return the perf tier allocs */
    return perf_tier_alloc_tracker;

//    ERROR_HANDLER:
//
//
//        if(perf_tier_alloc_tracker != NULL) {
//
//            for(int i=0; i<NUM_BLOCK_SIZES; i++) {
//
//                if(perf_tier_alloc_tracker->buckets[i] != NULL) {
//                    kmem_free(perf_tier_alloc_tracker->buckets[i],
//                              sizeof(struct bucket) + num_blocks[i] * sizeof(uint64_t));
//                }
//            }
//
//            /* Free the avl tree */
//            avl_destroy(&perf_tier_alloc_tracker->address_tree);
//
//            /* Free the rw lock */
//            rw_destroy(&perf_tier_alloc_tracker->lock);
//
//
//            mutex_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_lock));
//            cv_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_cv));
//
//
//            kmem_free(perf_tier_alloc_tracker, sizeof(struct perf_tier_alloc_tracker));
//        }
//
//    return NULL;
}


static void
free_performance_tier_alloc_tracker(struct perf_tier_alloc_tracker *perf_tier_alloc_tracker) {


    /* Iterate through the nodes of the avl address tree and release the
    * resources tied to the nodes */
    for(data_range_t *cookie=NULL, *data_range=avl_destroy_nodes(&perf_tier_alloc_tracker->address_tree, (void **)&cookie);
        data_range != NULL;
        data_range=avl_destroy_nodes(&perf_tier_alloc_tracker->address_tree, (void **)&cookie)) {

        // if(data_range->databuf != NULL) {
        //     abd_free(data_range->databuf);
        // }

        // data_range->databuf = NULL;

        kmem_free(data_range, sizeof(*data_range));
    }

    /* Free the avl tree */
    avl_destroy(&perf_tier_alloc_tracker->address_tree);

    /* Free the rw lock */
    rw_destroy(&perf_tier_alloc_tracker->lock);


    mutex_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_lock));
    cv_destroy(&(perf_tier_alloc_tracker->perf_tier_allocs_cv));

//    for(int i=0; i<NUM_BLOCK_SIZES; i++) {
//
//        kmem_free(perf_tier_alloc_tracker->buckets[i],
//                  sizeof(struct bucket) + perf_tier_alloc_tracker->buckets[i]->nblocks*sizeof(uint64_t));
//    }


    kmem_free(perf_tier_alloc_tracker,
              sizeof(struct perf_tier_alloc_tracker));

}


//static struct bucket *
//performance_tier_alloc_tracker_find_bucket(
//        struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
//        uint64_t size) {
//
//    struct bucket *bucket = NULL;
//
//    /* Get the bucket index for the size requested */
//    for(int i=0; i<NUM_BLOCK_SIZES && bucket == NULL; i++) {
//        if(size <= block_alloc_dist[i].size){
//            bucket = perf_tier_alloc_tracker->buckets[i];
//        }
//    }
//
//    return bucket;
//}


#if 0
static void
evict_done(zio_t *zio) {

    /* TODO remove print statement */
    zfs_dbgmsg("Eviction done IO_error = %d free from %ld of size %ld",
               zio->io_error, zio->io_offset, zio->io_size);
}
#endif

static void
performance_tier_alloc_tracker_evict_blocks(
        struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
        tier_t *tier,
        uint64_t size,
        //uint64_t curr_txg,
        uint64_t *last_evict_round) {

    ASSERT(size <= PERF_TIER_ALLOC_TRACKER_RECORD_SIZE);

    int nevicts = 0;
    uint64_t amt_freed = 0;
    // zio_t *root_zio = NULL;
    uint64_t ndata_range_changes;

    /* TODO need to rewrite code to avoid needing the struct spa_tier instead
     * of tier_t */
    struct spa_tier *spa_tier = (struct spa_tier *)tier;

    mutex_enter(&perf_tier_alloc_tracker->perf_tier_allocs_lock);

    ndata_range_changes = perf_tier_alloc_tracker->ndata_range_changes;

//    uint64_t spa_txg = -1;
//    dmu_tx_t *tx = NULL;
//    dsl_pool_t *pool = spa_get_dsl(spa_tier->spa);
//    objset_t *os = pool->dp_meta_objset;

    /* Wait until there is free space */
    while(*last_evict_round == spa_tier->num_evict_rounds &&
          nevicts == 0 &&
          size > amt_freed) {

        rw_enter(&perf_tier_alloc_tracker->lock, RW_WRITER);

        /* Get the first data range (sorted by txg, offset) */
        data_range_t *evict_candidate = avl_first(
                &perf_tier_alloc_tracker->address_tree);

        zfs_dbgmsg("evict_candidate = %p", evict_candidate);

        if(evict_candidate != NULL) {

            /* Get the transaction group to free */
            uint64_t txg = evict_candidate->dest_blkptr.blk_birth;

            zfs_dbgmsg(
                    "next txg = %ld refcount = %ld collision = %d offset %lld size %lld",
                    evict_candidate->dest_blkptr.blk_birth,
                    zfs_refcount_count(&evict_candidate->refcount),
                    evict_candidate->addr_collision,
                    evict_candidate->cap_offset,
                    evict_candidate->cap_size);

            do {

                data_range_t *next_dr = AVL_NEXT(
                        &perf_tier_alloc_tracker->address_tree,
                        evict_candidate);

                /* Get the current reference count */
                int64_t refcount = zfs_refcount_count(
                        &evict_candidate->refcount);

                ASSERT(refcount > 0);

                /* If there is only one reference count, then we can release this one */
                if(refcount == 1) {

                    avl_remove(&perf_tier_alloc_tracker->address_tree,
                               evict_candidate);


#if 0
                    zio_t * free_zio = zio_free_sync(NULL,
                                                     spa_tier->spa,
                                                     spa_syncing_txg(spa_tier->spa),
                                                     &evict_candidate->curr_blkptr,
                                                     0);

                    if (BP_IS_EMBEDDED(&evict_candidate->curr_blkptr)) {
                        zfs_dbgmsg("BP is embedded");
                    }

                    if(free_zio != NULL) {

                        zfs_dbgmsg("Real free_zio %p", free_zio);
                        if(root_zio == NULL) {
                            root_zio = zio_root(spa_tier->spa, evict_done, NULL, 0);
                            ASSERT(root_zio!=NULL);

                            zio_add_child(root_zio, free_zio);
                        }

                        zio_nowait(free_zio);
                    }

#endif

                    tier->deallocate_space(tier, 
                                           evict_candidate->perf_offset, 
                                           evict_candidate->perf_size);

                    ASSERT(zfs_refcount_count(&evict_candidate->refcount) == 1);
                    zfs_refcount_remove(&evict_candidate->refcount,
                                        &perf_tier_alloc_tracker->address_tree);

                    nevicts++;
                    amt_freed += evict_candidate->perf_size;
                    perf_tier_alloc_tracker->ndata_range_changes++;

                    /* TODO remove print statement */
                    zfs_dbgmsg("Evicting from %ld at %ld length: %ld",
                               evict_candidate->dest_blkptr.blk_birth,
                               evict_candidate->cap_offset, evict_candidate->cap_size );

                    data_range_destroy(evict_candidate);
                }

                evict_candidate = next_dr;

                if(evict_candidate != NULL) {
                    zfs_dbgmsg(
                            "next txg = %ld refcount = %ld collision = %d offset %lld size %lld",
                            evict_candidate->dest_blkptr.blk_birth,
                            zfs_refcount_count(&evict_candidate->refcount),
                            evict_candidate->addr_collision,
                            evict_candidate->cap_offset,
                            evict_candidate->cap_size);
                }

            } while(evict_candidate != NULL &&
                    evict_candidate->dest_blkptr.blk_birth==txg);
        }

        rw_exit(&perf_tier_alloc_tracker->lock);

        /* If there were no evictions then wait until some
         * data ranges are finished migrations */
        if(nevicts == 0) {

            /* TODO remove print statement */
            zfs_dbgmsg("Waiting on change");

            while(ndata_range_changes ==
                    perf_tier_alloc_tracker->ndata_range_changes) {
                cv_wait(&perf_tier_alloc_tracker->perf_tier_allocs_cv,
                        &perf_tier_alloc_tracker->perf_tier_allocs_lock);
            }

            zfs_dbgmsg("Awake");

        } else {

            // if(root_zio != NULL) {
            //     zfs_dbgmsg("Waiting on zio to finish");

            //     zio_wait(root_zio);
            //     zfs_dbgmsg("zio has finished");
            //     zio_execute(root_zio);
            // }

            spa_tier->num_evict_rounds++;

            zfs_dbgmsg("Broadcasting");

            cv_broadcast(&perf_tier_alloc_tracker->perf_tier_allocs_cv);
        }
    }

    *last_evict_round = spa_tier->num_evict_rounds;

//    spa_tier->spa->spa_dsl_pool->dp_tx.tx_sync_thread = curthread;
    //spa_sync(spa_tier->spa, spa_syncing_txg(spa_tier->spa));

//    zfs_dbgmsg("Syncing at %ld",
//               spa_tier->spa->spa_dsl_pool->dp_tx.tx_synced_txg);
//
//    spa_tier->spa->spa_dsl_pool->dp_tx.tx_synced_txg = 6;
//    spa_tier->spa->spa_syncing_txg =
//            spa_tier->spa->spa_dsl_pool->dp_tx.tx_synced_txg + 1;

    // spa_tier->spa->spa_dsl_pool->dp_tx.tx_sync_thread = curthread;

    // for(int i=0; i<spa_tier->spa->spa_root_vdev->vdev_children; i++) {
    //     vdev_sync(spa_tier->spa->spa_root_vdev->vdev_child[i],
    //               spa_tier->spa->spa_syncing_txg); 
    // }

//    metaslab_sync();
    //metaslab_sync_done(spa_tier->spa->spa_normal_class->)
    //vdev_sync(spa_tier->spa->spa_root_vdev, spa_syncing_txg(spa_tier ->spa));
    mutex_exit(&perf_tier_alloc_tracker->perf_tier_allocs_lock);
}

#if 0
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
#endif



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
    abd_free(zio->io_abd);
    //data_range->databuf = NULL;

    mutex_enter(lock);
    tiering_map->num_of_free_bufs++;
    cv_signal(cv);
    mutex_exit(lock);


    /* Drop the reference count on the data range and if count is
     * zero release it */
    ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker = 
        data_range->perf_tier_alloc_tracker;
    zfs_refcount_remove(&data_range->refcount, FTAG);
    data_range_reference_change(perf_tier_alloc_tracker);

    
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

static int
migration_issue_reads(tiering_map_t *tiering_map, list_t *list) {

    /* TODO should pass tier directly */
    tier_t *tier = tiering_map->perf_tier;
   

    /* Create the parent zio */
    zio_t *parent_zio = zio_root(tiering_map->capacity_vdev->vdev_spa,
                                 NULL, NULL, 0);
    

    /* Process the data ranges that need to be migration */
    for(data_range_t *data_range = list_remove_tail(list);
        data_range != NULL;
        data_range = list_remove_tail(list)) {

        zfs_refcount_transfer_ownership(&data_range->refcount, list, FTAG);

        /* TODO improve the allocation of data buffers */
        /* Create the data buffer */
        /*data_range->databuf*/ abd_t *buf = abd_alloc_linear(data_range->cap_size, B_FALSE);

        
        /* Read in the data from the performance tier */
    //    zio_t *zio = zio_read_phys(parent_zio,
    //                               tiering_map->performance_vdev->vdev_child[0],
    //                               data_range->perf_offset,
    //                               data_range->perf_size,
    //                               data_range->databuf,
    //                               ZIO_CHECKSUM_OFF,
    //                               vdev_tiering_performance_read_migration_child_done,
    //                               data_range,
    //                               ZIO_PRIORITY_ASYNC_READ,
    //                               ZIO_FLAG_CANFAIL,
    //                               B_FALSE);


        // zio_t *zio = tier->read(tier,
        //                         &data_range->curr_blkptr,
        //                         data_range->databuf,
        //                         data_range->perf_size,
        //                         vdev_tiering_performance_read_migration_child_done,
        //                         data_range);


        // zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, zio);

        // zio_add_child(parent_zio, zio);
        // zio_nowait(zio);


        ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
        ASSERT(data_range->perf_size == data_range->cap_size);
        
        int rc = tier->read(tier, /*data_range->databuf*/abd_to_buf(buf), 
                            data_range->perf_offset, 
                            data_range->perf_size);

        /* TODO handle error */
        ASSERT(rc == 0);

    
        zio_t *zio = zio_vdev_child_io(
                                parent_zio,
                                &data_range->dest_blkptr,
                                tiering_map->capacity_vdev,
                                data_range->cap_offset,
                                buf,
                                data_range->cap_size,
                                ZIO_TYPE_WRITE,
                                ZIO_PRIORITY_ASYNC_WRITE,
                                0,
                                vdev_tiering_performance_write_migration_child_done,
                                data_range);


        zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, zio);

        zio_nowait(zio);

        ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
    }


    /* Wait on the transfers to complete */
    zio_wait(parent_zio);

    return 0;
}

// static int
// migration_issue_writes(tiering_map_t *tiering_map, list_t *list) {

//     /* If the parent doesn't already exist then create it */
//     // if(parent_zio == NULL) {
//     //     parent_zio = zio_root(tiering_map->capacity_vdev->vdev_spa,
//     //                           NULL, NULL, 0);
//     //     parent_zio->io_txg = 0;
//     // }

//     /* Process the data ranges that need to be migration */
//     for(data_range_t *data_range = list_remove_tail(list);
//         data_range != NULL;
//         data_range = list_remove_tail(list)) {

//         zfs_refcount_transfer_ownership(&data_range->refcount, list, FTAG);

//         /* Write the data from the capacity tier */
//         zio_t *zio = zio_vdev_child_io(parent_zio,
//                                       &data_range->dest_blkptr,
//                                       tiering_map->capacity_vdev,
//                                       data_range->cap_offset,
//                                       data_range->databuf,
//                                       data_range->cap_size,
//                                       ZIO_TYPE_WRITE,
//                                       ZIO_PRIORITY_ASYNC_WRITE,
//                                       0,
//                                       vdev_tiering_performance_write_migration_child_done,
//                                       data_range);

//         zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, zio);

//         zio_nowait(zio);
//     }

//     return parent_zio;
// }

#define migration_thread_sleep_interval 20
#define MAX_BUFS_PER_ROUND 2

static void migration_thread(void *arg) {

    tiering_map_t *tiering_map = arg;
    list_t from_performance_tier_data_ranges;
    list_t to_capacity_tier_data_ranges;
    //zio_t *parent_zio = NULL;


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

            ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
        }


        /* There are data ranges that are ready to be transferred to the
        * capacity tier */
        // for (data_range_t *data_range = list_remove_tail(
        //         &tiering_map->to_capacity_tier_data_ranges);
        //      data_range!=NULL;
        //      data_range = list_remove_tail(
        //              &tiering_map->to_capacity_tier_data_ranges)) {

        //     list_insert_head(&to_capacity_tier_data_ranges, data_range);

        //     zfs_refcount_transfer_ownership(&data_range->refcount,
        //                                     &tiering_map->to_capacity_tier_data_ranges,
        //                                     &to_capacity_tier_data_ranges);

        // }

        /* Release the mutex since the working list doesn't need protection. */
        mutex_exit(&(tiering_map->tiering_migration_thr_lock));


        /* Lock the spa and create a parent zio that will unlock it on
         * completion of the io operations */
        //spa_config_enter(tiering_map->performance_vdev->vdev_spa, SCL_ALL, FTAG, RW_READER);
        spa_config_enter(tiering_map->capacity_vdev->vdev_spa, SCL_ALL, FTAG, RW_READER);


        if(list_is_empty(&from_performance_tier_data_ranges) == B_FALSE) {

            int rc = migration_issue_reads(tiering_map,
                                           &from_performance_tier_data_ranges);
        
            /* TODO handle error */
            ASSERT(rc == 0);
        }

        // if(list_is_empty(&to_capacity_tier_data_ranges) == B_FALSE) {

        //     parent_zio = migration_issue_writes(parent_zio,
        //                                         tiering_map,
        //                                         &to_capacity_tier_data_ranges);
        // }


        //zio_wait(parent_zio);

        //delay(SEC_TO_TICK(10));

        //spa_config_exit(tiering_map->performance_vdev->vdev_spa, SCL_ALL, FTAG);
        spa_config_exit(tiering_map->capacity_vdev->vdev_spa, SCL_ALL, FTAG);

        /* TODO need to know when to free the parent and children zios */
        // parent_zio = NULL;

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
        blkptr_t *bp, uint64_t io_offset, uint64_t io_size) {

    data_range_t search = {
            .dest_blkptr = *bp,
            .cap_offset = io_offset,
            .addr_collision = UINT_MAX
    };

    avl_index_t where;

    rw_enter(&perf_tier_alloc_tracker->lock, RW_READER);

    /* Find the mapping for the blkptr and offset */
    data_range_t *data_range = avl_find(&perf_tier_alloc_tracker->address_tree, &search, &where);

    /* TODO with addr collision this should be true remove once new implementation
     * is in place. */
    ASSERT(data_range  == NULL);

    /* Not found so find the next lowest entry */
    if(data_range == NULL) {
        data_range = avl_nearest(&perf_tier_alloc_tracker->address_tree, where, AVL_BEFORE);
    }
    
    /* TODO this check on the range may not be necessary or it may be necessary
     * to adjust the offsets on if they don't match so that the caller
     * will know how to adjust the I/O call offset correctly */
    /* Check the returned entry is valid and if not reset to NULL */
    if(data_range != NULL &&
            (data_range->cap_offset == io_offset &&
            data_range->cap_size == io_size)) {

        ASSERT(zfs_refcount_count(&data_range->refcount) > 0);

        /* Increment the reference count on the data range before returning it */
        zfs_refcount_add(&data_range->refcount, FTAG);

    } else {
        data_range = NULL;
    }


    rw_exit(&perf_tier_alloc_tracker->lock);

    return data_range;
}


static void
performance_tier_alloc_tracker_add_mapping(struct perf_tier_alloc_tracker *perf_tier_alloc_tracker,
                                           data_range_t *data_range)
{
    avl_index_t where;

    /* TODO this is temporary until fix addr collision is fixed */
    data_range->addr_collision = UINT_MAX;

    rw_enter(&perf_tier_alloc_tracker->lock, RW_WRITER);

    /* Test if the original data exists */
    data_range_t *prev_data_range = avl_find(&perf_tier_alloc_tracker->address_tree,
                                             data_range,
                                             &where);

    /* TODO with addr collision this should be true remove once new implementation
     * is in place. */
    ASSERT(prev_data_range  == NULL);

    if(prev_data_range == NULL) {

        /* TODO this check on the range may not be necessary or it may be necessary
         * to adjust the offsets on if they don't match so that the caller
         * will know how to adjust the I/O call offset correctly */
        /* Check the returned entry is valid and if not reset to NULL */
        prev_data_range = avl_nearest(&perf_tier_alloc_tracker->address_tree, where, AVL_BEFORE);

        /* Overlapping range so set the final addr collision to one more than
         * present */
        if((prev_data_range != NULL) &&
           (prev_data_range->cap_offset == data_range->cap_offset) &&
           (prev_data_range->cap_size == data_range->cap_size)) {

            data_range->addr_collision = prev_data_range->addr_collision+1;

        /* No collision so just set addr collision to 0 */
        } else {
            data_range->addr_collision = 0;
        }

        avl_insert(&perf_tier_alloc_tracker->address_tree, data_range, where);
    }

    zfs_refcount_add(&data_range->refcount, &perf_tier_alloc_tracker->address_tree);

    rw_exit(&perf_tier_alloc_tracker->lock);
}




static tiering_map_t *
vdev_tiering_map_init(vdev_t *my_vdev, tier_t *perf_tier, vdev_t *slow_tier) {

    /* Allocate and initialize the perf tier allocation tracker */
    struct perf_tier_alloc_tracker *perf_tier_alloc_tracker =
            allocate_perf_tier_alloc_tracker(perf_tier);

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
        tiering_map->perf_tier = perf_tier;
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
vdev_tiering_open(vdev_t *vd, uint64_t *asize, uint64_t *max_asize,
                  uint64_t *logical_ashift, uint64_t *physical_ashift)
{
    tiering_map_t *tiering_map = NULL;
    tier_t *perf_tier = NULL;
   

    /* TODO remove print statement */
    zfs_dbgmsg("Inside of vdev_tiering_open");

    /* TODO remove, for analysis and debugging */
    //memset(bsize_by_txg, 0, sizeof(bsize_by_txg));

    /* Check that there is one vdev for tiering */
    if(vd->vdev_children != 1) {
        vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
        return (SET_ERROR(EINVAL));
    }

    vd->vdev_tsd = NULL;


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

    /* Discover the tiers **/


    /* Create the name for the pool of the new tier */
    char name[ZFS_MAX_DATASET_NAME_LEN];
    int name_offset = snprintf(name, sizeof(name), "%s-tier", spa_name(vd->vdev_spa));

    ASSERT(MUTEX_HELD(&spa_namespace_lock));


    for (int i=0; i<1; i++) {

    
        snprintf(name+name_offset, sizeof(name)-name_offset, "%d", i);

        zfs_dbgmsg("Inside of %s@%d looking for spa %s", __FUNCTION__, __LINE__, name);


        /* TODO Need to transfer ownership of objset from FTAG to perf_tier */
        /* Create the performance tier */
        perf_tier = allocate_spa_tier();
        
        ASSERT(perf_tier != NULL);

        /* Initialize the performance tier */
        int rc = perf_tier->init(perf_tier, name);


        if(rc != 0) {
            zfs_dbgmsg("Error opening dataset %s at %s@%d", name, __FUNCTION__, __LINE__);
            return (SET_ERROR(rc));
        }
    }




    // for(int i=0; i<1; i++) {

    //     spa_t *tier_spa = NULL;

    //     snprintf(name+name_offset, sizeof(name)-name_offset, "%d", i);

    //     zfs_dbgmsg("Inside of %s@%d looking for spa %s", __FUNCTION__, __LINE__, name);

    //     /* Find the performance spa by name */
    //     tier_spa = spa_lookup(name);

    //     /* Spa not found, so report and error */
    //     if (tier_spa == NULL) {
    //         zfs_dbgmsg("Inside of %s@%d tier_spa = %s", __FUNCTION__, __LINE__, name);
    //         return (SET_ERROR(EINVAL));
    //     }

    //     ASSERT(spa_open(name, &tier_spa, FTAG) == 0);

    //     zfs_dbgmsg("Inside of %s@%d tier_spa = %s", __FUNCTION__, __LINE__, tier_spa);


    //     /* Since we aren't using the DMU layer at this point (may transition
    //      * to that later), we don't need the syncing and transaction threads */
    //     if (tier_spa->spa_sync_on) {
    //         txg_sync_stop(tier_spa->spa_dsl_pool);
    //         tier_spa->spa_sync_on = B_FALSE;
    //     }

    //     zio_test(tier_spa);
    //     metaslab_test(tier_spa);

    //     perf_tier = allocate_spa_tier(tier_spa);

    //     ASSERT(perf_tier != NULL);
    // }


    

    /* Create an initialize tiering map */
    tiering_map = vdev_tiering_map_init(vd,
                                        perf_tier,
                                        vd->vdev_child[0]);



    /* Store inside of the vdev private data */
    vd->vdev_tsd = tiering_map;

    zfs_dbgmsg("Inside of %s@%d tiering_map = %p\n", __FUNCTION__, __LINE__, tiering_map);

    
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
//    {
//        for(int txg=0; txg<(sizeof(bsize_by_txg)/sizeof(bsize_by_txg[0])); txg++) {
//            for(int bucket_idx = 0; bucket_idx<NUM_BLOCK_SIZES; bucket_idx++) {
//                if(bsize_by_txg[txg][bucket_idx] != 0) {
//                    zfs_dbgmsg("txg: %d bsize: %lld count: %d", txg, block_alloc_dist[bucket_idx].size, bsize_by_txg[txg][bucket_idx]);
//                }
//            }
//        }
//    }

   
    
    
    /* Stop the vdev tiering thread */
    if(tiering_map != NULL) {
    
        /* Iterate over the child vdevs and close them */
        for (int c = 0; c < vd->vdev_children; c++) {
            vdev_close(vd->vdev_child[c]);
        }

        tiering_map->perf_tier->fini(tiering_map->perf_tier);
    
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
vdev_tiering_capacity_allocate_child_done(zio_t *zio) {

    ASSERT(zio->io_error == 0);

    /* Get access to the data range entry and transfer ownership to this function */
    data_range_t *data_range = zio->io_private;
    zio->io_private = NULL;

    /* TOOD remove print statement */
    zfs_dbgmsg("IO_error = %d references held %d", zio->io_error,
               zfs_refcount_count(&data_range->refcount));

    zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);

    tiering_map_t *tiering_map = data_range->tiering_map;
    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    //kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;

    
    data_range->dest_blkptr = *zio->io_bp;


    /* TODO remove this verify code */
    zfs_blkptr_verify(zio->io_spa, &data_range->dest_blkptr, B_FALSE,
                      BLK_VERIFY_HALT);

    /* Add the data range to the address map */
    performance_tier_alloc_tracker_add_mapping(
            tiering_map->perf_tier_alloc_tracker, data_range);


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

    /* Execute zio */
    //zio_execute(data_range->orig_zio);
}


/* TODO this is only temporary until we have better flow control in the
 * write op */
struct write_op_data {
    kmutex_t lock;
    kcondvar_t cv;
    data_range_t *data_range;
    void *buf;
};


static void
vdev_tiering_performance_write_child_done(void *data, int error) {

    /* Get access to the data range entry and transfer ownership of the
     * data_range */
    //data_range_t *data_range = data; //zio->io_private;
    //zio->io_private = NULL;
    struct write_op_data *write_op_data = data;

    /* TODO remove print statement */
    zfs_dbgmsg("IO_error = %d references held %d", error /*zio->io_error*/,
               zfs_refcount_count(&write_op_data->data_range->refcount));


    /* Transfer ownership to myself from myself. TODO: Change this if
     * vdev_tiering_io_start write ever gives ownership to somenone other than 
     * this function */
    // zfs_refcount_transfer_ownership(&write_op_data->data_range->refcount, 
    //                                 write_op_data, FTAG);

    tiering_map_t *tiering_map = write_op_data->data_range->tiering_map;

    switch(error /*zio->io_error*/) {

        /* Success */
        case 0: {

            // data_range->curr_blkptr = *zio->io_bp;

//            zio_t *orig_zio = data_range->orig_zio;

            // /* TODO remove this verify code */
            // zfs_blkptr_verify(zio->io_spa, &data_range->curr_blkptr, B_FALSE,
            //                   BLK_VERIFY_HALT);

           

            /* Remove the local reference to the data range */
            // zfs_refcount_remove(&write_op_data->data_range->refcount, FTAG);

            // data_range_reference_change(write_op_data->data_range);

            mutex_enter(&write_op_data->lock);
            cv_signal(&write_op_data->cv);
            mutex_exit(&write_op_data->lock);

            
           
//            /* Create a child vdev io that only allocates on the capacity tier */
//            zio_t *cap_zio = zio_vdev_child_io(orig_zio,
//                                               orig_zio->io_bp,
//                                               tiering_map->capacity_vdev,
//                                               orig_zio->io_offset,
//                                               NULL, //zio->io_abd,
//                                               orig_zio->io_size,
//                                               orig_zio->io_type,
//                                               orig_zio->io_priority,
//                                               ZIO_FLAG_NODATA,
//                                               vdev_tiering_capacity_allocate_child_done,
//                                               data_range);
//
//            zfs_refcount_transfer_ownership(&data_range->refcount, FTAG, cap_zio);
//
//            zio_nowait(cap_zio);





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
            break;

        /* Out of space on the tier */
        case ENOSPC: {

            zfs_dbgmsg("Retrying at %ld length %ld",            
                       write_op_data->data_range->cap_offset,
                       write_op_data->data_range->cap_size);

            /* Evict or wait of eviction of allocation elements */
            performance_tier_alloc_tracker_evict_blocks(
                    tiering_map->perf_tier_alloc_tracker,
                    tiering_map->perf_tier,
                    write_op_data->data_range->perf_size, 
                    //zio->io_txg,
                    &write_op_data->data_range->num_evict_rounds);

            /* TODO handle errors */
            //ASSERT(zio->io_error==0);


            // zfs_refcount_transfer_ownership(&data_range->refcount, FTAG,
            //                                 write_op_data);

            /*zio_t *perf_zio*/

            int rc = tiering_map->perf_tier->write(
                            tiering_map->perf_tier,
                            write_op_data->buf,
                            write_op_data->data_range->perf_offset,
                            write_op_data->data_range->perf_size,
                            vdev_tiering_performance_write_child_done,
                            write_op_data);

            
            ASSERT(rc == 0);
            if(rc != 0) {
                // zfs_refcount_remove(&data_range->refcount, 
                //                     data_range->orig_zio);

            }

            //zio_add_child(zio_unique_parent(zio), perf_zio);

            //zio_nowait(perf_zio);
        }


            break;

        /* Failure */
        default:

            /* Remove the local reference to the data range */
            // zfs_refcount_remove(&data_range->refcount, FTAG);

            // data_range_reference_change(data_range);

            /* TODO handle errors */
            ASSERT(error == 0);


            //zio_execute(write_op_data->data_range->orig_zio);

            break;

    }
}


// static void
// vdev_tiering_performance_read_child_done(zio_t *zio) {

//     //vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_performance_read_child_done");

//     /* TODO handle error */
//     ASSERT(zio->io_error == 0);

//     /* Get access to the data range entry and transfer ownership to this function */
//     data_range_t *data_range = zio->io_private;
//     zio->io_private = NULL;

//     zfs_refcount_transfer_ownership(&data_range->refcount, zio, FTAG);

//     /* Remove the local reference to the data range */
//     zfs_refcount_remove(&data_range->refcount, FTAG);

//     data_range_reference_change(data_range);
// }




static void
vdev_tiering_capacity_read_child_done(zio_t *zio) {
    vdev_dbgmsg(zio->io_vd, "Inside of %s io_error = %d", __FUNCTION__, zio->io_error);

    ASSERT(zio->io_error == 0);
}


struct abd_iter_tier_read_func_priv {
    tier_t *tier;
    u_int64_t offset;
};

static int
abd_iter_tier_read_func(void *buf, size_t len, void *priv) {

    struct abd_iter_tier_read_func_priv *read_data = priv;

    int rc = read_data->tier->read(read_data->tier,
                                   buf,
                                   read_data->offset,
                                   len);
    if(rc == 0) {
        read_data->offset += len;
    }

    zfs_dbgmsg("abd_iter_tier_read_func rc = %d", rc);

    return rc;
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

//                spa_t *perf_spa = tiering_map->performance_vdev->vdev_spa;
//
//                spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);
//
//                /* Do the read off the physical tier now and fill in the parents
//                 * abd buffer */
//                zio_t *perf_read = zio_read_phys(NULL,
//                                                 tiering_map->performance_vdev->vdev_child[0],
//                                                 data_range->perf_offset,
//                                                 data_range->perf_size,
//                                                 zio->io_abd,
//                                                 ZIO_CHECKSUM_OFF,
//                                                 vdev_tiering_performance_read_child_done,
//                                                 data_range,
//                                                 zio->io_priority,
//                                                 ZIO_FLAG_CANFAIL,
//                                                 B_FALSE);
//
//                zfs_refcount_add(&data_range->refcount, perf_read);
//
//                zio_wait(perf_read);
//
//
//                spa_config_exit(perf_spa, SCL_ALL, FTAG);

                int rc = 0;

                /* Linear buffer so just get a reference to the raw buffer */
                if(abd_is_linear(zio->io_abd)) {
                
                    rc = tiering_map->perf_tier->read(
                                            tiering_map->perf_tier,
                                            abd_to_buf(zio->io_abd),
                                            data_range->perf_offset,
                                            data_range->perf_size);

                /* Scattered buffer so need to iterate over buffers calling
                   the read function */
                } else {

                    struct abd_iter_tier_read_func_priv priv = {
                        .tier   = tiering_map->perf_tier,
                        .offset = data_range->perf_offset
                        };

                    rc = abd_iterate_func(zio->io_abd, 0, data_range->perf_size,  
                                abd_iter_tier_read_func, &priv);
                }



                if(rc != 0) {
                    vdev_dbgmsg(zio->io_vd,
                        "Error in vdev_tiering_io_start read op offset: %llu length %llu rc = %d",
                        zio->io_offset, zio->io_size, rc);

                    zio->io_error = rc;
                    goto READ_DONE;
                } else {
                    zio->io_error = 0;
                }

                //zfs_refcount_add(&data_range->refcount, perf_read);

                //zio_wait(perf_read);

                /* Create a child nop and use that to signal the parent that it is
                 * done */
                // zio_t * nop_zio = zio_null(zio,
                //                            zio->io_spa,
                //                            tiering_map->capacity_vdev,
                //                            NULL,
                //                            NULL,
                //                            ZIO_FLAG_CANFAIL);

                // zio_add_child(zio, nop_zio);

                // zio_nowait(nop_zio);

                /* Remove the local reference to the data range */
                ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
                
                struct perf_tier_alloc_tracker *perf_tier_alloc_tracker = 
                        data_range->perf_tier_alloc_tracker;
                zfs_refcount_remove(&data_range->refcount, FTAG);

                data_range_reference_change(perf_tier_alloc_tracker);

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

            READ_DONE:

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


//           data_range = performance_tier_alloc_tracker_get_block(
//                   tiering_map->perf_tier_alloc_tracker, tiering_map,
//                   zio->io_bp, zio->io_offset, zio->io_size);
//
//            zfs_refcount_transfer_ownership(&data_range->refcount,
//                                            performance_tier_alloc_tracker_get_block,
//                                            FTAG);

            /* TODO need to check for space here, freeing allocations here,
             * and blocking if space is not available */


           /* TODO transfer ownership to here */
           data_range = data_range_create(tiering_map, zio->io_offset,
                                          zio->io_size, -1, zio->io_size,
                                          tiering_map->perf_tier_alloc_tracker);


           zfs_refcount_transfer_ownership(&data_range->refcount, data_range_create, FTAG);


            // data_range->databuf = zio->io_abd;
            data_range->orig_zio = zio;

//

//           zio->io_prop.zp_copies = 1;

//           spa_t *perf_spa = tiering_map->performance_vdev->vdev_spa;
//           zio_prop_t *zp = &zio->io_prop;

//           vdev_dbgmsg(zio->io_vd, "vdev_tiering_io_start zp_checksum %d",
//                        zp->zp_checksum);

//           /* TODO this is a workaround because zp_checksum is set to inherit
//            * but needs to be higher, need to create a new zio_prop with
//            * the correct settings */
//           zp->zp_checksum = ZIO_CHECKSUM_OFF;
//           zp->zp_compress = ZIO_COMPRESS_OFF;
//
//           ASSERT(zp->zp_checksum >= ZIO_CHECKSUM_OFF);
//           ASSERT(zp->zp_checksum < ZIO_CHECKSUM_FUNCTIONS);
//           ASSERT(zp->zp_compress >= ZIO_COMPRESS_OFF);
//           ASSERT(zp->zp_compress < ZIO_COMPRESS_FUNCTIONS);
//           ASSERT(DMU_OT_IS_VALID(zp->zp_type));
//           ASSERT(zp->zp_level < 32);
//           ASSERT(zp->zp_copies > 0);
//           ASSERT(zp->zp_copies <= spa_max_replication(perf_spa));
//
//
//
//
//
//            /* Write the data to the physical location on the performance tier */
//            spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);
//
//            zio_t *perf_zio = zio_write_phys(NULL,
//                                           tiering_map->performance_vdev->vdev_child[0],
//                                           data_range->perf_offset, //zio->io_offset,
//                                           data_range->perf_size, //io->io_size,
//                                           zio->io_abd,
//                                           ZIO_CHECKSUM_OFF,
//                                           vdev_tiering_performance_write_child_done,
//                                           data_range,
//                                           zio->io_priority,
//                                           ZIO_FLAG_CANFAIL,
//                                           B_FALSE);
//
//            zfs_refcount_add(&data_range->refcount, perf_zio);
//
//            zio_wait(perf_zio);
//
//            spa_config_exit(perf_spa, SCL_ALL, FTAG);

            /* Create a child nop and use that to signal the parent that it is
              * done */
            // zio_t * nop_zio = zio_root(zio->io_spa,
            //                            NULL,
            //                            NULL,
            //                            ZIO_FLAG_CANFAIL);

            // data_range->orig_zio = nop_zio;

            

           

            struct write_op_data write_op_data;

            mutex_init(&write_op_data.lock, NULL, MUTEX_DEFAULT, NULL);
            cv_init(&write_op_data.cv, NULL, CV_DEFAULT, NULL);

            write_op_data.data_range = data_range;
            write_op_data.buf = abd_to_buf(zio->io_abd);


            zfs_refcount_add(&data_range->refcount, &write_op_data);


            mutex_enter(&write_op_data.lock);

            data_range->perf_offset = zio->io_offset;

            int rc = 0;

            do {
                /*zio_t *perf_zio*/
                int rc = tiering_map->perf_tier->write(tiering_map->perf_tier,
                                                    /*data_range->databuf*/write_op_data.buf,
                                                    zio->io_offset,
                                                    data_range->perf_size,
                                                    vdev_tiering_performance_write_child_done,
                                                    &write_op_data);

                /* Evaluate the initial result of the write */
                switch(rc) {
                    
                    /* Success so wait on signal from handler */
                    case 0:
                        cv_wait(&write_op_data.cv, &write_op_data.lock);
                        break;

                    /* Out of space error, so free some */
                    case ENOSPC:
                        performance_tier_alloc_tracker_evict_blocks(
                            tiering_map->perf_tier_alloc_tracker,
                            tiering_map->perf_tier,
                            data_range->perf_size,
                            &data_range->num_evict_rounds);

                        break;

                    /* Failure so set the io error and skip to DONE */
                    default:
                        zio->io_error = rc;
                        goto WRITE_DONE;

                }

            } while(rc != 0);
            
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

            WRITE_DONE:
                
                mutex_exit(&write_op_data.lock);

                mutex_destroy(&write_op_data.lock);
                cv_destroy(&write_op_data.cv);

                ASSERT(zfs_refcount_count(&data_range->refcount) > 1);
                
                struct perf_tier_alloc_tracker *perf_tier_alloc_tracker = 
                        data_range->perf_tier_alloc_tracker;
                zfs_refcount_remove(&data_range->refcount, &write_op_data);
                data_range_reference_change(perf_tier_alloc_tracker);

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

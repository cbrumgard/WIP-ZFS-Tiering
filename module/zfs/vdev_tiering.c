
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


struct data_range {

    /* Pointer to the top private data structure */
    tiering_map_t *tiering_map;

    /* Offset of the data range */
    u_int64_t offset;

    /* Size of the data range */
    u_int64_t size;

    /* Data buffer */
    abd_t *databuf;

    /* Node pointers */
    list_node_t list_node;

    /* Prev writes list */
    list_node_t prev_writes_list_node;

    zio_t *orig_zio;

    /* TODO rewrite attempt */
    blkptr_t blkptr;
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

    /*** Water marks for locating data ***/

    /* All transactions at or below this number are on the capacity tier */
    uint64_t on_capcity_tier_txg;

    /* All transactions at or above this number still reside on the
     * performance tier */
    uint64_t on_perf_tier_txg;


    /* TODO list for tracking prev writes */
    list_t prev_writes;
};


static void
vdev_tiering_performance_read_migration_child_done(zio_t *zio) {

    //vdev_dbgmsg(zio->io_vd,
    // "Inside of vdev_tiering_performance_migration_read_child_done, error = %d",
    // zio->io_error);

    /* TODO check error conditions */


    data_range_t *data_range = zio->io_private;
    kmutex_t *lock = &data_range->tiering_map->tiering_migration_thr_lock;
    kcondvar_t *cv = &data_range->tiering_map->tiering_migration_thr_cv;


    mutex_enter(lock);

    /* Move the data range (io_private of child) that are done to the ready
     * to be written to the capacity tier list and signal the migration thread */
    list_insert_head(&(data_range->tiering_map->to_capacity_tier_data_ranges),
                         data_range);

    cv_signal(cv);
    mutex_exit(lock);
}

//static void
//vdev_tiering_performance_read_migration_parent_done(zio_t *zio) {
//
//    vdev_dbgmsg(zio->io_vd, "Inside of %s, error = %d", __FUNCTION__, zio->io_error);
//
//
////    tiering_map_t *tiering_map = zio->io_private;
////    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
////    kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;
//
//    //vdev_dbgmsg(zio->io_vd, "Inside of %s, error = %d", __FUNCTION__, zio->io_error);
//
//
//    /* TODO check error conditions */
//
//
//
//
////    spa_vdev_state_exit(tiering_map->performance_vdev->vdev_spa,
////                        tiering_map->performance_vdev, 0);
//
//
//
///* TODO need to get this part working, but can't figure out to get the
// * parent zio to step */
////    spa_vdev_state_exit(tiering_map->tiering_vdev->vdev_spa,
////                        tiering_map->tiering_vdev, 0);
////    mutex_enter(lock);
////
////    /* Move the data ranges (io_private of child) that are done to the ready
////     * to be written to the capacity tier list and signal the migration thread */
////    for(zio_t *child_zio = list_head(&(zio->io_child_list));
////        child_zio != NULL;
////        child_zio = list_next(&(zio->io_child_list), child_zio)) {
////
////        list_insert_head(&(tiering_map->to_capacity_tier_data_ranges),
////                         child_zio->io_private);
////    }
////
////    cv_signal(cv);
////    mutex_exit(lock);
//}


static void
vdev_tiering_performance_write_migration_child_done(zio_t *zio) {

    //vdev_dbgmsg(zio->io_vd, "Inside of %s, error = %d", __FUNCTION__, zio->io_error);

    data_range_t *data_range = zio->io_private;
    tiering_map_t *tiering_map = data_range->tiering_map;
    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    kcondvar_t *cv = &tiering_map->tiering_migration_thr_cv;


    /* TODO Free should be moved to the parent at a later time */
    abd_free(data_range->databuf);
    data_range->databuf = NULL;

    kmem_free(data_range, sizeof(*data_range));

    zio->io_private = NULL;

    mutex_enter(lock);
    tiering_map->num_of_free_bufs++;
    cv_signal(cv);
    mutex_exit(lock);
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

        /* TODO improve the allocation of data buffers */
        /* Create the data buffer */
        data_range->databuf = abd_alloc(data_range->size, B_FALSE);

        /* Read in the data from the performance tier */
        zio_nowait(zio_vdev_child_io(parent_zio,
                                     NULL,
                                     tiering_map->performance_vdev,
                                     data_range->offset,
                                     data_range->databuf,
                                     data_range->size,
                                     ZIO_TYPE_READ,
                                     ZIO_PRIORITY_ASYNC_READ,
                                     0,
                                     vdev_tiering_performance_read_migration_child_done,
                                     data_range));
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

        /* Read in the data from the performance tier */
        zio_nowait(zio_vdev_child_io(parent_zio,
                                      &data_range->blkptr,
                                      tiering_map->capacity_vdev,
                                      data_range->offset,
                                      data_range->databuf,
                                      data_range->size,
                                      ZIO_TYPE_WRITE,
                                      ZIO_PRIORITY_ASYNC_WRITE,
                                      0,
                                      vdev_tiering_performance_write_migration_child_done,
                                      data_range));

//        zio_wait(zio_write_phys(NULL,
//                                  tiering_map->capacity_vdev,
//                                  data_range->offset,
//                                  data_range->size,
//                                  data_range->databuf,
//                                  ZIO_CHECKSUM_OFF,
//                                  vdev_tiering_performance_write_migration_child_done,
//                                  data_range,
//                                  ZIO_PRIORITY_ASYNC_WRITE,
//                                0,
//                                  B_FALSE));
    }

    return parent_zio;
}

#define migration_thread_sleep_interval 60
#define MAX_BUFS_PER_ROUND 64

static void migration_thread(void *arg) {

    tiering_map_t *tiering_map = arg;
    list_t from_performance_tier_data_ranges;
    list_t to_capacity_tier_data_ranges;
    zio_t *parent_zio = NULL;

    /* Create a working list for data ranges being processed */
    list_create(&from_performance_tier_data_ranges, sizeof(data_range_t),
                offsetof(data_range_t, list_node));

    list_create(&to_capacity_tier_data_ranges, sizeof(data_range_t),
                offsetof(data_range_t, list_node));

    /* TODO remove print statement */
    vdev_dbgmsg(tiering_map->tiering_vdev, "migration_thread is running");

    /* Run until awoken and check if exit thread is set*/
    mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    while(tiering_map->tiering_thread_exit == 0) {

        while((tiering_map->num_of_free_bufs == 0 ||
               list_is_empty(&tiering_map->from_performance_tier_data_ranges)) &&
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

        vdev_dbgmsg(tiering_map->tiering_vdev, "migration_thread is awake");
        //vdev_dbgmsg(tiering_map->tiering_vdev, "num of free bufs1 = %llu", tiering_map->num_of_free_bufs);

        /* There are data ranges on the performance tier to be migrated, move
         * them to a non-contended list */
        while(tiering_map->num_of_free_bufs > 0 &&
              list_is_empty(&tiering_map->from_performance_tier_data_ranges) == B_FALSE) {

            data_range_t *data_range = list_remove_tail(
                    &tiering_map->from_performance_tier_data_ranges);

            tiering_map->num_of_free_bufs--;
//              vdev_dbgmsg(tiering_map->tiering_vdev,
//                    "migration_thread has data range to read at %llu of size %llu",
//                    data_range->offset, data_range->size);

            list_insert_head(&from_performance_tier_data_ranges,
                             data_range);
        }

        //vdev_dbgmsg(tiering_map->tiering_vdev, "num of free bufs2 = %llu", tiering_map->num_of_free_bufs);



        /* There are data ranges that are ready to be transferred to the
        * capacity tier */
        for (data_range_t *data_range = list_remove_tail(
                &tiering_map->to_capacity_tier_data_ranges);
             data_range!=NULL;
             data_range = list_remove_tail(
                     &tiering_map->to_capacity_tier_data_ranges)) {

            vdev_dbgmsg(tiering_map->tiering_vdev,
                    "migration_thread has data ranges to write at %llu of size %llu",
                    data_range->offset, data_range->size);

            list_insert_head(&to_capacity_tier_data_ranges, data_range);
        }

        //vdev_dbgmsg(tiering_map->tiering_vdev, "num of free bufs3 = %llu", tiering_map->num_of_free_bufs);


        /* Release the mutex since the working list doesn't need protection. */
        mutex_exit(&(tiering_map->tiering_migration_thr_lock));



        /* Lock the spa and create a parent zio that will unlock it on
         * completion of the io operations */
        spa_vdev_state_enter(tiering_map->performance_vdev->vdev_spa, RW_READER);




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
        spa_vdev_state_exit(tiering_map->performance_vdev->vdev_spa,
                            tiering_map->performance_vdev, 0);


        /* TODO need to know when to free the parent and children zios */
        parent_zio = NULL;


        /*uint64_t txg = spa_vdev_enter(tiering_map->performance_vdev->vdev_spa); */


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


    vdev_dbgmsg(tiering_map->tiering_vdev, "migration_thread is stopped");

    thread_exit();
}


static tiering_map_t *
vdev_tiering_map_init(vdev_t *my_vdev, vdev_t *fast_tier, vdev_t *slow_tier) {

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
                    offsetof(data_range_t, list_node));
        list_create(&tiering_map->to_capacity_tier_data_ranges, sizeof(data_range_t),
                    offsetof(data_range_t, list_node));

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



        /* TODO test code to track previous writes */
        list_create(&tiering_map->prev_writes, sizeof(data_range_t),
                    offsetof(data_range_t, prev_writes_list_node));

        /* Initialize data locality tx watermarks */
        tiering_map->on_capcity_tier_txg = 0;
        tiering_map->on_perf_tier_txg = 0;
    }

    return tiering_map;
}




static void
vdev_tiering_map_free(tiering_map_t *tiering_map) {


    mutex_destroy(&(tiering_map->tiering_migration_thr_lock));
    cv_destroy(&(tiering_map->tiering_migration_thr_cv));

    list_destroy(&tiering_map->from_performance_tier_data_ranges);
    list_destroy(&tiering_map->to_capacity_tier_data_ranges);

    list_destroy(&tiering_map->prev_writes);

    kmem_free(tiering_map, sizeof(tiering_map_t));
}

static int
vdev_tiering_open(vdev_t *vd, u_int64_t *asize, u_int64_t *max_asize,
                  u_int64_t *ashift)
{
    tiering_map_t *tiering_map = NULL;

    /* TODO remove print statement */
    zfs_dbgmsg("Inside of vdev_tiering_open");

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

    ASSERT(MUTEX_HELD(&spa_namespace_lock));

    zfs_dbgmsg("Inside of %s@%d looking for spa %s", __FUNCTION__, __LINE__, name);

    /* Find the performance spa by name */
    performance_spa = spa_lookup(name);

    /* Spa not found, so report and error */
    if (performance_spa == NULL) {
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
        *ashift = MAX(*ashift, child_vdev->vdev_ashift);
    }

    /* Create an initialize tiering map */
    tiering_map = vdev_tiering_map_init(vd,
                                        performance_spa->spa_root_vdev,
                                        vd->vdev_child[0]);

    /* Store inside of the vdev private data */
    vd->vdev_tsd = tiering_map;


    /* TODO remove print statement */
    vdev_dbgmsg(vd,"vd = %s fast vd = %s slow vd = %s spa = %p fast spa = %p slow spa = %p",
            vd->vdev_path, performance_spa->spa_root_vdev->vdev_path, vd->vdev_child[0]->vdev_path, vd->vdev_spa, performance_spa->spa_root_vdev->vdev_spa, vd->vdev_child[0]->vdev_spa);

    /* Success if tiering map was created successfully */
    return tiering_map == NULL;
}

static void
vdev_tiering_close(vdev_t *vd)
{
    tiering_map_t *tiering_map = vd->vdev_tsd;

    /* TODO remove print statement */
    vdev_dbgmsg(vd, "Inside of vdev_tiering_close");

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

    /* Get access to the data range entry */
    data_range_t *data_range = zio->io_private;

    /* No migration is required */
    if(data_range == NULL) {
        vdev_dbgmsg(zio->io_vd, "Inside of %s, no migration is required", __FUNCTION__);
    }


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
}



static void
vdev_tiering_capacity_allocate_child_done(zio_t *zio) {
    //vdev_dbgmsg(zio->io_vd, "Inside of %s", __FUNCTION__);


    /* Get access to the data range entry */
    data_range_t *data_range = zio->io_private;


    char buf1[256];
    char buf2[256];


    snprintf_blkptr(buf1, sizeof(buf1), &data_range->blkptr);
    snprintf_blkptr(buf2, sizeof(buf2), zio->io_bp);


    vdev_dbgmsg(zio->io_vd, "orig bp = %s", buf1);
    vdev_dbgmsg(zio->io_vd, "new bp  = %s", buf2);
}

static void
vdev_tiering_capacity_read_child_done(zio_t *zio) {
    vdev_dbgmsg(zio->io_vd, "Inside of %s", __FUNCTION__);

}

static void
vdev_tiering_io_start(zio_t *zio) {

    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_vd->vdev_tsd;
//    kmutex_t *lock = &tiering_map->tiering_migration_thr_lock;
    vdev_t *vd;
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

            /* TODO currently reads only happen on the slow tier, once we
             * have data migration working then we need to select where to read
             * from */

//            if(zio->io_size < 8192) {
//                vd = tiering_map->performance_vdev;
//            } else {
            vd = tiering_map->capacity_vdev;
//            }
//
            /* Schedule a read on the capacity tier */
//            zio_nowait(
//                    zio_vdev_child_io(zio, zio->io_bp, vd,
//                                      zio->io_offset, zio->io_abd, zio->io_size,
//                                      zio->io_type, zio->io_priority, 0,
//                                      vdev_tiering_capacity_read_child_done,
//                                      tiering_map));

            spa_t *perf_spa = tiering_map->performance_vdev->vdev_spa;

            spa_config_enter(perf_spa, SCL_ALL, FTAG, RW_READER);

            /* Do the read off the physical tier now and fill in the parents
             * abd buffer */
            zio_wait(
                    zio_read_phys(NULL,
                                  tiering_map->performance_vdev->vdev_child[0],
                                  zio->io_offset,
                                  zio->io_size,
                                  zio->io_abd,
                                  ZIO_CHECKSUM_OFF,
                                  vdev_tiering_performance_read_child_done,
                                  tiering_map,
                                  zio->io_priority,
                                  ZIO_FLAG_CANFAIL,
                                  B_FALSE));

            spa_config_exit(perf_spa, SCL_ALL, FTAG);


            /* Create a child nop and use that to signal the parent that it is
             * done */
            zio_t * nop_zio = zio_null(zio,
                                       zio->io_spa,
                                       vd,
                                       vdev_tiering_capacity_read_child_done,
                                       tiering_map,
                                 ZIO_FLAG_CANFAIL);


            zio_add_child(zio, nop_zio);

            zio_nowait(nop_zio);

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

           /* Large writes need a data range to handle migration */
           //if(zio->io_size >= 8192) {

                /* Create a data range and add it to the list of data ranges for
                 * later migration (Need to capture this information here because
                 * the child io will shift the offset within zio of the callback */
                data_range = kmem_alloc(sizeof(data_range_t), KM_SLEEP);
                data_range->tiering_map = tiering_map;
                data_range->offset = zio->io_offset;
                data_range->size = zio->io_size;
                data_range->databuf = NULL;
                data_range->orig_zio = zio;
                memcpy(&data_range->blkptr, zio->io_bp, sizeof(blkptr_t));
           // }



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



           vdev_dbgmsg(zio->io_vd,
                       "Inside of vdev_tiering_io_start before zio_wait: io_metaslab_class %p",
                       zio->io_metaslab_class);

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

            zio_wait(
                    zio_write_phys(NULL,
                                   tiering_map->performance_vdev->vdev_child[0],
                                   zio->io_offset,
                                   zio->io_size,
                                   zio->io_abd,
                                   ZIO_CHECKSUM_OFF,
                                   vdev_tiering_performance_write_child_done,
                                   data_range,
                                   zio->io_priority,
                                   ZIO_FLAG_CANFAIL,
                                   B_FALSE));

            spa_config_exit(perf_spa, SCL_ALL, FTAG);


            vdev_dbgmsg(zio->io_vd, "Inside of vdev_tiering_io_start after zio_wait");


            /* Create a child vdev io that only allocates on the capacity tier */
            zio_nowait(
                    zio_vdev_child_io(zio,
                                      zio->io_bp,
                                      tiering_map->capacity_vdev,
                                      zio->io_offset,
                                      zio->io_abd,
                                      zio->io_size,
                                      zio->io_type,
                                      zio->io_priority,
                                      ZIO_FLAG_NODATA,
                                      vdev_tiering_capacity_allocate_child_done,
                                      data_range));


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


/* Provides the tiering operation functions. Only op_type and op_leaf are set to
 * anything meaningful at the moment */
vdev_ops_t vdev_tiering_ops = {
    .vdev_op_open = vdev_tiering_open,
    .vdev_op_close = vdev_tiering_close,
    .vdev_op_asize = vdev_default_asize,    /* Use the default method since
 *                                             the tiering vdev does none of
 *                                             it's own allocations */
    .vdev_op_io_start = vdev_tiering_io_start,
    .vdev_op_io_done = vdev_tiering_io_done,
    .vdev_op_state_change = NULL,
    .vdev_op_need_resilver = NULL,
    .vdev_op_hold = NULL,
    .vdev_op_rele = NULL,
    .vdev_op_remap = NULL,
    .vdev_op_xlate = NULL,
    .vdev_op_type = VDEV_TYPE_TIERING,  /* Name of vdev type */
    .vdev_op_leaf = B_FALSE             /* Not a leaf vdev */
};



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


struct data_range {

    u_int64_t offset;
    u_int64_t size;
    list_node_t list_node;
};

struct tiering_map {

    /* Pointer to the performance tier vdev */
    vdev_t *performance_tier;

    /* Pointer to the capacity tier vdev */
    vdev_t *capacity_tier;


    /* List of ranges (by offset and length) on the performance tier */
    list_t data_ranges;

    /*   */
    kmutex_t tiering_migration_thr_lock;

    /*  */
    kcondvar_t tiering_migration_thr_cv;

    /* */
    kthread_t *tiering_thread;

    /* Flag for controlling the tiering thread */
    u_int8_t tiering_thread_exit;
};

typedef struct data_range data_range_t;
typedef struct tiering_map tiering_map_t;


#define migration_thread_sleep_interval 10

static void migration_thread(void *arg) {

    tiering_map_t *tiering_map = arg;


    /* TODO remove print statement */
    cmn_err(CE_WARN, "migration_thread is running");

    /* Run until awoken and check if exit thread is set*/
    mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    while(tiering_map->tiering_thread_exit == 0) {
        cv_timedwait(&(tiering_map->tiering_migration_thr_cv),
                &(tiering_map->tiering_migration_thr_lock),
                ddi_get_lbolt() + SEC_TO_TICK(migration_thread_sleep_interval));
        mutex_exit(&(tiering_map->tiering_migration_thr_lock));


        cmn_err(CE_WARN, "migration_thread is awake");

        /* Look for migrations to perform */
        for(data_range_t *data_range = list_remove_tail(&tiering_map->data_ranges);
            data_range != NULL;
            data_range = list_remove_tail(&tiering_map->data_ranges)) {

            cmn_err(CE_WARN, "migration_thread has data range at %ull of size %ull",
                    data_range->offset, data_range->size);

            kmem_free(data_range, sizeof(data_range_t));
        }



        mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    }

    /* Signal that the thread is stopped */
    mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    tiering_map->tiering_thread_exit = 0;
    tiering_map->tiering_thread = NULL;
    cv_signal(&(tiering_map->tiering_migration_thr_cv));
    mutex_exit(&(tiering_map->tiering_migration_thr_lock));


    cmn_err(CE_WARN, "migration_thread is stopped");

    thread_exit();
}


static tiering_map_t *
vdev_tiering_map_init(vdev_t *fast_tier, vdev_t *slow_tier) {

    /* Allocate the central main data structure for the tiering vdev instance */
    tiering_map_t *tiering_map = kmem_zalloc(sizeof(tiering_map_t), KM_SLEEP);

    /* This should always happen since sleep is specified until memory is
     * available but just in case */
    if(tiering_map != NULL) {
        tiering_map->performance_tier = fast_tier;
        tiering_map->capacity_tier = slow_tier;
    }


    list_create(&tiering_map->data_ranges, sizeof(tiering_map_t),
                offsetof(data_range_t, list_node));

    mutex_init(&tiering_map->tiering_migration_thr_lock, NULL, MUTEX_DEFAULT, NULL);
    cv_init(&tiering_map->tiering_migration_thr_cv, NULL, CV_DEFAULT, NULL);

    tiering_map->tiering_thread = thread_create(NULL,
                                                0,
                                                migration_thread,
                                                tiering_map,
                                                0,
                                                &p0,
                                                TS_RUN,
                                                minclsyspri);


    return tiering_map;
}




static void
vdev_tiering_map_free(tiering_map_t *tiering_map) {


    mutex_destroy(&(tiering_map->tiering_migration_thr_lock));
    cv_destroy(&(tiering_map->tiering_migration_thr_cv));

    kmem_free(tiering_map, sizeof(tiering_map_t));
}


static int
vdev_tiering_open(vdev_t *vd, u_int64_t *asize, u_int64_t *max_asize,
                  u_int64_t *ashift)
{
    tiering_map_t *tiering_map = NULL;

    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_open");

    /* Check that there are two vdevs for tiering */
    if(vd->vdev_children != 2) {
        vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
        return (SET_ERROR(EINVAL));
    }

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
    tiering_map = vdev_tiering_map_init(vd->vdev_child[0], vd->vdev_child[1]);

    /* Store inside of the vdev private data */
    vd->vdev_tsd = tiering_map;

    /* Success if tiering map was created successfully */
    return tiering_map == NULL;
}

static void
vdev_tiering_close(vdev_t *vd)
{
    tiering_map_t *tiering_map = vd->vdev_tsd;

    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_close");

    /* Iterate over the child vdevs and close them */
    for (int c = 0; c < vd->vdev_children; c++) {
        vdev_close(vd->vdev_child[c]);
    }

    /* Stop the vdev tiering thread */
    mutex_enter(&(tiering_map->tiering_migration_thr_lock));
    tiering_map->tiering_thread_exit = 1;
    cv_signal(&(tiering_map->tiering_migration_thr_cv));
    while(tiering_map->tiering_thread_exit != 0) {
        cv_wait(&(tiering_map->tiering_migration_thr_cv),
                &(tiering_map->tiering_migration_thr_lock));
    }
    mutex_exit(&(tiering_map->tiering_migration_thr_lock));


    /* TODO need to decide when to free tiering map, may need to hold if
     * ops are still underway */
    vdev_tiering_map_free(vd->vdev_tsd);
}

#if 0
static void
vdev_tiering_capacity_write_child_done(zio_t *zio) {

    /* Get access to my vdev private data */
    //tiering_map_t *tiering_map = zio->io_vsd;

    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_capacity_write_child_done");

    /* TODO implement */

    /* TODO check for errors */
}

#endif

static void
vdev_tiering_performance_write_child_done(zio_t *zio) {


    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_performance_write_child_done");

#if 1
    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_private;

    /* TODO check for errors on write to performance tier */



    /* TODO see how zio->io_priority works, might be able to modify it to make
     * this less important */

    /* Check on whether to use rewrite, physical write or this */

    /* TODO look up SPA, abd,  */


    /* Create a data range add add it to the list of data ranges for
     * later migration */
    data_range_t *data_range = kmem_alloc(sizeof(data_range_t), KM_SLEEP);
    data_range->offset = zio->io_offset;
    data_range->size = zio->io_size;

    mutex_enter(&tiering_map->tiering_migration_thr_lock);
    list_insert_head(&tiering_map->data_ranges, data_range);
    cv_signal(&tiering_map->tiering_migration_thr_cv);
    mutex_exit(&tiering_map->tiering_migration_thr_lock);



//    zio_t *migrate_zio = zio_create(
//            NULL, /* No parent io */
//            NULL, /* No spa */
//            0,    /* No transaction group */
//            NULL, /* No block pointer */
//            NULL, /* No abd */
//            zio->io_size, /* Logical size */
//            zio->io_size, /* Physical size */
//            vdev_tiering_capacity_write_child_done, /* Callback */
//            tiering_map, /* My data structure */
//            ZIO_TYPE_WRITE,
//            ZIO_FLAG_PHYSICAL & ~ZIO_FLAG_IO_ALLOCATING/* flags ? */,
//            tiering_map->slow_tier, /* Capacity slow tier */
//            zio->io_offset,
//            NULL,                         /* Bookmark */
//            ZIO_STAGE_VDEV_IO_START >> 1, /* Vdev stage, skip a bunch of other things in the pipeline */
//            ZIO_VDEV_CHILD_PIPELINE,      /* Child Pipeline */);



    /* Copy the data from the original buffer to the new buffer */
    /* TODO eventually this will submitting a read instead to get the
     * data from the performance tier and then issue another zio
     * for writing the data */
//    abd_t *wdatabuf = abd_alloc_sametype(zio->io_abd, zio->io_size);
//    abd_copy(wdatabuf, zio->io_abd, zio->io_size);





#if 0
    /* New root ZIO */
    zio_t *root_zio = zio_root(tiering_map->slow_tier->vdev_spa, NULL, NULL, 0);

    /* Create an actual worker zio off of root */
    zio_t *migrate_zio = zio_vdev_child_io(
            root_zio,
            NULL,
            tiering_map->slow_tier,
            zio->io_offset,
            wdatabuf,
            zio->io_size,
            ZIO_TYPE_WRITE,
            ZIO_PRIORITY_ASYNC_WRITE,
            ZIO_FLAG_PHYSICAL & ~ZIO_FLAG_IO_ALLOCATING,
            vdev_tiering_capacity_write_child_done,
            tiering_map);

    zio_nowait(migrate_zio);

#endif

#if 0
    zio_t *migrate_zio = zio_rewrite(NULL,
                                     tiering_map->capacity_tier->vdev_spa,
                                     0,
                                     zio->io_bp,
                                     wdatabuf,
                                     zio->io_size,
                                     vdev_tiering_capacity_write_child_done,
                                     tiering_map,
                                     ZIO_PRIORITY_ASYNC_WRITE,
                                     ~ZIO_FLAG_IO_ALLOCATING,
                                     NULL);
#endif

#if 0
    zio_t *migrate_zio = zio_vdev_delegated_io(
            tiering_map->slow_tier,
            zio->io_offset,
            wdatabuf,
            zio->io_size,
            zio->io_type,
            zio->io_priority,
            0,
            vdev_tiering_capacity_write_child_done,
            tiering_map);
#endif

//    zio_nowait(root_zio);
 //     zio_nowait(migrate_zio);
//    zio_execute(root_zio);

#endif
}

static void
vdev_tiering_performance_read_child_done(zio_t *zio) {

    cmn_err(CE_WARN, "Inside of vdev_tiering_performance_read_child_done");
}


static void
vdev_tiering_io_start(zio_t *zio) {

    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_vd->vdev_tsd;

    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_io_start, tiering_map = %p", tiering_map);

    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ:
            cmn_err(CE_WARN, "vdev_tiering_io_start read op offset: %llu length %llu", zio->io_offset, zio->io_size);

            /* TODO currently reads only happen on the fast tier, once we
             * have data migration working then we need to select where to read
             * from */

            /* Schedule a read on the fast tier */
            zio_nowait(
                    zio_vdev_child_io(zio, zio->io_bp, tiering_map->performance_tier,
                                      zio->io_offset, zio->io_abd, zio->io_size,
                                      zio->io_type, zio->io_priority, 0,
                                      vdev_tiering_performance_read_child_done,
                                      tiering_map));

            /* Execute zio */
            zio_execute(zio);

            break;

        /* Write operation */
        /* TODO Implement write op */
        case ZIO_TYPE_WRITE:
            cmn_err(CE_WARN, "vdev_tiering_io_start write op offset: %llu length %llu sending to %s",
                    zio->io_offset, zio->io_size,
                    tiering_map->performance_tier->vdev_path);

            /* TODO this is only the base case of a write, need to handle more
             * complex cases like reslivering and scrubs */

            /* TODO find out what zio->io_abd is */

            /* Schedule a write to the fast tier */
            zio_nowait(
                    zio_vdev_child_io(zio, zio->io_bp, tiering_map->performance_tier,
                                      zio->io_offset, zio->io_abd, zio->io_size,
                                      zio->io_type, zio->io_priority, 0,
                                      vdev_tiering_performance_write_child_done,
                                      tiering_map));

            /* Execute zio */
            zio_execute(zio);

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
            cmn_err(CE_WARN, "vdev_tiering_io_start unsupported operation %d",
                    zio->io_type);
            zio->io_error = SET_ERROR(ENOTSUP);
            zio_interrupt(zio);
            break;
    }
}


static void
vdev_tiering_io_done(zio_t *zio) {

    /* TODO remove print statement */
    cmn_err(CE_WARN, "Inside of vdev_tiering_io_done");

    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ:
            cmn_err(CE_WARN, "vdev_tiering_io_done read op offset: %llu length %llu", zio->io_offset, zio->io_size);
            break;

            /* Write operation */
            /* TODO Implement write op */
        case ZIO_TYPE_WRITE:
            cmn_err(CE_WARN, "vdev_tiering_io_done write op offset: %llu length %llu", zio->io_offset, zio->io_size);
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
            cmn_err(CE_WARN, "vdev_tiering_io_done unsupported operation %d",
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
    .vdev_op_type = VDEV_TYPE_TIERING,  /* Nmae of vdev type */
    .vdev_op_leaf = B_FALSE             /* Not a leaf vdev */
};

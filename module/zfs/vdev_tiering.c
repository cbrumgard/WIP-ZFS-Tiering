
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


struct tiering_map {

    /* Pointer to the fast tier vdev */
    vdev_t *fast_tier;

    /* Pointer to the slow tier vdev */
    vdev_t *slow_tier;
};

typedef struct tiering_map tiering_map_t;

static tiering_map_t *
vdev_tiering_map_init(vdev_t *fast_tier, vdev_t *slow_tier) {

    /* Allocate the central main data structure for the tiering vdev instance */
    tiering_map_t *tiering_map = kmem_zalloc(sizeof(tiering_map_t), KM_SLEEP);

    /* This should always happen since sleep is specified until memory is
     * available but just in case */
    if(tiering_map != NULL) {
        tiering_map->fast_tier = fast_tier;
        tiering_map->slow_tier = slow_tier;
    }

    return tiering_map;
}

static void
vdev_tiering_map_free(tiering_map_t *tiering_map) {

    kmem_free(tiering_map, sizeof(tiering_map_t));
}


static int
vdev_tiering_open(vdev_t *vd, u_int64_t *asize, u_int64_t *max_asize,
                  u_int64_t *ashift)
{
    tiering_map_t *tiering_map = NULL;

    /* TODO remove print statement */
    cmn_err(CE_NOTE, "Inside of vdev_tiering_open");

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
    /* TODO remove print statement */
    cmn_err(CE_NOTE, "Inside of vdev_tiering_close");

    /* Iterate over the child vdevs and close them */
    for (int c = 0; c < vd->vdev_children; c++) {
        vdev_close(vd->vdev_child[c]);
    }

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
    cmn_err(CE_NOTE, "Inside of vdev_tiering_capacity_write_child_done");

    /* TODO implement */

    /* TODO check for errors */
}

#endif

static void
vdev_tiering_performance_write_child_done(zio_t *zio) {


    /* TODO remove print statement */
    cmn_err(CE_NOTE, "Inside of vdev_tiering_performance_write_child_done");

#if 0
    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_vsd;

    /* TODO check for errors on write to performance tier */





    /* TODO see how zio->io_priority works, might be able to modify it to make
     * this less important */

    /*zio_t *next_zio = */zio_vdev_delegated_io(tiering_map->slow_tier,
                                     zio->io_offset,
                                     zio->io_abd, zio->io_size, zio->io_type,
                                     zio->io_priority, 0,
                                     vdev_tiering_capacity_write_child_done,
                                     tiering_map);

//    zio_nowait(next_zio);
//    zio_execute(next_zio);

#endif
}

static void
vdev_tiering_performance_read_child_done(zio_t *zio) {

    cmn_err(CE_NOTE, "Inside of vdev_tiering_performance_read_child_done");
}


static void
vdev_tiering_io_start(zio_t *zio) {

    /* Get access to my vdev private data */
    tiering_map_t *tiering_map = zio->io_vd->vdev_tsd;

    /* TODO remove print statement */
    cmn_err(CE_NOTE, "Inside of vdev_tiering_io_start");

    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ:
            cmn_err(CE_NOTE, "vdev_tiering_io_start read op offset: %llu length %llu", zio->io_offset, zio->io_size);

            /* TODO currently reads only happen on the fast tier, once we
             * have data migration working then we need to select where to read
             * from */

            /* Schedule a read on  the fast tier */
            zio_nowait(
                    zio_vdev_child_io(zio, zio->io_bp, tiering_map->fast_tier,
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
            cmn_err(CE_NOTE, "vdev_tiering_io_start write op offset: %llu length %llu sending to %s",
                    zio->io_offset, zio->io_size, tiering_map->fast_tier->vdev_path);

            /* TODO this is only the base case of a write, need to handle more
             * complex cases like reslivering and scrubs */

            /* TODO find out what zio->io_abd is */

            /* Schedule a write to the fast tier */
            zio_nowait(
                    zio_vdev_child_io(zio, zio->io_bp, tiering_map->fast_tier,
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
            cmn_err(CE_NOTE, "vdev_tiering_io_start unsupported operation %d",
                    zio->io_type);
            zio->io_error = SET_ERROR(ENOTSUP);
            zio_interrupt(zio);
            break;
    }
}


static void
vdev_tiering_io_done(zio_t *zio) {

    /* TODO remove print statement */
    cmn_err(CE_NOTE, "Inside of vdev_tiering_io_done");

    switch(zio->io_type) {

        /* Read operation */
        /* TODO Implement read op */
        case ZIO_TYPE_READ:
            cmn_err(CE_NOTE, "vdev_tiering_io_done read op offset: %llu length %llu", zio->io_offset, zio->io_size);
            break;

            /* Write operation */
            /* TODO Implement write op */
        case ZIO_TYPE_WRITE:
            cmn_err(CE_NOTE, "vdev_tiering_io_done write op offset: %llu length %llu", zio->io_offset, zio->io_size);
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
            cmn_err(CE_NOTE, "vdev_tiering_io_done unsupported operation %d",
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

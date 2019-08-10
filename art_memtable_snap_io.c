#ifndef _SCM_PCLINT_
#include "art_pri.h"
#include "art_memtable_snap_io.h"
#include "art_memtable.h"
#include "art_path.h"
#include "errcode.h"
#include "art_log.h"
#include "art_snap_path_impl.h"
#include "art_key_path_impl.h"
#include "art_snapshot.h"
#include "art_snapshot_memtable.h"
#include "art_snap_path_memtable.h"
#include "art_snap_path_sstable.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif


#define ART_MEMTABLE_SNAP_IO_CHECK_NODE(node, errode) \
    if(!is_snap_inner_node(node) && !is_art_leaf_node(node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, snap_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errode;    \
    }


/**
 * search value from the snap node
 * @arg t The tree
 * @arg parent_node snap root/leaf
 * @arg snapshotid
 * @return kOK means successfully, otherwise other code
 */
int32_t memtable_snap_search(
    IN art_tree_t* t,
    IN art_snap_node* parent_node,
    IN artree_get_para_t *get_para)
{
    art_snap_node* cur_node      = NULL;
    art_snap_node* next_node     = NULL;
    int32_t        result        = kOk;
    uint32_t       depth         = 0;
    uint32_t       snapshotid    = get_para->snapshotid;
    uint32_t       idx_in_parent = 0;

    cur_node = parent_node;
    if (NULL == cur_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, parent node is null.");
        return kInvalidArgument;
    }

    ART_MEMTABLE_SNAP_IO_CHECK_NODE(cur_node, kInvalidArgument);

    while(is_snap_inner_node(cur_node))
    {
        result = memtable_snap_search_node_equal(cur_node, snapshotid, SNAPID_LEN, depth, &next_node, &idx_in_parent);
        if(kOk != result)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "search snapid [%u] failed, result [%d], depth[%u] node [%u.%u.%u].", snapshotid, result, depth, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return kArtSnapIdNotFound;
        }

        depth += cur_node->partial_len;
        depth += 1;
        cur_node = next_node;
    }
    UNREFERENCE_PARAM(idx_in_parent);
    ART_ASSERT(is_art_leaf_node(cur_node));

    return  art_get_check_and_get_leaf(t, (void*)cur_node, get_para);
}


/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT
 */
/*int32_t memtable_io_snap_get_smaller_or_equal(
    IN art_tree_t* t,
    IN art_snap_node *start_node,
    IN uint32_t snapshotid)
{
    int32_t result                      = kOk;
    
    art_path_operations_t ops;
    art_snap_path_t snap_path;
    ops.read_plog_f = t->ops.read;
    ops.get_buffer_f = t->ops.get_buffer;
    ops.release_buffer_f = t->ops.release_buffer;

    init_snap_path(&snap_path);
    init_snap_path_ops(&snap_path, &ops);
    assign_snap_path_internal_ops(&snap_path, t->art_ops);

    result = memtable_snap_path_get_smaller_or_equal(&snap_path, start_node, 0, snapshotid);
    if(ART_COMPARE_RESULT_GREATER == result)
    {
        return kArtSnapIdNotFound;
    }

    return result;
}
*/

#ifdef __cplusplus
}
#endif



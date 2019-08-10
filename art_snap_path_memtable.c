#ifndef _SCM_PCLINT_
#include <assert.h>
#include "art_log.h"
#include "art_snapshot_memtable.h"
#include "art_snap_path_impl.h"
#include "art_snap_path_memtable.h"
#include "art_sfmea.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif


#define MEMTABLE_SNAP_PATH_CHECK_NODE(node, errode) \
    if(!is_snap_inner_node(node) && !is_art_leaf_node(node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, snap_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errode;    \
    }

    

/**
 * get left/right most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_snap_path_get_lr_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f)
{
    art_snap_node *cur_node                 = NULL;
    art_snap_node *next_node                = NULL;

    uint8_t key                             = 0;
    int32_t result                          = kOk;

    cur_node = start_node;
    
    MEMTABLE_SNAP_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    while(is_snap_inner_node(cur_node))
    {
        // 1. add node and partial to snap path
        push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
        push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);

        LVOS_TP_START(ART_TP_MEMTABLE_SNAP_NODE_ERR_RETURN_KNOTFOUND, &result)
        // 2. get left/right child
        result = snap_get_lr_child_f(cur_node, &next_node, &pos_in_parent);
        LVOS_TP_END
        if(kOk != result)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }

        // 3. set next node child_pos and key prefix
        set_last_child_pos_in_snap_path(snap_path, pos_in_parent);
        key = snap_get_key_by_idx(cur_node, pos_in_parent);
        push_key_into_snap_path(snap_path, &key, 1);

        // 4. set child current node
        cur_node = next_node;
    }

    assert(is_art_leaf_node(cur_node));
    
    // 5. push last leaf to path
    push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);

    return kOk;
}


/**
 * get left most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_snap_path_get_left_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent)
{
    return memtable_snap_path_get_lr_most_path(snap_path, start_node, pos_in_parent, memtable_snap_get_node_left_child);
}


/**
 * get right most path for parent_node
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @return kOk means ok, otherwise something wrong.
int32_t memtable_snap_path_get_right_most_path(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node, // snap node or leaf
    IN uint32_t pos_in_parent)
{
    return memtable_snap_path_get_lr_most_path(snap_path, start_node, pos_in_parent, memtable_snap_get_node_right_child);
} */


/**
 * switch path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @arg snap_get_next_child_f function pointer for get next/prev child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_snap_path_switch_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f)
{
    art_snap_node *cur_node                 = NULL;
    art_snap_node *next_node                = NULL;

    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    
    uint8_t key                             = 0;
    int32_t result                          = kOk;

    if(check_snap_path_empty(snap_path))
    {
        return kArtIteratorEnd;
    }
    
    cur_node = (art_snap_node *)get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
    
    MEMTABLE_SNAP_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    // 1. leaf node need to pop from path
    if(is_art_leaf_node(cur_node))
    {
        pop_node_from_snap_path(snap_path);
        if(check_snap_path_empty(snap_path))
        {
            return kArtIteratorEnd;
        }
        cur_node = (art_snap_node *)get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
    }

    
    // 2. backtrace in snap tree
    while(is_snap_inner_node(cur_node))
    {
        // backtrace prev key
        pop_key_from_snap_path(snap_path, 1);
        
        // get next/prev snap child
        result = snap_get_next_child_f(cur_node, child_pos, &next_node, &pos_in_parent);

        // no next child, need to backtrace
        if(result != kOk)
        {
            assert(kNotFound == result);
            // no next child, need to backtrace
            (void)pop_node_from_snap_path(snap_path);
            pop_key_from_snap_path(snap_path, cur_node->partial_len);

            if(check_snap_path_empty(snap_path))
            {
                return kArtIteratorEnd;
            }

            // get parent node in path
            cur_node = (art_snap_node *)get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
            continue;
        }
        
        // set next node child_pos and key prefix
        set_last_child_pos_in_snap_path(snap_path, pos_in_parent);
        key = snap_get_key_by_idx(cur_node, pos_in_parent);
        push_key_into_snap_path(snap_path, &key, 1);

        // get next node left/right most path
        result = memtable_snap_path_get_lr_most_path(snap_path, next_node, pos_in_parent, snap_get_lr_child_f);
        if(kNotFound == result)
        {
        	return kInternalError;
        }
        break;
    }

    return kArtIteratorContinue;
}


/**
 * switch prev path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.

int32_t memtable_snap_path_switch_prev_path(
    IO art_snap_path_t *snap_path)
{
    return memtable_snap_path_switch_path(snap_path, memtable_snap_get_node_right_child, memtable_snap_get_node_prev_child);
} */


/**
 * switch next path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_snap_path_switch_next_path(
    IO art_snap_path_t *snap_path)
{
    return memtable_snap_path_switch_path(snap_path, memtable_snap_get_node_left_child, memtable_snap_get_node_next_child);
}


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means leaf in path equal/bigger than input.snapid, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
static int32_t memtable_snap_path_do_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid)
{
    art_snap_node *cur_node                 = NULL;
    art_snap_node *next_node                = NULL;

    int32_t result                          = ART_COMPARE_RESULT_EQUAL; // art return code
    int32_t ret                             = kOk; // pelagodb return code

    uint32_t idx_in_parent                  = 0;
    uint8_t key                             = 0;
    uint32_t depth                          = 0;
    uint32_t found_snap_id                  = 0;

    cur_node = start_node;

    MEMTABLE_SNAP_PATH_CHECK_NODE(cur_node, ART_COMPARE_RESULT_ERR);

    while(is_snap_inner_node(cur_node))
    {
        depth = get_prefix_len_in_snap_path(snap_path);
        
        // get next equal or bigger child
        result = memtable_snap_search_node_equal_or_bigger(cur_node, snapshotid, SNAPID_LEN, depth, &next_node, &idx_in_parent);
        if(ART_COMPARE_RESULT_EQUAL == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            cur_node = next_node;
            pos_in_parent = idx_in_parent;
            continue;
        }
        
        if(ART_COMPARE_RESULT_GREATER == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            // get its left most path.
            ret = memtable_snap_path_get_left_most_path(snap_path, next_node, idx_in_parent);
            //assert(kOk == ret);
            if(kNotFound == ret)
            {
            	return ART_COMPARE_RESULT_ERR;
            }
            break;
        }
        
        // ART_COMPARE_RESULT_LESS == result. caller needs to switch path at last node in path use child_pos.
        break;
    }

    if(ART_COMPARE_RESULT_EQUAL != result)
    {
        return result;
    }

    assert(is_art_leaf_node(cur_node));
    found_snap_id = snap_path->art_ops->leaf_ops.get_snap_id(cur_node);
    if(found_snap_id > snapshotid)
    {
        // in this case, return GREATER_FOUND means found the required data, caller can use it to return.
        push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
        result = ART_COMPARE_RESULT_GREATER;
    }
    else if(found_snap_id == snapshotid)
    {
        push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
        result = ART_COMPARE_RESULT_EQUAL;
    }
    else
    {
        result = ART_COMPARE_RESULT_LESS;
    }

    UNREFERENCE_PARAM(ret);
    return result;
}


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT
 */
int32_t memtable_snap_path_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid)
{
    int32_t result                          = ART_COMPARE_RESULT_EQUAL;

    result = memtable_snap_path_do_get_bigger_or_equal(snap_path, start_node, pos_in_parent, snapshotid);
    if(ART_COMPARE_RESULT_LESS != result)
    {
        return result;
    }

    // in this case, last node in path is smaller than required. pop it from path, then switch path.
    result = memtable_snap_path_switch_next_path(snap_path);

	if(kInternalError == result)
	{
		return ART_COMPARE_RESULT_ERR;
	}
    // if kArtIteratorEnd == result, path will be empty.
    return kArtIteratorContinue == result ? ART_COMPARE_RESULT_GREATER : ART_COMPARE_RESULT_LESS;
}



/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_LESS means leaf in path equal/smaller than input.snapid, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
/*static int32_t memtable_snap_path_do_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid)
{
    art_snap_node *cur_node                 = NULL;
    art_snap_node *next_node                = NULL;

    int32_t result                          = ART_COMPARE_RESULT_EQUAL; // art return code
    int32_t ret                             = kOk; // pelagodb return code

    uint32_t idx_in_parent                  = 0;
    uint8_t key                             = 0;
    uint32_t depth                          = 0;
    uint32_t found_snap_id                  = 0;

    cur_node = start_node;

    MEMTABLE_SNAP_PATH_CHECK_NODE(cur_node, ART_COMPARE_RESULT_ERR);

    while(is_snap_inner_node(cur_node))
    {
        depth = get_prefix_len_in_snap_path(snap_path);
        
        // get next equal or bigger child
        result = memtable_snap_search_node_equal_or_bigger(cur_node, snapshotid, SNAPID_LEN, depth, &next_node, &idx_in_parent);
        if(ART_COMPARE_RESULT_EQUAL == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            cur_node = next_node;
            pos_in_parent = idx_in_parent;
            continue;
        }
        
        if(ART_COMPARE_RESULT_LESS == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            // get its right most path.
            ret = memtable_snap_path_get_right_most_path(snap_path, next_node, idx_in_parent);
            assert(kOk == ret);
            break;
        }
        
        // ART_COMPARE_RESULT_LESS == result. caller needs to switch path at last node in path use child_pos.
        break;
    }

    if(ART_COMPARE_RESULT_EQUAL != result)
    {
        return result;
    }

    assert(is_art_leaf_node(cur_node));
    found_snap_id = snap_path->art_ops->leaf_ops.get_snap_id(cur_node);
    if(found_snap_id > snapshotid)
    {
        // not found.
        result = ART_COMPARE_RESULT_GREATER;
    }
    else if(found_snap_id == snapshotid)
    {
        // find equal
        push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
        result = ART_COMPARE_RESULT_EQUAL;
    }
    else
    {
        // find smaller
        push_node_into_snap_path(snap_path, (void *)cur_node, pos_in_parent);
        result = ART_COMPARE_RESULT_LESS;
    }

    UNREFERENCE_PARAM(ret);
    return result;
}
*/

/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @return ART_COMPARE_RESULT
 */
/*int32_t memtable_snap_path_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN art_snap_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid)
{
    int32_t result                          = ART_COMPARE_RESULT_EQUAL;

    result = memtable_snap_path_do_get_smaller_or_equal(snap_path, start_node, pos_in_parent, snapshotid);
    if(ART_COMPARE_RESULT_GREATER != result)
    {
        return result;
    }

    // in this case, last node in path is bigger than required. pop it from path, then switch path.
    result = memtable_snap_path_switch_prev_path(snap_path);

    // if kArtIteratorEnd == result, path will be empty.
    return kArtIteratorContinue == result ? ART_COMPARE_RESULT_LESS : ART_COMPARE_RESULT_GREATER;
}*/

void memtable_clear_snap_path(art_snap_path_t *snap_path)
{
    while(!check_snap_path_empty(snap_path))
    {
        (void)pop_node_from_snap_path(snap_path);
    }
    clear_snap_path_prefix(snap_path);
}

#ifdef __cplusplus
}
#endif



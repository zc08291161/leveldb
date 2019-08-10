#ifndef _SCM_PCLINT_
#include <assert.h>
#include "art_log.h"
#include "art_sstable.h"
#include "art_snapshot.h"
#include "art_snap_path_impl.h"
#include "art_snap_path_sstable.h"
#include "art_sfmea.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif


#define SSTABLE_SNAP_PATH_CHECK_NODE(node, errode) \
    if(!is_snap_inner_node(node) && !is_art_leaf_node(node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, snap_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errode;    \
    }

#define SSTABLE_CHECK_LEAF_NODE(node, errode) \
    if(!is_art_leaf_node(node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "the leaf_node[%d] is not leaf", ((art_node*)node)->node_type);    \
        return errode;    \
    }

/**
 * get left/right most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_snap_path_get_lr_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC iterator_snap_get_lr_child_f)
{
    art_snap_node *cur_node                 = NULL;
    void *cur_origin_node                   = NULL;
    void *next_origin_node                  = NULL;
    uint8_t key                             = 0;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    int32_t result                          = kOk;

    get_buffer_f = snap_path->ops.get_buffer_f;
    
    cur_origin_node = start_origin_node;
    if(TRUE == snap_path->is_need_release)
    {
        cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    
    SSTABLE_SNAP_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    while(is_snap_inner_node(cur_node))
    {
        // 2. add node and partial to snap path
        set_node_release_status_snap_path(snap_path, snap_path->is_need_release, snap_path->buff_node);
        push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
        push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);

        // 3. get left/right child
        LVOS_TP_START(ART_TP_ITERATOR_GET_LR_SNAP_NODE_READPLOG_FILE_NOT_FOUND, &result)
        result = iterator_snap_get_lr_child_f(cur_node, snap_path->iterator, args, &next_origin_node, &pos_in_parent);
        LVOS_TP_END
        if(kOk != result)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }

        // 4. set next node child_pos and key prefix
        set_last_child_pos_in_snap_path(snap_path, pos_in_parent);
        key = snap_get_key_by_idx(cur_node, pos_in_parent);
        push_key_into_snap_path(snap_path, &key, 1);

        // 5. set child current node
        cur_origin_node = next_origin_node;
        if(TRUE == snap_path->is_need_release)
        {
            cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
        }
        else
        {
            cur_node = cur_origin_node;
        }
    }

    SSTABLE_CHECK_LEAF_NODE(cur_node, kIOError);
    
    // 6. push last leaf to path
    set_node_release_status_snap_path(snap_path, snap_path->is_need_release, snap_path->buff_node);    
    push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);

    return kOk;
}


/**
 * get left most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_snap_path_get_left_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args)
{
    return sstable_snap_path_get_lr_most_path(snap_path, start_origin_node, pos_in_parent, args, iterator_sstable_snap_get_node_left_child);
}


/**
 * get right most path for parent_node
 * @arg art_snap_path_t pointer to path
 * @arg start_origin_node start node
 * @arg parent node position in inner node
 * @arg args, read arg
 * @return kOk means ok, otherwise something wrong.
 */
/*int32_t sstable_snap_path_get_right_most_path(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node, // snap node or leaf
    IN uint32_t pos_in_parent,
    IN void *args)
{
    return sstable_snap_path_get_lr_most_path(snap_path, start_origin_node, pos_in_parent, args, sstable_snap_get_node_right_child);
}*/


/**
 * switch path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @arg snap_get_lr_child_f function pointer for get left/right child
 * @arg snap_get_next_child_f function pointer for get next/prev child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_snap_path_switch_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f,
    OUT int32_t *inner_result)
{
    void *cur_origin_node                   = NULL;
    art_snap_node *cur_node                 = NULL;
    void *next_origin_node                  = NULL;

    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    uint8_t key                             = 0;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    int32_t result                          = kOk;

    get_buffer_f = snap_path->ops.get_buffer_f;
    release_buffer_f = snap_path->ops.release_buffer_f;

    if(check_snap_path_empty(snap_path))
    {
        *inner_result = kArtIteratorEnd;
        return kOk;
    }

    iterator_sstable_snap_path_get_buff_node_info(snap_path, &snap_path->buff_node, &snap_path->is_need_release);
    cur_origin_node = get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
    if(TRUE == snap_path->is_need_release)
    {
        cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    
    SSTABLE_SNAP_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    // 1. leaf node need to pop from path
    if(is_art_leaf_node(cur_node))
    {
        cur_origin_node = pop_node_from_snap_path(snap_path);
        //release_buffer_f(cur_origin_node);
        release_read_buff(snap_path->buff_node, cur_origin_node, snap_path->is_need_release, release_buffer_f);

        // check empty again
        if(check_snap_path_empty(snap_path))
        {
            *inner_result = kArtIteratorEnd;
            return kOk;
        }

        iterator_sstable_snap_path_get_buff_node_info(snap_path, &snap_path->buff_node, &snap_path->is_need_release);
        cur_origin_node = get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
        if(TRUE == snap_path->is_need_release)
        {
            cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
        }
        else
        {
            cur_node = cur_origin_node;
        }
    }
 
    // 2. backtrace in snap tree
    while(is_snap_inner_node(cur_node))
    {
        // backtrace prev key
        pop_key_from_snap_path(snap_path, 1);
        
        // get next/prev snap child
		LVOS_TP_START(ART_TP_ITERATOR_GET_LR_SNAP_NODE_READPLOG_FILE_NOT_FOUND, &result)
        result = snap_get_next_child_f(cur_node, child_pos, (art_iterator *)snap_path->iterator, args, &next_origin_node, &pos_in_parent);
		LVOS_TP_END
        // no next child, need to backtrace
        if(result != kOk)
        {
            if (result != kArtSnapIdNotFound)
            {
                return result;
            }
            // no next child, need to backtrace
            iterator_sstable_snap_path_get_buff_node_info(snap_path, &snap_path->buff_node, &snap_path->is_need_release);
            cur_origin_node = pop_node_from_snap_path(snap_path);
            pop_key_from_snap_path(snap_path, cur_node->partial_len);
            //release_buffer_f(cur_origin_node);
            release_read_buff(snap_path->buff_node, cur_origin_node, snap_path->is_need_release, release_buffer_f);

            if(check_snap_path_empty(snap_path))
            {
                *inner_result = kArtIteratorEnd;
                return kOk;
            }

            // get parent node in path
            iterator_sstable_snap_path_get_buff_node_info(snap_path, &snap_path->buff_node, &snap_path->is_need_release);
            cur_origin_node = get_last_node_in_snap_path(snap_path, &pos_in_parent, &child_pos, &parent_idx);
            if(TRUE == snap_path->is_need_release)
            {
                cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
            }
            else
            {
                cur_node = cur_origin_node;
            }
            continue;
        }
        
        // set next node child_pos and key prefix
        set_last_child_pos_in_snap_path(snap_path, pos_in_parent);
        key = snap_get_key_by_idx(cur_node, pos_in_parent);
        push_key_into_snap_path(snap_path, &key, 1);

        // get next node left/right most path
        result = sstable_snap_path_get_lr_most_path(snap_path, next_origin_node, pos_in_parent, args, snap_get_lr_child_f);
        if(kNotFound == result)
        {
            return kInternalError;
        }

        if(kOk != result)
        {
            return result;
        }
        break;
    }

    *inner_result = kArtIteratorContinue;
    return kOk;
}


/**
 * switch prev path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
/*int32_t sstable_snap_path_switch_prev_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args)
{
    return sstable_snap_path_switch_path(snap_path, args, sstable_snap_get_node_right_child, sstable_snap_get_node_prev_child);
}*/


/**
 * switch next path in snap tree
 * @arg snap_path ,last node in path is leaf or snap inner node
 * @arg args, read arg
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_snap_path_switch_next_path(
    IO art_snap_path_t *snap_path, // last node in path is leaf or snap node
    IN void *args,
    OUT int32_t *inner_result)
{
    return sstable_snap_path_switch_path(snap_path, args, iterator_sstable_snap_get_node_left_child, iterator_sstable_snap_get_node_next_child, inner_result);
}



/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means leaf in path equal/bigger than input.snapid, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
static int32_t sstable_snap_path_do_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args,
    OUT int32_t* inner_result)
{
    void *cur_origin_node                   = NULL;
    art_snap_node *cur_node                 = NULL;
    void *next_origin_node                  = NULL;
    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0;
    uint8_t key                             = 0;
    uint32_t depth                          = 0;
    uint32_t found_snap_id                  = 0;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;
    bool is_need_release                    = false;
    void* buff_node                         = NULL;
    
    get_buffer_f = snap_path->ops.get_buffer_f;
    release_buffer_f = snap_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node;
    if(TRUE == snap_path->is_need_release)
    {
        cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    SSTABLE_SNAP_PATH_CHECK_NODE(cur_node, ART_COMPARE_RESULT_ERR);
    *inner_result = ART_COMPARE_RESULT_EQUAL;
    
    while(is_snap_inner_node(cur_node))
    {
        is_need_release = snap_path->is_need_release;
        buff_node       = snap_path->buff_node;
        
        depth = get_prefix_len_in_snap_path(snap_path);
        // get next equal or bigger child
        LVOS_TP_START(ART_TP_ITERATOR_GET_LR_SNAP_NODE_READPLOG_FILE_NOT_FOUND, &result)
        result = iterator_sstable_snap_search_node_equal_or_bigger(snap_path->iterator, cur_node, snapshotid, SNAPID_LEN, depth, args, &next_origin_node, &idx_in_parent, inner_result);
        LVOS_TP_END
        if(kOk != result)
        {
            release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
            return result;
        }

        if(ART_COMPARE_RESULT_EQUAL == *inner_result)
        {
            // add current node and partial to snap path
            set_node_release_status_snap_path(snap_path, is_need_release, buff_node);            
            push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            cur_origin_node = next_origin_node;
            if (TRUE == snap_path->is_need_release)
            {
                cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
            }
            else
            {
                cur_node = cur_origin_node;
            }
            pos_in_parent = idx_in_parent;
            continue;
        }
        
        if(ART_COMPARE_RESULT_GREATER == *inner_result)
        {
            // add current node and partial to snap path
            set_node_release_status_snap_path(snap_path, is_need_release, buff_node);
            push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            // get its left most path.
            result = sstable_snap_path_get_left_most_path(snap_path, next_origin_node, idx_in_parent, args);
            if(kOk != result)
            {
                return result;
            }
            break;
        }
        
        // ART_COMPARE_RESULT_LESS == result. caller needs to switch path at last node in path use child_pos.
        release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
        break;
    }

    if(ART_COMPARE_RESULT_EQUAL != *inner_result)
    {
        return kOk;
    }

    assert(is_art_leaf_node(cur_node));
    found_snap_id = snap_path->art_ops->leaf_ops.get_snap_id(cur_node);
    if(found_snap_id > snapshotid)
    {
        set_node_release_status_snap_path(snap_path, snap_path->is_need_release, snap_path->buff_node);
        push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
        *inner_result = ART_COMPARE_RESULT_GREATER;
    }
    else if(found_snap_id == snapshotid)
    {
        set_node_release_status_snap_path(snap_path, snap_path->is_need_release, snap_path->buff_node);
        push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
        *inner_result = ART_COMPARE_RESULT_EQUAL;
    }
    else
    {
        release_read_buff(snap_path->buff_node, cur_origin_node, snap_path->is_need_release, release_buffer_f);
        *inner_result = ART_COMPARE_RESULT_LESS;
    }

    return kOk;
}


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT
 */
int32_t sstable_snap_path_get_bigger_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args,
    OUT int32_t *inner_result)
{
    //int32_t result                          = ART_COMPARE_RESULT_EQUAL;
    int32_t result                          = kOk;

    result = sstable_snap_path_do_get_bigger_or_equal(snap_path, start_origin_node, pos_in_parent, snapshotid, args, inner_result);
    if(kOk != result || ART_COMPARE_RESULT_LESS != *inner_result)
    {
        return result;
    }
    

    // in this case, last node in path is smaller than required. pop it from path, then switch path.
    result = sstable_snap_path_switch_next_path(snap_path, args, inner_result);
    if(kOk != result)
    {
        return result;
    }

    // if kArtIteratorEnd == result, path will be empty.
    *inner_result = (kArtIteratorContinue == *inner_result ? ART_COMPARE_RESULT_GREATER : ART_COMPARE_RESULT_LESS);
    return kOk;
}



/**
 * get a path that snapid smaller or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_LESS means leaf in path equal/smaller than input.snapid, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
/*static int32_t sstable_snap_path_do_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args)
{
    void *cur_origin_node                   = NULL;
    art_snap_node *cur_node                 = NULL;
    void *next_origin_node                  = NULL;

    int32_t result                          = ART_COMPARE_RESULT_EQUAL;
    int32_t ret                             = kOk;

    uint32_t idx_in_parent                  = 0;
    
    uint8_t key                             = 0;
    uint32_t depth                          = 0;

    uint32_t found_snap_id                  = 0;

    READ_PLOG_FUNC read_plog_f              = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    read_plog_f = snap_path->ops.read_plog_f;
    get_buffer_f = snap_path->ops.get_buffer_f;
    release_buffer_f = snap_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node;
    cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);

    SSTABLE_SNAP_PATH_CHECK_NODE(cur_node, ART_COMPARE_RESULT_ERR);

    while(is_snap_inner_node(cur_node))
    {
        depth = get_prefix_len_in_snap_path(snap_path);
        // get next equal or bigger child
        result = sstable_snap_search_node_equal_or_smaller(cur_node, snapshotid, SNAPID_LEN, depth, read_plog_f, args, &next_origin_node, &idx_in_parent);

        if(ART_COMPARE_RESULT_EQUAL == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            cur_origin_node = next_origin_node;
            cur_node = (art_snap_node *)get_buffer_f(cur_origin_node);
            pos_in_parent = idx_in_parent;
            continue;
        }
        
        if(ART_COMPARE_RESULT_LESS == result)
        {
            // add current node and partial to snap path
            push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
            push_key_into_snap_path(snap_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix
            set_last_child_pos_in_snap_path(snap_path, idx_in_parent);
            key = snap_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_snap_path(snap_path, &key, 1);

            // get its left most path.
            ret = sstable_snap_path_get_right_most_path(snap_path, next_origin_node, idx_in_parent, args);
            assert(kOk == ret);
            break;
        }
        
        // ART_COMPARE_RESULT_LESS == result. caller needs to switch path at last node in path use child_pos.
        release_buffer_f(cur_origin_node);
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
        release_buffer_f(cur_origin_node);
        result = ART_COMPARE_RESULT_GREATER;
    }
    else if(found_snap_id == snapshotid)
    {
        push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
        result = ART_COMPARE_RESULT_EQUAL;
    }
    else
    {
        push_node_into_snap_path(snap_path, cur_origin_node, pos_in_parent);
        result = ART_COMPARE_RESULT_LESS;
    }

    UNREFERENCE_PARAM(ret);
    return result;

}*/


/**
 * get a path that snapid bigger or equal than input.snapid
 * @arg snap_path pointer to snap path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg snapshotid
 * @arg args read plog para
 * @return ART_COMPARE_RESULT
 */
/*int32_t sstable_snap_path_get_smaller_or_equal(
    IO art_snap_path_t *snap_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint32_t snapshotid,
    IN void *args)
{
    int32_t result                          = ART_COMPARE_RESULT_EQUAL;

    result = sstable_snap_path_do_get_smaller_or_equal(snap_path, start_origin_node, pos_in_parent, snapshotid, args);
    if(ART_COMPARE_RESULT_GREATER != result)
    {
        return result;
    }

    // in this case, last node in path is smaller than required. pop it from path, then switch path.
    result = sstable_snap_path_switch_prev_path(snap_path, args);

    // if kArtIteratorEnd == result, path will be empty.
    return kArtIteratorContinue == result ? ART_COMPARE_RESULT_LESS : ART_COMPARE_RESULT_GREATER;
}*/

void sstable_clear_snap_path(art_snap_path_t *snap_path)
{
	void *node = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f = snap_path->ops.release_buffer_f;
    void* buff_node = NULL;
    bool is_need_release = FALSE;
    
    while(!check_snap_path_empty(snap_path))
    {
        iterator_sstable_snap_path_get_buff_node_info(snap_path, &buff_node, &is_need_release);
        
        node = pop_node_from_snap_path(snap_path);
        
        release_read_buff(buff_node, node, is_need_release, release_buffer_f);
    }
    clear_snap_path_prefix(snap_path);
}

#ifdef __cplusplus
}
#endif


#ifndef _SCM_PCLINT_
#include "art_log.h"
#include "art_sstable.h"
#include "art_key_path_impl.h"
#include "art_key_path_sstable.h"
#include "art_sfmea.h"
#ifndef WIN32_DEBUG
#include "logging.h"
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif


#define SSTABLE_KEY_PATH_CHECK_FUSION_NODE(node, errcode) \
    if(!is_sstable_fusion_node((void *)node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, fusion_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errcode;    \
    }


#define SSTABLE_KEY_PATH_CHECK_ART_INNER_NODE(node, errcode) \
    if(!is_art_inner_node((void *)node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, inner_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errcode;    \
    }


#define SSTABLE_KEY_PATH_CHECK_ART_INNER_OR_FUSION_NODE(node, errcode) \
    if(!is_art_inner_node((void *)node) && !is_sstable_fusion_node((void *)node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, fusion_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errcode;    \
    }

/**
 * sstable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @arg fusion_get_lr_child_f function pointer for get left/right art child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
static int32_t sstable_key_path_get_lr_most_path_in_fusion_node(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC iterator_fusion_get_lr_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    int32_t result                          = kNotFound;
    uint8_t key                             = 0;

    get_buffer_f = key_path->ops.get_buffer_f;
    
    cur_origin_node = start_origin_node; 
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    
    SSTABLE_KEY_PATH_CHECK_FUSION_NODE(cur_node, kInvalidArgument);

    // 1. add current node and partial to key path
    set_node_release_status_key_path(key_path, key_path->is_need_release, key_path->buff_node);
    push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
    push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

    // 2. get left/right child
    LVOS_TP_START(ART_TP_ITERATOR_GET_LR_FUSION_CHILD_READPLOG_FILE_NOT_FOUND, &result)
    result = iterator_fusion_get_lr_child_f(cur_node, key_path->iterator, args, &next_origin_node, &pos_in_parent);
    LVOS_TP_END
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child in fusion node failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
        return result;
    }

    // 3. set next node pos and key prefix
    assert(END_POSITION_INDEX != pos_in_parent);
    key = art_get_key_by_idx(cur_node, pos_in_parent);
    push_key_into_key_path(key_path, &key, 1);

    //verify fusion node key
    result = sstable_fusion_node_verify_key(NULL, cur_node, key_path->prefix, key_path->prefix_len);
    if (kOk != result)
    {
        //key verify fail, return kArtVerifyKeyInvalid
        return result;
    }

    set_last_child_pos_in_key_path(key_path, pos_in_parent);
    if(next_origin_node == cur_node)
    {
        // next node is current node, leaf in fusion node. no next child
        *snap_origin_node = NULL;
    }
    else
    {
        *snap_origin_node = next_origin_node;
        *snap_pos_in_parent = pos_in_parent;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    }

    return kOk;
}


/**
 * sstable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_key_path_get_lr_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC iterator_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC iterator_fusion_get_lr_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;
    
    int32_t result                          = kNotFound;
    uint8_t key                             = 0;

    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node; 
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    // 1. check input start node type
    if(!is_art_inner_node(cur_node) && !is_sstable_fusion_node((void *)cur_node))
    {
        *snap_origin_node = cur_origin_node;
        *snap_pos_in_parent = pos_in_parent;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
        return kOk;
    }
        
    while(is_art_inner_node(cur_node))
    {
		/* check snapid range */
		if ((GetType == type) && (kPelagoDBSystemTable != t->apply_type))
		{
			result = art_check_snapid_range(snapshotid, cur_node, t);
			if (kOk != result)
			{
                //release_buffer_f(cur_origin_node);
                release_read_buff(key_path->buff_node, cur_origin_node, key_path->is_need_release, release_buffer_f);
				ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable key path get bigger or equal, foundSnapid[%u] is more than maxSnapid.",
					         snapshotid);
				return result;
			}
		}
        
        // 2. add node and partial to key path
        set_node_release_status_key_path(key_path, key_path->is_need_release, key_path->buff_node);        
        push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
        push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

        // 3. get left/right child
        LVOS_TP_START(ART_TP_ITERATOR_GET_LR_CHILD_READPLOG_FILE_NOT_FOUND, &result)
        result = iterator_get_lr_child_f(cur_node, (art_iterator*)key_path->iterator, args, &next_origin_node, &pos_in_parent);
        LVOS_TP_END
        if(kOk != result)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }

        // 4. set next node child_pos and key prefix
        set_last_child_pos_in_key_path(key_path, pos_in_parent);
        if(END_POSITION_INDEX != pos_in_parent)
        {
            key = art_get_key_by_idx(cur_node, pos_in_parent);
            push_key_into_key_path(key_path, &key, 1);
        }

        // 5. set child current node
        cur_origin_node = next_origin_node;
        if(TRUE == key_path->is_need_release)
        {
            cur_node = (art_node *)get_buffer_f(cur_origin_node);
        }
        else
        {
            cur_node = cur_origin_node;
        }
    }

    // handle last node fusion node
    if(is_sstable_fusion_node((void *)cur_node))
    {
        return sstable_key_path_get_lr_most_path_in_fusion_node(key_path, cur_origin_node, pos_in_parent, args,
            iterator_fusion_get_lr_child_f, snap_origin_node, snap_pos_in_parent);
    }

    // handle last node snap/leaf node
    *snap_origin_node = cur_origin_node;
    *snap_pos_in_parent = pos_in_parent;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;

    return kOk;
}


/**
 * sstable get key left most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_key_path_get_left_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return sstable_key_path_get_lr_most_path(type, t, snapshotid, key_path, start_origin_node, pos_in_parent, args, iterator_sstable_get_node_left_child, iterator_sstable_get_fusion_node_left_child, snap_origin_node, snap_pos_in_parent);
}


/**
 * sstable get key right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
/*int32_t sstable_key_path_get_right_most_path(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return sstable_key_path_get_lr_most_path(key_path, start_origin_node, pos_in_parent, args, sstable_get_node_right_child, sstable_get_fusion_node_right_child, snap_origin_node, snap_pos_in_parent);
}*/


/**
 * sstable switch key path in fusion node
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @arg fusion_get_next_child_f function pointer for get next/prev in fusion child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
static int32_t sstable_key_path_switch_path_in_fusion_node(
    IO art_key_path_t *key_path,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_NEXT_FUNC fusion_get_next_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    //int32_t result                          = kNotFound;
    int32_t result                          = kOk;
    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx_in_path             = 0;
    uint8_t key                             = 0;

    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;
    
    // 1. get fusion node in path
    iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
    cur_origin_node = get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx_in_path);
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    SSTABLE_KEY_PATH_CHECK_FUSION_NODE(cur_node, kInvalidArgument);
    
    // 2. backtrace key
    pop_key_from_key_path(key_path, 1);
    
    // 3. get next child
    LVOS_TP_START(ART_TP_ITERATOR_GET_LR_FUSION_CHILD_READPLOG_FILE_NOT_FOUND, &result)
    result = fusion_get_next_child_f(cur_node, child_pos, (art_iterator*)key_path->iterator, args, &next_origin_node, &pos_in_parent);
    LVOS_TP_END
    if(result != kOk)
    {
        if (result != kNotFound)
        {
            return result;
        }
        // no next child, need to backtrace. still remain its key prefix in path
        iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
        cur_origin_node = pop_node_from_key_path(key_path);
        pop_key_from_key_path(key_path, cur_node->partial_len);
        //release_buffer_f(cur_origin_node);
        release_read_buff(key_path->buff_node, cur_origin_node, key_path->is_need_release, release_buffer_f);

        //return kArtIteratorEnd;
        *inner_result = kArtIteratorEnd;
        result = kOk;
        return result;
    }

    // set next node child_pos and key prefix
    set_last_child_pos_in_key_path(key_path, pos_in_parent);
    key = art_get_key_by_idx(cur_node, pos_in_parent);
    push_key_into_key_path(key_path, &key, 1);
    if(next_origin_node == cur_node)
    {
        // next node is current node, leaf in fusion node. no next child
        *snap_origin_node = NULL;
    }
    else
    {
        *snap_origin_node = next_origin_node;
        *snap_pos_in_parent = pos_in_parent;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    }

    //return kArtIteratorContinue;
    *inner_result = kArtIteratorContinue;
    return result;
}


/**
 * sstable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @arg fusion_get_next_child_f function pointer for get next/prev in fusion child
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_key_path_switch_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC fusion_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_NEXT_FUNC fusion_get_next_child_f,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;
    int32_t result                          = kOk;
	*inner_result                           = kArtIteratorEnd;
    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx_in_path             = 0;
    uint8_t key                             = 0;

    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;

    if(check_key_path_empty(key_path))
    {
        *inner_result = kArtIteratorEnd;
        return result;
    }

    // 1. get last node in path
    iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
    cur_origin_node = get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx_in_path);
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    SSTABLE_KEY_PATH_CHECK_ART_INNER_OR_FUSION_NODE(cur_node, kInvalidArgument);

    // 2. may be last node is fusion node
    if(is_sstable_fusion_node((void *)cur_node))
    {
        result = sstable_key_path_switch_path_in_fusion_node(key_path, args, fusion_get_next_child_f, snap_origin_node, snap_pos_in_parent, inner_result);
        if(result != kOk)
        {
            return result;
        }
    }
    
    if(kArtIteratorContinue == *inner_result)
    {
        //return kArtIteratorContinue;
        *inner_result = kArtIteratorContinue;
        return result;
    }

    // may be last node is fusion node, and popped in switch path in fusion node.
	if (check_key_path_empty(key_path))
	{
		//return kArtIteratorEnd;
		*inner_result = kArtIteratorEnd;
        return result;
	}

    ART_ASSERT(kArtIteratorEnd == *inner_result);
    // 3. switch path in art tree
    iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
    cur_origin_node = get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx_in_path);
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    while(is_art_inner_node(cur_node))
    {
        // backtrace prev key prefix
        END_POSITION_INDEX != child_pos ? pop_key_from_key_path(key_path, 1) : child_pos;
        // get next/prev snap child
        LVOS_TP_START(ART_TP_ITERATOR_GET_LR_CHILD_READPLOG_FILE_NOT_FOUND, &result)
        result = art_get_next_child_f(cur_node, child_pos, (art_iterator*)key_path->iterator, args, &next_origin_node, &pos_in_parent);
        LVOS_TP_END
        // no next child, need to backtrace
        if(result != kOk)
        {
            if (result != kNotFound)
            {
                return result;
            }
            // no next child, need to backtrace
            iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
            cur_origin_node = pop_node_from_key_path(key_path);
            pop_key_from_key_path(key_path, cur_node->partial_len);
            //release_buffer_f(cur_origin_node);
            release_read_buff(key_path->buff_node, cur_origin_node, key_path->is_need_release, release_buffer_f);

            if(check_key_path_empty(key_path))
            {
                //return kArtIteratorEnd;
                *inner_result = kArtIteratorEnd;
                return kOk;
            }

            // get parent node in path, may be NULL
            iterator_sstable_key_path_get_buff_node_info(key_path, &key_path->buff_node, &key_path->is_need_release);
            cur_origin_node = get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx_in_path);
            if(TRUE == key_path->is_need_release)
            {
                cur_node = (art_node *)get_buffer_f(cur_origin_node);
            }
            else
            {
                cur_node = cur_origin_node;
            }
            continue;
        }
        
        // set next node child_pos and key prefix
        set_last_child_pos_in_key_path(key_path, pos_in_parent);
        if(END_POSITION_INDEX != pos_in_parent)
        {
            key = art_get_key_by_idx(cur_node, pos_in_parent);
            push_key_into_key_path(key_path, &key, 1);
        }

        // get next node left/right most path
        result = sstable_key_path_get_lr_most_path(type, t, snapshotid, key_path, next_origin_node, pos_in_parent, args, art_get_lr_child_f, fusion_get_lr_child_f, snap_origin_node, snap_pos_in_parent);
        //assert(kOk == result);
        if (result != kOk)
        {
            return result;
        }
        break;
    }

    *inner_result = kArtIteratorContinue;
    return result;
}


/**
 * sstable switch prev path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
/*int32_t sstable_key_path_switch_prev_path(
    IO art_key_path_t *key_path,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return sstable_key_path_switch_path(key_path, args, sstable_get_node_right_child, sstable_get_node_prev_child, sstable_get_fusion_node_right_child, sstable_get_fusion_node_prev_child, snap_origin_node, snap_pos_in_parent);
}*/


/**
 * sstable switch next path entrance
 * @arg key_path, pointer to key path
 * @arg args read plog para
 * @out snap root/leaf node, NULL is possible
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_key_path_switch_next_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapshotid,
    IO art_key_path_t *key_path,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    return sstable_key_path_switch_path(type, t, snapshotid, key_path, args, iterator_sstable_get_node_left_child, iterator_sstable_get_node_next_child, iterator_sstable_get_fusion_node_left_child, iterator_sstable_get_fusion_node_next_child, snap_origin_node, snap_pos_in_parent, inner_result);
}


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
static int32_t sstable_key_path_do_get_bigger_or_equal_in_fusion_node(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;

    //int32_t result                          = ART_COMPARE_RESULT_EQUAL;
    //int32_t ret                             = kOk;
    int32_t result                            = kOk;

    uint32_t idx_in_parent                  = 0;    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;
    bool is_need_release                    = key_path->is_need_release;
    void* buff_node                         = key_path->buff_node;

    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node;
	if (TRUE == key_path->is_need_release)
	{
		cur_node = (art_node *)get_buffer_f(cur_origin_node);
	}
	else
	{
		cur_node = cur_origin_node;
	}
    
    SSTABLE_KEY_PATH_CHECK_FUSION_NODE(cur_node, ART_COMPARE_RESULT_ERR);

    depth = get_prefix_len_in_key_path(key_path);
    // get next equal or bigger child
    LVOS_TP_START(ART_TP_ITERATOR_GET_LR_FUSION_CHILD_READPLOG_FILE_NOT_FOUND, &result)
    result = iterator_sstable_search_fusion_node_equal_or_bigger(cur_node, (art_iterator*)key_path->iterator, key, key_len, depth, snapshotid, args, &next_origin_node, &idx_in_parent, inner_result);
    LVOS_TP_END
    if (result != kOk)
    {
        release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
        return result;
    }
    if(ART_COMPARE_RESULT_LESS == *inner_result)
    {
        // caller needs switch path at last node in path use child_pos.
		release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
        //return ART_COMPARE_RESULT_LESS;
        *inner_result = ART_COMPARE_RESULT_LESS;
        return result;
    }

    // add current node and partial to key path
    set_node_release_status_key_path(key_path, is_need_release, buff_node);
    push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
    push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

    // set next node child_pos and key prefix
    set_last_child_pos_in_key_path(key_path, idx_in_parent);
    c = art_get_key_by_idx(cur_node, idx_in_parent);
    push_key_into_key_path(key_path, &c, 1);
    //verify fusion node key
    result = sstable_fusion_node_verify_key(NULL, cur_node, key_path->prefix, key_len);
    if (kOk != result)
    {
        //release fusion next child
        if (next_origin_node != cur_node)
        {
            release_read_buff(key_path->buff_node, next_origin_node, key_path->is_need_release, release_buffer_f);
        }
        //key verify fail, return kArtVerifyKeyInvalid
        return result;
    }
    if(next_origin_node == cur_node)
    {
        // next node is current node, leaf in fusion node. no next child
        *snap_origin_node = NULL;
    }
    else
    {
        *snap_origin_node = next_origin_node;
        *snap_pos_in_parent = idx_in_parent;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    }
    
    if(ART_COMPARE_RESULT_GREATER == *inner_result)
    {
        //return ART_COMPARE_RESULT_GREATER;
        *inner_result = ART_COMPARE_RESULT_GREATER;
        return result;
    }

    depth = get_prefix_len_in_key_path(key_path);
    if(key_len > depth)
    {
		release_read_buff(key_path->buff_node, next_origin_node, key_path->is_need_release, release_buffer_f);
        *inner_result = ART_COMPARE_RESULT_LESS;
        return result;
    }
    
    //UNREFERENCE_PARAM(ret);
    
    *inner_result = ART_COMPARE_RESULT_EQUAL;
    return result;
}


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
static int32_t sstable_key_path_do_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0;
    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;
    bool is_need_release                    = false;
    void* buff_node                         = NULL;
    
    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;
    
    cur_origin_node = start_origin_node;
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    SSTABLE_KEY_PATH_CHECK_ART_INNER_OR_FUSION_NODE(cur_node, ART_COMPARE_RESULT_ERR);

    while(is_art_inner_node(cur_node))
    {
        is_need_release = key_path->is_need_release;
        buff_node       = key_path->buff_node;
        /* check snapid range */
		if ((GetType == type) && (kPelagoDBSystemTable != t->apply_type))
		{
			result = art_check_snapid_range(snapshotid, cur_node, t);
			if (kOk != result)
			{
#ifndef WIN32
				INDEX_LOG_INFO_LIMIT("memtable key path get bigger or equal, foundSnapid[%u] is more than maxSnapid key_len[%u] depth[%u].",
					                 snapshotid, key_len, depth);
#endif
				release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
                *inner_result = ART_COMPARE_RESULT_LESS;
				return kOk;
			}
		}
        
        depth = get_prefix_len_in_key_path(key_path);
        // get next equal or bigger child
        result = iterator_sstable_search_node_equal_or_bigger((art_iterator*)key_path->iterator, cur_node,
            key, key_len, depth, args, &next_origin_node, &idx_in_parent, inner_result);
        if (result != kOk)
        {
			release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
            return result;
        }
        if(ART_COMPARE_RESULT_EQUAL == *inner_result)
        {
            // add current node and partial to key path
            set_node_release_status_key_path(key_path, is_need_release, buff_node);
            push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
            push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix if not end node
            set_last_child_pos_in_key_path(key_path, idx_in_parent);
            if(END_POSITION_INDEX != idx_in_parent)
            {
                c = art_get_key_by_idx(cur_node, idx_in_parent);
                push_key_into_key_path(key_path, &c, 1);
            }

            cur_origin_node = next_origin_node;
            if(TRUE == key_path->is_need_release)
            {
                cur_node = (art_node *)get_buffer_f(cur_origin_node);
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
            // add current node and partial to key path
            set_node_release_status_key_path(key_path, is_need_release, buff_node);            
            push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
            push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix
            set_last_child_pos_in_key_path(key_path, idx_in_parent);
            if(END_POSITION_INDEX != idx_in_parent)
            {
                c = art_get_key_by_idx(cur_node, idx_in_parent);
                push_key_into_key_path(key_path, &c, 1);
            }

            // get its left most path
            result = sstable_key_path_get_left_most_path(type, t, snapshotid, key_path, next_origin_node, idx_in_parent, args, snap_origin_node, snap_pos_in_parent);
            if(kArtSnapIdNotFound == result)
            {
                *inner_result = ART_COMPARE_RESULT_LESS;
				return kOk;
            }
            break;
        }
        
        // ART_COMPARE_RESULT_LESS == result. caller needs to switch path at last node in path use child_pos.
		release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
        break;
    }

    if(ART_COMPARE_RESULT_EQUAL != *inner_result)
    {
        return result;
    }

    if(is_sstable_fusion_node((void *)cur_node))
    {
        return sstable_key_path_do_get_bigger_or_equal_in_fusion_node(key_path, cur_origin_node, idx_in_parent, key, key_len, snapshotid, args, snap_origin_node, snap_pos_in_parent, inner_result);
    }

    depth = get_prefix_len_in_key_path(key_path);
    if(key_len > depth)
    {
		release_read_buff(key_path->buff_node, cur_origin_node, key_path->is_need_release, release_buffer_f);
        *inner_result = ART_COMPARE_RESULT_LESS;
        return result;
    }
    
    // handle last node snap/leaf node
    *snap_origin_node = cur_origin_node;
    *snap_pos_in_parent = pos_in_parent;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    *inner_result = ART_COMPARE_RESULT_EQUAL;
    return result;

}


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t sstable_key_path_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent,
    OUT int32_t *inner_result)
{
    int32_t result                          = kOk;

    result = sstable_key_path_do_get_bigger_or_equal(key_path, start_origin_node, pos_in_parent, key, key_len, snapshotid, type, t, args, snap_origin_node, snap_pos_in_parent, inner_result);
    //if(ART_COMPARE_RESULT_LESS != result)
    if (result != kOk || ART_COMPARE_RESULT_LESS != *inner_result)
    {
        return result;
    }

    // in this case, last node in path is smaller than required. switch path at last node use child_pos.
    result = sstable_key_path_switch_next_path(type, t, snapshotid, key_path, args, snap_origin_node, snap_pos_in_parent, inner_result);
    /* if result = kArtSnapIdNotFound, so found_snapid is more than maxSnapid */
    while (kArtSnapIdNotFound == result)
    {
        result = sstable_key_path_switch_next_path(type, t, snapshotid, key_path, args, snap_origin_node, snap_pos_in_parent, inner_result);
    }
	if(result != kOk)
    {
        return result;
    }
    
    //return kArtIteratorContinue == result ? ART_COMPARE_RESULT_GREATER : ART_COMPARE_RESULT_LESS;
    *inner_result = (kArtIteratorContinue == *inner_result ? ART_COMPARE_RESULT_GREATER : ART_COMPARE_RESULT_LESS);
    return result;
}


/**
 * get a path that key equal than input.key in fusion node
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return return kok means found, or other code.
 */
static int32_t sstable_key_path_get_equal_in_fusion_node(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0;
    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;
    bool is_need_release                    = key_path->is_need_release;
    void* buff_node                         = key_path->buff_node;
    
    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node;
    if (TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }

    SSTABLE_KEY_PATH_CHECK_FUSION_NODE(cur_node, kInvalidArgument);

    depth = get_prefix_len_in_key_path(key_path);
    
    result = iterator_sstable_search_fusion_node_equal(cur_node, key, key_len, depth, (art_iterator*)key_path->iterator, args, &next_origin_node, &idx_in_parent);
    if(kOk != result)
    {
        // not found
        release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
        //release_buffer_f(cur_origin_node);
        return result;
    }

    // add current node and partial to key path
    set_node_release_status_key_path(key_path, is_need_release, buff_node);    
    push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
    push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

    // set next node child_pos and key prefix
    set_last_child_pos_in_key_path(key_path, idx_in_parent);
    c = art_get_key_by_idx(cur_node, idx_in_parent);
    push_key_into_key_path(key_path, &c, 1);
    //verify fusion node key
    result = sstable_fusion_node_verify_key(NULL, cur_node, key_path->prefix, key_path->prefix_len);
    if (kOk != result)
    {
        //release fusion next child
        if (next_origin_node != cur_node)
        {
            release_read_buff(key_path->buff_node, next_origin_node, key_path->is_need_release, release_buffer_f);
        }
        //key verify fail, return kArtVerifyKeyInvalid
        return result;
    }
    depth = get_prefix_len_in_key_path(key_path);
    if(depth != key_len && cur_node == next_origin_node)
    {
        // not found
        return kNotFound;
    }

    if(depth != key_len && cur_node != next_origin_node)
    {
        // not found
        release_read_buff(key_path->buff_node, next_origin_node, key_path->is_need_release, release_buffer_f);
        //release_buffer_f(next_origin_node);
        return kNotFound;
    }
    
    if(next_origin_node == cur_node)
    {
        // next node is current node, leaf in fusion node. no next child
        *snap_origin_node = NULL;
    }
    else
    {
        *snap_origin_node = next_origin_node;
        *snap_pos_in_parent = idx_in_parent;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
        ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    }

    return kOk;
}


/**
 * get a path that key equal than input.key
 * @arg key_path pointer to key path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return return kok means found, or other code.
 */
int32_t sstable_key_path_get_equal(
    IO art_key_path_t *key_path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args,
    OUT void **snap_origin_node,
    OUT uint32_t *snap_pos_in_parent)
{
    void *cur_origin_node                   = NULL;
    art_node *cur_node                      = NULL;
    void *next_origin_node                  = NULL;
    GET_BUFFER_FUNC get_buffer_f            = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f    = NULL;

    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0;
    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;
    bool is_need_release                    = false;
    void* buff_node                         = NULL;
    get_buffer_f = key_path->ops.get_buffer_f;
    release_buffer_f = key_path->ops.release_buffer_f;

    cur_origin_node = start_origin_node;
    if(TRUE == key_path->is_need_release)
    {
        cur_node = (art_node *)get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    
    SSTABLE_KEY_PATH_CHECK_ART_INNER_OR_FUSION_NODE(cur_node, kInvalidArgument);

    while(is_art_inner_node(cur_node))
    {
        is_need_release = key_path->is_need_release;
        buff_node       = key_path->buff_node;
        
        depth = get_prefix_len_in_key_path(key_path);
        // get next equal
        result = iterator_sstable_search_node_equal(cur_node, key_path->iterator, key, key_len,
            depth, args, &next_origin_node, &idx_in_parent);
        if(kOk != result)
        {
            //release_buffer_f(cur_origin_node);
            release_read_buff(buff_node, cur_origin_node, is_need_release, release_buffer_f);
            break;
        }
        
        // add current node and partial to key path
        set_node_release_status_key_path(key_path, is_need_release, buff_node);        
        push_node_into_key_path(key_path, cur_origin_node, pos_in_parent);
        push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

        // set next node child_pos and key prefix
        set_last_child_pos_in_key_path(key_path, idx_in_parent);
        if(END_POSITION_INDEX != idx_in_parent)
        {
            c = art_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_key_path(key_path, &c, 1);
        }

        // set child current node
        pos_in_parent = idx_in_parent;
        cur_origin_node = next_origin_node;
        if(TRUE == key_path->is_need_release)
        {
            cur_node = (art_node *)get_buffer_f(cur_origin_node);
        }
        else
        {
            cur_node = cur_origin_node;
        }
    }

    if(kOk != result)
    {
        return result;
    }

    // handle last fusion node
    if(is_sstable_fusion_node((void *)cur_node))
    {
        return sstable_key_path_get_equal_in_fusion_node(key_path, cur_origin_node, pos_in_parent, key, key_len, args, snap_origin_node, snap_pos_in_parent);
    }

    // handle last node snap/leaf
    depth = get_prefix_len_in_key_path(key_path);
    if(depth != key_len)
    {
        // input key is longger
        //release_buffer_f(cur_origin_node);
        release_read_buff(key_path->buff_node, cur_origin_node, key_path->is_need_release, release_buffer_f);
        return kNotFound;
    }

    *snap_origin_node = cur_origin_node;
    *snap_pos_in_parent = pos_in_parent;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.is_need_release = key_path->is_need_release;
    ((art_iterator*)key_path->iterator)->art_path.snap_path.buff_node = key_path->buff_node;
    
    return kOk;
}

void sstable_clear_key_path(art_key_path_t *key_path)
{
	void *node = NULL;
    RELEASE_BUFFER_FUNC release_buffer_f = key_path->ops.release_buffer_f;
    void* buff_node = NULL;
    bool is_need_release = FALSE;
    
    while(!check_key_path_empty(key_path))
    {
        iterator_sstable_key_path_get_buff_node_info(key_path, &buff_node, &is_need_release);
        
        node = pop_node_from_key_path(key_path);
        
        release_read_buff(buff_node, node, is_need_release, release_buffer_f);
    }
    clear_key_path_prefix(key_path);
}

#ifdef __cplusplus
}
#endif



#ifndef _SCM_PCLINT_
#include "art_log.h"
#include "art_memtable.h"
#include "art_key_path_impl.h"
#include "art_key_path_memtable.h"
#include "art_snapshot_sstable.h"
#include "art_sfmea.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif


#define MEMTABLE_KEY_PATH_CHECK_NODE(node, errode) \
    if(!is_art_inner_node(node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, art_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        assert(0);      \
        return errode;    \
    }

/**
 * memtable get key left/right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_key_path_get_lr_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    uint8_t key                             = 0;
    int32_t result                          = kOk;

    cur_node = start_node;
    while(is_art_inner_node(cur_node))
    {
        /* check snapid range */
		if ((GetType == type) && (kPelagoDBSystemTable != t->apply_type))
		{
			result = art_check_snapid_range(snapid, cur_node, t);
			if (kOk != result)
			{
				ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable key path get bigger or equal, foundSnapid[%u] is more than maxSnapid.",
					         snapid);
				return result;
			}
		}
        
        // 2. add node and partial to snap path
        push_node_into_key_path(key_path, (void *)cur_node, pos_in_parent);
        push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);

        LVOS_TP_START(ART_TP_MEMTABLE_NODE_ERR_RETURN_KNOTFOUND, &result)
        // 3. get left/right child
        result = art_get_lr_child_f(cur_node, &next_node, &pos_in_parent);
        LVOS_TP_END
        if(kOk != result)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }

        // 4. set next node child_pos and key prefix if not end node
        set_last_child_pos_in_key_path(key_path, pos_in_parent);
        if(END_POSITION_INDEX != pos_in_parent)
        {
            key = art_get_key_by_idx(cur_node, pos_in_parent);
            push_key_into_key_path(key_path, &key, 1);
        }
        
        // 5. set child current node
        cur_node = next_node;
    }

    *snap_node = (art_snap_node*)cur_node;
    *snap_pos_in_parent = pos_in_parent;

    return kOk;
}


/**
 * memtable get key left most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_key_path_get_left_most_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return memtable_key_path_get_lr_most_path(type, t, snapid, key_path, start_node, pos_in_parent, memtable_get_node_left_child, snap_node, snap_pos_in_parent);
}


/**
 * memtable get key right most path entrance
 * @arg key_path, pointer to key path
 * @arg start_node, start node
 * @arg pos_in_parent, start node position in parent
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kOk means ok, otherwise something wrong.
 */
/*int32_t memtable_key_path_get_right_most_path(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return memtable_key_path_get_lr_most_path(key_path, start_node, pos_in_parent, memtable_get_node_right_child, snap_node, snap_pos_in_parent);
}*/


/**
 * memtable switch next/prev path at last node in path
 * @arg key_path, pointer to key path
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_key_path_switch_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    
    uint8_t key                             = 0;

    int32_t result                          = kArtIteratorEnd;

    if(check_key_path_empty(key_path))
    {
        return kArtIteratorEnd;
    }
    
    cur_node = (art_node *)get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx);
    
    MEMTABLE_KEY_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    // 1. backtrace in art tree
    while(is_art_inner_node(cur_node))
    {
        // backtrace prev key
        END_POSITION_INDEX != child_pos ? pop_key_from_key_path(key_path, 1) : child_pos;

        // get next/prev child
        result = art_get_next_child_f(cur_node, child_pos, &next_node, &pos_in_parent);

        // no next child, need to backtrace
        if(result != kOk)
        {
            assert(kNotFound == result);
            // no next child, need to backtrace
            (void)pop_node_from_key_path(key_path);
            pop_key_from_key_path(key_path, cur_node->partial_len);
            if(check_key_path_empty(key_path))
            {
                return kArtIteratorEnd;
            }

            // get parent node in path
            cur_node = (art_node *)get_last_node_in_key_path(key_path, &pos_in_parent, &child_pos, &parent_idx);
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
        result = memtable_key_path_get_lr_most_path(type, t, snapid, key_path, next_node, pos_in_parent, art_get_lr_child_f, snap_node, snap_pos_in_parent);
        //assert(kOk == result);
        if(kNotFound == result)
        {
        	return kInternalError;
        }
        break;
    }

    /* 向上回溯时被maxsnapid挡住 节点中子节点数目为O导致返回kNotFound */
    if(kOk != result)
    {
        return result;
    }
    //UNREFERENCE_PARAM(result);
    return kArtIteratorContinue;
}


/**
 * memtable switch prev path at last node in path
 * @arg key_path, pointer to key path
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
/*int32_t memtable_key_path_switch_prev_path(
    IO art_key_path_t *key_path,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return memtable_key_path_switch_path(key_path, memtable_get_node_right_child, memtable_get_node_prev_child, snap_node, snap_pos_in_parent);
}*/


/**
 * memtable switch next path at last node in path
 * @arg key_path, pointer to key path
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_key_path_switch_next_path(
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    IO art_key_path_t *key_path,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    return memtable_key_path_switch_path(type, t, snapid, key_path, memtable_get_node_left_child, memtable_get_node_next_child, snap_node, snap_pos_in_parent);
}


 /**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
static int32_t memtable_key_path_do_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
 {
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    int32_t result                          = ART_COMPARE_RESULT_EQUAL;
    int32_t ret                             = kOk;

    uint32_t idx_in_parent                  = 0;
    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;

    cur_node = start_node;
    
    MEMTABLE_KEY_PATH_CHECK_NODE(cur_node, ART_COMPARE_RESULT_ERR);
    
    while(is_art_inner_node(cur_node))
    {
		/* check snapid range */
		if ((GetType == type) && (kPelagoDBSystemTable != t->apply_type))
		{
			ret = art_check_snapid_range(snapid, cur_node, t);
			if (kOk != ret)
			{
				ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable key path get bigger or equal, foundSnapid[%u] is more than maxSnapid key_len[%u] depth[%u].",
					         snapid, key_len, depth);
				return ART_COMPARE_RESULT_LESS;
			}
		}
        
        depth = get_prefix_len_in_key_path(key_path);
        
        // get next equal or bigger child
        result = memtable_search_node_equal_or_bigger(cur_node, key, key_len, depth, &next_node, &idx_in_parent);
        if(ART_COMPARE_RESULT_EQUAL == result)
        {
            // add current node and partial to key path
            push_node_into_key_path(key_path, (void *)cur_node, pos_in_parent);
            push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);
        
            // set next node child_pos and key prefix if not end node
            set_last_child_pos_in_key_path(key_path, idx_in_parent);
            if(END_POSITION_INDEX != idx_in_parent)
            {
                c = art_get_key_by_idx(cur_node, idx_in_parent);
                push_key_into_key_path(key_path, &c, 1);
            }
            
            cur_node = next_node;
            pos_in_parent = idx_in_parent;
            continue;
        }
        
        if(ART_COMPARE_RESULT_GREATER == result)
        {
            // add current node and partial to key path
            push_node_into_key_path(key_path, (void *)cur_node, pos_in_parent);
            push_key_into_key_path(key_path, cur_node->partial, cur_node->partial_len);
            
            // set next node child_pos and key prefix if not end node
            set_last_child_pos_in_key_path(key_path, idx_in_parent);
            if(END_POSITION_INDEX != idx_in_parent)
            {
                c = art_get_key_by_idx(cur_node, idx_in_parent);
                push_key_into_key_path(key_path, &c, 1);
            }

            // push next child to path, caller needs to pop it from path, then get its left most path.
            ret = memtable_key_path_get_left_most_path(type, t, snapid, key_path, next_node, idx_in_parent, snap_node, snap_pos_in_parent);
            //assert(kOk == ret);

            //需增加kNotfound错误码处理，当读写并发时可能会出现节点中子节点数目为0的情况，返回kNotFound问题，
            //可返回ART_COMPARE_RESULT_ERR，一直透传
            if(kNotFound == ret)
            {
            	ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path get left most path error, next_node childnum[%d] node_type[%d] perfix_len[%d] ret[%d].", 
            	             next_node->num_children, next_node->node_type, next_node->partial_len, ret);
            	return ART_COMPARE_RESULT_ERR;
            }
            
            if(kArtSnapIdNotFound == ret)
            {
                return ART_COMPARE_RESULT_LESS;
            }
            break;
        }
        
        // handle ART_COMPARE_RESULT_LESS == result. caller needs switch path at last node in path, use child_pos.
        UNREFERENCE_PARAM(ret);
        break;
    }

    if(ART_COMPARE_RESULT_EQUAL != result)
    {
        return result;
    }

    depth = get_prefix_len_in_key_path(key_path);
    if(key_len > depth)
    {
        return ART_COMPARE_RESULT_LESS;
    }

    *snap_node = (art_snap_node*)cur_node;
    *snap_pos_in_parent = pos_in_parent;

    return ART_COMPARE_RESULT_EQUAL;
}


/**
 * get a path that key bigger or equal than input.key
 * @arg key_path pointer to key path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t memtable_key_path_get_bigger_or_equal(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t type,
    IN art_tree_t* t,
    IN uint32_t snapid,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    int32_t result                          = ART_COMPARE_RESULT_EQUAL;

    result = memtable_key_path_do_get_bigger_or_equal(key_path, start_node, pos_in_parent, key, key_len, type, t, snapid, snap_node, snap_pos_in_parent);
    if(ART_COMPARE_RESULT_LESS != result)
    {
        return result;
    }

    // in this case, last node in path is smaller than required., then switch path.
    result = memtable_key_path_switch_next_path(type, t, snapid, key_path, snap_node, snap_pos_in_parent);
            
    /* if result = kArtSnapIdNotFound, so found_snapid is more than maxSnapid */
    while(kArtSnapIdNotFound == result)
    {
        result = memtable_key_path_switch_next_path(type, t, snapid, key_path, snap_node, snap_pos_in_parent);
    }

	//增加kNotFound错误码处理
    if(kInternalError == result)
    {
    	ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path switch next path error, result[%d].", result);
    	return ART_COMPARE_RESULT_ERR;
    }

    
    // if kArtIteratorEnd == result, path will be empty.
    return kArtIteratorContinue == result ? ART_COMPARE_RESULT_GREATER : ART_COMPARE_RESULT_LESS;
}


/**
 * get a path that key equal than input.key
 * @arg key_path pointer to key path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @out snap root/leaf node
 * @out snap root/leaf node's position in parent 
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and user needs to call switch path when return ART_COMPARE_RESULT_LESS.
 */
int32_t memtable_key_path_get_equal(
    IO art_key_path_t *key_path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    OUT art_snap_node **snap_node,
    OUT uint32_t *snap_pos_in_parent)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    int32_t result                          = kOk;

    uint32_t idx_in_parent                  = 0;
    
    uint8_t c                               = 0;
    uint32_t depth                          = 0;

    cur_node = start_node;

    MEMTABLE_KEY_PATH_CHECK_NODE(cur_node, kInvalidArgument);

    while(is_art_inner_node(cur_node))
    {
        depth = get_prefix_len_in_key_path(key_path);
        // get next equal
        result = memtable_search_node_equal(cur_node, key, key_len, depth, &next_node, &idx_in_parent);
        if(kOk != result)
        {
            break;
        }
        
        // add current node and partial to key path
        push_node_into_key_path(key_path, (void *)cur_node, pos_in_parent);
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
        cur_node = next_node;
    }

    if(kOk != result)
    {
        return result;
    }

    depth = get_prefix_len_in_key_path(key_path);
    if(depth != key_len)
    {
        return kNotFound;
    }

    *snap_node = (art_snap_node*)cur_node;
    *snap_pos_in_parent = pos_in_parent;

    return kOk;
}

void memtable_clear_key_path(art_key_path_t *key_path)
{
    while(!check_key_path_empty(key_path))
    {
        (void)pop_node_from_key_path(key_path);
    }
    clear_key_path_prefix(key_path);
}


#ifdef __cplusplus
}
#endif


#ifndef _SCM_PCLINT_
#include <assert.h>
#include "art_log.h"
#include "art_sstable.h"
#include "art_key_path_impl.h"
#include "art_snap_path_impl.h"
#include "art_key_path_sstable.h"
#include "art_snap_path_sstable.h"
#include "art_pub.h"
#include "art_sfmea.h"
#include "art_ftds.h"
#endif

#ifndef ARTREE_DEBUG
#ifndef _SCM_PCLINT_
#include "../../../../debug/common_ftds.h"
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
 * sstable get left/right most path entrance
 * @arg path, pointer to path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @out snap_get_lr_child_f function pointer for get left/right in snap child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_path_get_lr_most_path(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC iterator_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC iterator_fusion_get_lr_child_f,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC iterator_snap_get_lr_child_f)
{
    //start_origin_node may be snap node,art node, leaf node, fusion node
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;
    int32_t result                          = kNotFound;
    art_tree_t* tree                        = path->t;
    void* cur_node           			    = NULL;
    void *cur_origin_node                   = NULL;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    GET_BUFFER_FUNC get_buffer_f 			= path->t->ops.get_buffer;
    result = sstable_key_path_get_lr_most_path(SeekType, tree, 0, &path->key_path, start_origin_node,
        pos_in_parent, args, iterator_get_lr_child_f, iterator_fusion_get_lr_child_f, &snap_origin_node, &snap_pos_in_parent);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path get lr failed, result %d.", result);
        return result;
    }

    // fusion node, no snap tree
    if(NULL == snap_origin_node)
    {
        return kOk;
    }
    
    result = sstable_snap_path_get_lr_most_path(&path->snap_path, snap_origin_node, snap_pos_in_parent,
        args, iterator_snap_get_lr_child_f);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path get lr failed, result %d.", result);
        return result;
    }

    cur_origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
    if(TRUE == path->snap_path.is_need_release)
    {
        cur_node = get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
	//verify leaf key
	result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
	if (kOk != result)
	{
		return result;
	}
    
    return kOk;
}


/**
 * sstable get left most path entrance
 * @arg path, pointer to path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_path_get_left_most_path(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args)
{
    return sstable_path_get_lr_most_path(path, start_origin_node, pos_in_parent, args, iterator_sstable_get_node_left_child, iterator_sstable_get_fusion_node_left_child, iterator_sstable_snap_get_node_left_child);
}


/**
 * sstable get right most path entrance
 * @arg path, pointer to path
 * @arg start_origin_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg args read plog para
 * @return kOk means ok, otherwise something wrong.
 */
int32_t sstable_path_get_right_most_path(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN void *args)
{
    return sstable_path_get_lr_most_path(path, start_origin_node, pos_in_parent, args, iterator_sstable_get_node_right_child, iterator_sstable_get_fusion_node_right_child, iterator_sstable_snap_get_node_right_child);
}


/**
 * sstable switch left/right path entrance
 * @arg path, pointer to path
 * @arg args read plog para
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @arg fusion_get_lr_child_f function pointer for get left/right in fusion child
 * @arg fusion_get_next_child_f function pointer for get next/prev in fusion child
 * @arg snap_get_lr_child_f function pointer for get left/right in snap child
 * @arg snap_get_next_child_f function pointer for get next/prev in snap child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_path_switch_path(
    IO art_path_t *path,
    IN void *args,
    IN ITERATOR_SSTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_LR_CHILD_FUNC fusion_get_lr_child_f,
    IN ITERATOR_SSTABLE_GET_FUSION_NODE_NEXT_FUNC fusion_get_next_child_f,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN ITERATOR_SSTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f)
{
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;
    int32_t result                          = kOk;
    int32_t inner_result                    = kArtIteratorEnd;
    art_tree_t tree                         = {0};  
    void*    cur_node                       = NULL;
    void*    cur_origin_node                = NULL;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    uint32_t pos_in_parent                  = 0;
    GET_BUFFER_FUNC get_buffer_f 			= path->t->ops.get_buffer;
	
    // switch path in snap path
    result = sstable_snap_path_switch_path(&path->snap_path, args,  snap_get_lr_child_f, snap_get_next_child_f, &inner_result);
    if (result != kOk)
    {
        return result;
    }

	if (path->snap_path.path_index > 0)
	{
        cur_origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
        if (TRUE == path->snap_path.is_need_release)
        {
            cur_node = get_buffer_f(cur_origin_node);
        }
        else
        {
            cur_node = cur_origin_node;
        }
		//verify leaf key
		result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
		if (kOk != result)
		{
			return result;
		}
	}
    
    if(kArtIteratorContinue == inner_result)
    {
        return kArtIteratorContinue;
    }

    assert(kArtIteratorEnd == inner_result);
    // switch path in key path
    result = sstable_key_path_switch_path(SeekType, &tree, 0, &path->key_path, args, art_get_lr_child_f, art_get_next_child_f, fusion_get_lr_child_f, fusion_get_next_child_f, &snap_origin_node, &snap_pos_in_parent, &inner_result);
    if (result != kOk)
    {
        return result;
    }
    if(kArtIteratorContinue != inner_result)
    {
        return inner_result;
    }

    // last node in path is fusion node
    if(NULL == snap_origin_node)
    {
        return kArtIteratorContinue;
    }

    // get left most path in snap tree
    result = sstable_snap_path_get_lr_most_path(&path->snap_path, snap_origin_node, snap_pos_in_parent, args, snap_get_lr_child_f);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path switch lr get snap lr most path failed, result %d.", result);
        return result;
    }

    cur_origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
    if (TRUE == path->snap_path.is_need_release)
    {
        cur_node = get_buffer_f(cur_origin_node);
    }
    else
    {
        cur_node = cur_origin_node;
    }
    //verify leaf key
	result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
	if (kOk != result)
	{
		return result;
	}

    return kArtIteratorContinue;
}

/**
 * sstable switch prev path entrance
 * @arg path, pointer to path
 * @arg args read plog para
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_path_switch_prev_path(
    IO art_path_t *path,
    IN void *args)
{
    return sstable_path_switch_path(
               path,
               args,
               iterator_sstable_get_node_right_child,
               iterator_sstable_get_node_prev_child,
               iterator_sstable_get_fusion_node_right_child,
               iterator_sstable_get_fusion_node_prev_child,
               iterator_sstable_snap_get_node_right_child,
               iterator_sstable_snap_get_node_prev_child);
}


/**
 * sstable switch left/right path entrance
 * @arg path, pointer to path
 * @arg args read plog para
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t sstable_path_switch_next_path(
    IO art_path_t *path,
    IN void *args)
{
    return sstable_path_switch_path(
               path,
               args,
               iterator_sstable_get_node_left_child,
               iterator_sstable_get_node_next_child,
               iterator_sstable_get_fusion_node_left_child,
               iterator_sstable_get_fusion_node_next_child,
               iterator_sstable_snap_get_node_left_child,
               iterator_sstable_snap_get_node_next_child);
}


/**
 * get a path that key bigger or equal than input.key
 * @arg path pointer to path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t sstable_path_get_bigger_or_equal(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args,
    OUT int32_t *inner_result)
{
    int32_t result                          = kOk;
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;
    *inner_result                           = ART_COMPARE_RESULT_EQUAL;
    bool tp_open                            = true;
    RELEASE_BUFFER_FUNC release_buffer      = path->t->ops.release_buffer;

    void* cur_node           			    = NULL;
	GET_BUFFER_FUNC get_buffer_f 			= path->t->ops.get_buffer;
    
    LVOS_TP_START(ART_TP_ITERATOR_SEEK_READPLOG_FILE_NOT_FOUND, &result)
    tp_open = false;    
    result = sstable_key_path_get_bigger_or_equal(&path->key_path, start_origin_node, pos_in_parent,
        key, key_len, snapshotid, type, t, args, &snap_origin_node, &snap_pos_in_parent, inner_result);
    LVOS_TP_END
    if(true == tp_open)
    {
        //release_buffer(start_origin_node);
        release_read_buff(path->key_path.buff_node ,start_origin_node, path->key_path.is_need_release, release_buffer);
    }
    if (result != kOk)
    {
        return result;
    }
    if(ART_COMPARE_RESULT_LESS == *inner_result)
    {
        ART_ASSERT(check_key_path_empty(&path->key_path));
        //return ART_COMPARE_RESULT_LESS;
        *inner_result = ART_COMPARE_RESULT_LESS;
        return result;
    }

    if(NULL == snap_origin_node)
    {
        return result;
    }


    if(ART_COMPARE_RESULT_GREATER == *inner_result)
    {
        result = sstable_snap_path_get_left_most_path(&path->snap_path, snap_origin_node, snap_pos_in_parent, args);
        //ART_ASSERT(kOk == snap_result);
        //return ART_COMPARE_RESULT_GREATER;
        *inner_result = ART_COMPARE_RESULT_GREATER;
        return result;
    }


    result = sstable_snap_path_get_bigger_or_equal(&path->snap_path, snap_origin_node, snap_pos_in_parent, snapshotid, args, inner_result);
    if (result != kOk)
    {
        return result;
    }

	if(path->snap_path.path_index > 0)
	{
		if (TRUE == path->snap_path.is_need_release)
		{
			cur_node = get_buffer_f(path->snap_path.path[path->snap_path.path_index - 1].node);
		}
		else
		{
			cur_node = path->snap_path.path[path->snap_path.path_index - 1].node;
		}

	    //verify leaf key
		result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
		if (kOk != result)
		{
			return result;
		}
	}
    
    if(ART_COMPARE_RESULT_LESS != *inner_result)
    {
        //return snap_result;
        return result;
    }

    ART_ASSERT(check_snap_path_empty(&path->snap_path));
    result = sstable_key_path_switch_next_path(SeekType, t, 0, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, inner_result);
    if (result != kOk)
    {
        return result;
    }
    if(kArtIteratorEnd == *inner_result)
    {
        ART_ASSERT(check_key_path_empty(&path->key_path));
        //return ART_COMPARE_RESULT_LESS;
        *inner_result = ART_COMPARE_RESULT_LESS;
        return result;
    }

    if(NULL == snap_origin_node)
    {
        //return ART_COMPARE_RESULT_GREATER;
        *inner_result = ART_COMPARE_RESULT_GREATER;
        return result;
    }

    result = sstable_snap_path_get_left_most_path(&path->snap_path, snap_origin_node, snap_pos_in_parent, args);
    //assert(kOk == snap_result);
    if (result != kOk)
    {
        return result;
    }
    //return ART_COMPARE_RESULT_GREATER;
    *inner_result = ART_COMPARE_RESULT_GREATER;
    return result;
}


/**
 * get a path that key equal than input.key, and with smallest snap id
 * @arg path pointer to path
 * @arg start_origin_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @arg args read plog para 
 * @return return kok means found, or other code.
 */
int32_t sstable_path_get_equal_key_smallest_snapid(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args)
{
    int32_t result                          = kOk;
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;

    // get path with identical key
    result = sstable_key_path_get_equal(&path->key_path, start_origin_node, pos_in_parent, key, key_len, args, &snap_origin_node, &snap_pos_in_parent);
    if(kOk != result)
    {
        return result;
    }

    // last node in path is fusion node
    if(NULL == snap_origin_node)
    {
        return kOk;
    }

    // get smallest snap id path
    result = sstable_snap_path_get_left_most_path(&path->snap_path, snap_origin_node, snap_pos_in_parent, args);
    //assert(kOk == result);
    if(result != kOk)
    {
        return result;
    }

    return kOk;
}


/**
 * get value from path
 * @arg art_path path, last node in path is fusion node
 * @arg value. output value
 * @arg value_len. output value length
 * @return kok means found, or other code.
 */
int32_t sstable_path_get_value(
    IN art_path_t *path,
    OUT uint8_t **value, 
    OUT uint32_t *value_len)
{
    void *origin_node                   = NULL;
    void *node                          = NULL;
    art_data *data                      = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    ART_ASSERT(!check_key_path_empty(&path->key_path) || !check_snap_path_empty(&path->snap_path));
    if(check_key_path_empty(&path->key_path) && check_snap_path_empty(&path->snap_path))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "iterator path is empty, path(%p)", path);
        return kInvalidArgument;
    }
    if(!check_snap_path_empty(&path->snap_path))
    {
        iterator_sstable_snap_path_get_buff_node_info(&path->snap_path, &path->snap_path.buff_node, &path->snap_path.is_need_release);
        origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
        if (TRUE == path->snap_path.is_need_release)
        {
            node = path->snap_path.ops.get_buffer_f(origin_node);
        }
        else
        {
            node = origin_node;
        }
        assert(is_art_leaf_node(node));
        *value = path->snap_path.art_ops->leaf_ops.get_value(node);
        *value_len = path->snap_path.art_ops->leaf_ops.get_value_len(node);

        return kOk;
    }
    iterator_sstable_key_path_get_buff_node_info(&path->key_path, &path->key_path.buff_node, &path->key_path.is_need_release);
    origin_node = get_last_node_in_key_path(&path->key_path, &pos_in_parent, &child_pos, &parent_idx);
    if (TRUE == path->key_path.is_need_release)
    {
        node = path->key_path.ops.get_buffer_f(origin_node);
    }
    else
    {
        node = origin_node;
    }

    //in normal situation, iterator path store a leaf or a fusion node 
    if(!is_sstable_fusion_node(node))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path check fail, snap path empty but last node of key path is not fusion node, node(%p), node_type(%d)", node, *(uint8_t*)node);
        return kInvalidArgument;
    }
    
    data = (art_data *)sstable_get_fusion_node_value_by_index(node, child_pos);
    if(NULL == data)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get data from fusion node fail, data is null, node(%p), child_pos(%u)", node, child_pos);
        return kIOError;
    }
    *value = data->value;
    *value_len = sstable_get_fusion_node_value_len_by_index(node, child_pos);
    return kOk;
}


/**
 * get snapid from path
 * @arg art_path path, last node in path is fusion node
 * @arg snap_id. output snapshot id
 * @return kok means found, or other code.
 */
int32_t sstable_path_get_snapid(
    IN art_path_t *path,
    OUT uint32_t *snap_id)
{
    void *origin_node                   = NULL;
    void *node                          = NULL;
    art_data *data                      = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    ART_ASSERT(!check_key_path_empty(&path->key_path) || !check_snap_path_empty(&path->snap_path));
    if(check_key_path_empty(&path->key_path) && check_snap_path_empty(&path->snap_path))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "iterator path is empty, path(%p)", path);
        return kInvalidArgument;
    }
    if(!check_snap_path_empty(&path->snap_path))
    {
        iterator_sstable_snap_path_get_buff_node_info(&path->snap_path, &path->snap_path.buff_node, &path->snap_path.is_need_release);
        origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
        if (TRUE == path->snap_path.is_need_release)
        {
            node = path->snap_path.ops.get_buffer_f(origin_node);
        }
        else
        {
            node = origin_node;
        }
        
        ART_ASSERT(is_art_leaf_node(node));
        *snap_id = path->snap_path.art_ops->leaf_ops.get_snap_id(node);

        return kOk;
    }
    iterator_sstable_key_path_get_buff_node_info(&path->key_path, &path->key_path.buff_node, &path->key_path.is_need_release);
    origin_node = get_last_node_in_key_path(&path->key_path, &pos_in_parent, &child_pos, &parent_idx);
    if (TRUE == path->key_path.is_need_release)
    {
        node = path->key_path.ops.get_buffer_f(origin_node);
    }
    else
    {
        node = origin_node;
    }
    if(!is_sstable_fusion_node(node))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path check fail, snap path empty but last node of key path is not fusion node, node(%p), node_type(%d)", node, *(uint8_t*)node);
        return kIOError;
    }
    
    data = (art_data *)sstable_get_fusion_node_value_by_index(node, child_pos);
    if(NULL == data)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get data from fusion node fail, data is null, node(%p), child_pos(%u)", node, child_pos);
        return kIOError;
    }
    *snap_id = data->snapid;
    return kOk;
}


/**
 * check whether the value of op code is to delete
 * @arg art_path path, last node in path is fusion node
 * @arg isdeleted. output check op code is delete
 * @return kok means found, or other code.
 */
int32_t sstable_path_check_is_delete(
    IN art_path_t *path, 
    OUT bool *isdeleted)
{
    void *origin_node                   = NULL;
    void *node                          = NULL;
    art_data *data                      = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    
    ART_ASSERT(!check_key_path_empty(&path->key_path) || !check_snap_path_empty(&path->snap_path));
    if(check_key_path_empty(&path->key_path) && check_snap_path_empty(&path->snap_path))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "iterator path is empty, path(%p)", path);
        return kInvalidArgument;
    }
    if(!check_snap_path_empty(&path->snap_path))
    {
        iterator_sstable_snap_path_get_buff_node_info(&path->snap_path, &path->snap_path.buff_node, &path->snap_path.is_need_release);
        origin_node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
        if (TRUE == path->snap_path.is_need_release)
        {
            node = path->snap_path.ops.get_buffer_f(origin_node);
        }
        else
        {
            node = origin_node;
        }
        
        ART_ASSERT(is_art_leaf_node(node));
        *isdeleted = (kTypeDeletion == path->snap_path.art_ops->leaf_ops.get_op_type(node));

        return kOk;
    }
    
    iterator_sstable_key_path_get_buff_node_info(&path->key_path, &path->key_path.buff_node, &path->key_path.is_need_release);
    origin_node = get_last_node_in_key_path(&path->key_path, &pos_in_parent, &child_pos, &parent_idx);
    if (TRUE == path->key_path.is_need_release)
    {
        node = path->key_path.ops.get_buffer_f(origin_node);
    }
    else
    {
        node = origin_node;
    }
    //in normal situation, iterator path store a leaf or a fusion node 
    if(!is_sstable_fusion_node(node))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path check fail, snap path empty but last node of key path is not fusion node, node(%p), node_type(%d)", node, *(uint8_t*)node);
        return kIOError;
    }
    data = (art_data *)sstable_get_fusion_node_value_by_index(node, child_pos);
    if(NULL == data)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get data from fusion node fail, data is null, node(%p), child_pos(%u)", node, child_pos);
        return kIOError;
    }
    *isdeleted = (kTypeDeletion == data->op_type);
    return kOk;
}


int32_t sstable_path_switch_next_path_with_condition(
    IN art_tree_t* t,
    IO art_path_t *path,
    IN void *args,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN bool return_snap_once)
{
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;
    uint32_t snapid                         = 0;
    int32_t result                          = kOk;
    int32_t inner_result                    = ART_COMPARE_RESULT_EQUAL;
	RELEASE_BUFFER_FUNC release_buffer_f	= NULL;  
    void* cur_node           			    = NULL;
	GET_BUFFER_FUNC get_buffer_f 			= path->t->ops.get_buffer;
    uint64_t time                           = 0;

	release_buffer_f = path->key_path.ops.release_buffer_f;
    start_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, &time);
    
    if(false == return_snap_once)
    {
        // switch path in snap path
        result = sstable_snap_path_switch_next_path(&path->snap_path, args, &inner_result);
        if (result != kOk)
        {
            sstable_clear_key_path(&path->key_path);
            end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, result, time);
            return result;
        }
        if(kArtIteratorContinue == inner_result)
        {
            iterator_sstable_snap_path_get_buff_node_info(&path->snap_path, &path->snap_path.buff_node, &path->snap_path.is_need_release);
			if (TRUE == path->snap_path.is_need_release)
			{
				cur_node = get_buffer_f(path->snap_path.path[path->snap_path.path_index - 1].node);
			}
			else
			{
				cur_node = path->snap_path.path[path->snap_path.path_index - 1].node;
			}

            //verify leaf key
			result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
			if (kOk != result)
			{
				return result;
			}
            
            (void)sstable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
                return kOk;
            }
        }
    }
    sstable_clear_snap_path(&path->snap_path);
    result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, &inner_result);

    while(kArtSnapIdNotFound == result)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            sstable_clear_key_path(&path->key_path);
            end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
            return CHECK_KEY_OUT_OF_RANGE;
        }

        result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, &inner_result);
    }
    
    if (result != kOk)
    {
        sstable_clear_key_path(&path->key_path);
        if (NULL != snap_origin_node)
		{
		    release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, release_buffer_f);
		}
        end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, result, time);
        return result;
    }
    
    while(kArtIteratorContinue == inner_result)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            sstable_clear_key_path(&path->key_path);
			if (NULL != snap_origin_node)
			{
                release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, release_buffer_f);
			}
            end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
            return CHECK_KEY_OUT_OF_RANGE;
        }
        if(NULL != snap_origin_node)
        {
            result = sstable_snap_path_get_bigger_or_equal(&path->snap_path, snap_origin_node, snap_pos_in_parent, snapid_min, args, &inner_result);
            if (result != kOk)
            {
                sstable_clear_snap_path(&path->snap_path);
                sstable_clear_key_path(&path->key_path);
                end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, result, time);
                return result;
            }

            if(path->snap_path.path_index > 0)
            {
                iterator_sstable_snap_path_get_buff_node_info(&path->snap_path, &path->snap_path.buff_node, &path->snap_path.is_need_release);
				if (TRUE == path->snap_path.is_need_release)
				{
					cur_node = get_buffer_f(path->snap_path.path[path->snap_path.path_index - 1].node);
				}
				else
				{
					cur_node = path->snap_path.path[path->snap_path.path_index - 1].node;
				}

                //verify leaf key
                result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
                if (kOk != result)
                {
                    return result;
                }
            }
            
            if(ART_COMPARE_RESULT_LESS != inner_result)
            {
                (void)sstable_path_get_snapid(path, &snapid);
                if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
                {
                    end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
                    return kOk;
                }                
            }
        }else
        {
            (void)sstable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
                return kOk;
            }
        }        
        sstable_clear_snap_path(&path->snap_path);
        snap_origin_node = NULL;
        result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, &inner_result);            

        while(kArtSnapIdNotFound == result)
        {
            if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
            {
                sstable_clear_key_path(&path->key_path);
                end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, kOk, time);
                return CHECK_KEY_OUT_OF_RANGE;
            }

            result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, &inner_result);
        }
    
        if (result != kOk)
        {
            if (NULL != snap_origin_node)
            {
                release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, release_buffer_f);
            }
            sstable_clear_key_path(&path->key_path);
            end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, result, time);
            return result;
        }
    }
    if (NULL != snap_origin_node)
	{
		release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, release_buffer_f);
	}
    end_comm_ftds(ART_ITOR_ST_NEXT_CONDITION, result, time);
    return CHECK_KEY_OUT_OF_RANGE;
}

int32_t sstable_path_get_bigger_or_equal_with_conditon(
    IO art_path_t *path,
    IN void *start_origin_node,
    IN uint32_t pos_in_parent,
    IN uint8_t *starting_key,
    IN uint32_t starting_key_len,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN art_tree_t* t,
    IN void *args,
    OUT int32_t *inner_result)
{
    int32_t result                          = kOk;
    void *snap_origin_node                  = NULL;
    uint32_t snap_pos_in_parent             = 0;
    uint32_t snapid                         = 0;
    bool tp_open                            = true;
    void* cur_node           			    = NULL;
    GET_BUFFER_FUNC get_buffer_f 			= path->t->ops.get_buffer;

    LVOS_TP_START(ART_TP_ITERATOR_GET_LR_CHILD_READPLOG_FILE_NOT_FOUND, &result)
    LVOS_TP_START(ART_TP_ITERATOR_GET_FIRST_CONDITION_READPLOG_FILE_NOT_FOUND, &result)
    tp_open = false;
    result = sstable_key_path_get_bigger_or_equal(&path->key_path, start_origin_node, pos_in_parent, starting_key, starting_key_len, snapid_min, GetType, t, args, &snap_origin_node, &snap_pos_in_parent, inner_result);
    LVOS_TP_END
    LVOS_TP_END
    if(tp_open == true)
    {
        release_read_buff(path->key_path.buff_node, start_origin_node, path->key_path.is_need_release, path->key_path.ops.release_buffer_f);
        //path->key_path.ops.release_buffer_f(start_origin_node);
    }
    
    if (result != kOk)
    {
        sstable_clear_key_path(&path->key_path);
        if(NULL != snap_origin_node)
        {
            //path->key_path.ops.release_buffer_f(snap_origin_node);
            release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, path->snap_path.ops.release_buffer_f);
        }
        return result;
    }
    if(ART_COMPARE_RESULT_LESS == *inner_result)
    {
        assert(check_key_path_empty(&path->key_path));
        *inner_result = CHECK_KEY_OUT_OF_RANGE;
        return kOk;
    }
    *inner_result = kArtIteratorContinue;

    while(kArtIteratorContinue == *inner_result)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            sstable_clear_key_path(&path->key_path);
            if(NULL != snap_origin_node)
            {
                //path->key_path.ops.release_buffer_f(snap_origin_node);
                release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, path->snap_path.ops.release_buffer_f);
            }
            //return CHECK_KEY_OUT_OF_RANGE;
            *inner_result = CHECK_KEY_OUT_OF_RANGE;
            return kOk;
        }
        if(NULL != snap_origin_node)
        {
            result = sstable_snap_path_get_bigger_or_equal(&path->snap_path, snap_origin_node, snap_pos_in_parent, snapid_min, args, inner_result);
            if (result != kOk)
            {
                sstable_clear_snap_path(&path->snap_path);
                sstable_clear_key_path(&path->key_path);
                return result;
            }

            if(path->snap_path.path_index > 0)
            {
				if (TRUE == path->snap_path.is_need_release)
				{
					cur_node = (art_node *)get_buffer_f(path->snap_path.path[path->snap_path.path_index - 1].node);
				}
				else
				{
					cur_node = path->snap_path.path[path->snap_path.path_index - 1].node;
				}

                //verify leaf key
                result = check_key_valid(path->t, cur_node, path->key_path.prefix, path->key_path.prefix_len);
                if (kOk != result)
                {
                    return result;
                }
            }
    
            if(ART_COMPARE_RESULT_LESS != *inner_result)
            {
                (void)sstable_path_get_snapid(path, &snapid);
                if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
                {
                    return kOk;
                }                
            }
        }else
        {
            (void)sstable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                return kOk;
            }
        }
        sstable_clear_snap_path(&path->snap_path);
        snap_origin_node = NULL;
        result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, inner_result);                

        /* if result = kArtSnapIdNotFound, so found_snapid is more than maxSnapid */
        while(kArtSnapIdNotFound == result)
        {
            result = sstable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, args, &snap_origin_node, &snap_pos_in_parent, inner_result);                
        }
        
        if (result != kOk)
        {
            sstable_clear_key_path(&path->key_path);
            if(NULL != snap_origin_node)
            {
                //path->key_path.ops.release_buffer_f(snap_origin_node);
                release_read_buff(path->snap_path.buff_node, snap_origin_node, path->snap_path.is_need_release, path->snap_path.ops.release_buffer_f);
            }
            return result;
        }
    }
    
    *inner_result = CHECK_KEY_OUT_OF_RANGE;
    return kOk;
}

#ifdef __cplusplus
}
#endif


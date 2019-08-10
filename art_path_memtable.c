#ifndef _SCM_PCLINT_
#include <assert.h>
#include "art_log.h"
#include "art_memtable.h"
#include "art_snapshot.h"
#include "art_snapshot_memtable.h"
#include "art_key_path_impl.h"
#include "art_snap_path_impl.h"
#include "art_key_path_memtable.h"
#include "art_snap_path_memtable.h"
#include "art_pub.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif


/**
 * memtable get left/right most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @out snap_get_lr_child_f function pointer for get left/right in snap child
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_lr_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f)
{
    //start_origin_node may be snap node,art node, leaf node, fusion node
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;
    int32_t result                          = kNotFound;
    art_tree_t* tree                        = path->t;
    
    result = memtable_key_path_get_lr_most_path(SeekType, tree, 0, &path->key_path, start_node, pos_in_parent, art_get_lr_child_f, &snap_node, &snap_pos_in_parent);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "memtable path get lr failed, result %d.", result);
        if(kNotFound == result)
    	{
    		return kInternalError;
    	}
        return result;
    }
    
    result = memtable_snap_path_get_lr_most_path(&path->snap_path, snap_node, snap_pos_in_parent, snap_get_lr_child_f);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable path get lr failed, result %d.", result);
        if(kNotFound == result)
    	{
    		return kInternalError;
    	}
        return result;
    }

    return kOk;
}


/**
 * memtable get left most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_left_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent)
{
    return memtable_path_get_lr_most_path(path, start_node, pos_in_parent, memtable_get_node_left_child, memtable_snap_get_node_left_child);
}


/**
 * memtable get right most path entrance
 * @arg path, pointer to path
 * @arg start_node, start node, may be fusion node
 * @arg pos_in_parent, start node position in parent
 * @return kOk means ok, otherwise something wrong.
 */
int32_t memtable_path_get_right_most_path(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent)
{    
    return memtable_path_get_lr_most_path(path, start_node, pos_in_parent, memtable_get_node_right_child, memtable_snap_get_node_right_child);
}


/**
 * memtable switch left/right path entrance
 * @arg path, pointer to path
 * @arg art_get_lr_child_f function pointer for get left/right art child
 * @arg art_get_next_child_f function pointer for get next/prev art child
 * @arg snap_get_lr_child_f function pointer for get left/right in snap child
 * @arg snap_get_next_child_f function pointer for get next/prev in snap child
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_path(
    IO art_path_t *path,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN MEMTABLE_GET_NODE_NEXT_FUNC art_get_next_child_f,
    IN MEMTABLE_SNAP_GET_NODE_LR_CHILD_FUNC snap_get_lr_child_f,
    IN MEMTABLE_SNAP_GET_NODE_NEXT_FUNC snap_get_next_child_f)
{
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;
    int32_t result                          = kArtIteratorEnd;
    art_tree_t tree                         = {0}; 

    // switch path in snap path
    result = memtable_snap_path_switch_path(&path->snap_path, snap_get_lr_child_f, snap_get_next_child_f);
	if(kInternalError == result)
	{
		return result;
	}
	
    if(kArtIteratorContinue == result)
    {
        return kArtIteratorContinue;
    }

    ART_ASSERT(kArtIteratorEnd == result);
    ART_ASSERT(check_snap_path_empty(&path->snap_path));
    // switch path in key path
    result = memtable_key_path_switch_path(SeekType, &tree, 0, &path->key_path, art_get_lr_child_f, art_get_next_child_f, &snap_node, &snap_pos_in_parent);
    if(kArtIteratorContinue != result)
    {
        return result;
    }

    // get left most path in snap tree
    result = memtable_snap_path_get_lr_most_path(&path->snap_path, snap_node, snap_pos_in_parent, snap_get_lr_child_f);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "memtable path switch lr get snap lr most path failed, result %d.", result);
        if(kNotFound == result)
        {
        	return kInternalError;
        }
        return result;
    }

    return kArtIteratorContinue;
}


/**
 * memtable switch prev path entrance
 * @arg path, pointer to path
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_prev_path(
    IO art_path_t *path)
{
    return memtable_path_switch_path(path, memtable_get_node_right_child, memtable_get_node_prev_child, memtable_snap_get_node_right_child, memtable_snap_get_node_prev_child);
}


/**
 * memtable switch next path entrance
 * @arg path, pointer to path
 * @return kArtIteratorContinue means switch ok, kArtIteratorEnd means cannot switch path.
 */
int32_t memtable_path_switch_next_path(
    IO art_path_t *path)
{
    return memtable_path_switch_path(path, memtable_get_node_left_child, memtable_get_node_next_child, memtable_snap_get_node_left_child, memtable_snap_get_node_next_child);
}


/**
 * get a path that key bigger or equal than input.key
 * @arg path pointer to path
 * @arg start_node start node
 * @arg pos_in_parent node position in inner node
 * @arg key
 * @arg key_len
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t memtable_path_get_bigger_or_equal(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t)
{
    int32_t key_result                      = ART_COMPARE_RESULT_EQUAL;
    int32_t snap_result                     = ART_COMPARE_RESULT_EQUAL;
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;

    key_result = memtable_key_path_get_bigger_or_equal(&path->key_path, start_node, pos_in_parent, key, key_len, type, t, snapshotid, &snap_node, &snap_pos_in_parent);
	if(ART_COMPARE_RESULT_ERR == key_result)
	{
		return key_result;
	}
	
    if(ART_COMPARE_RESULT_LESS == key_result)
    {
        ART_ASSERT(check_key_path_empty(&path->key_path));
        return ART_COMPARE_RESULT_LESS;
    }

    if(ART_COMPARE_RESULT_GREATER == key_result)
    {
        snap_result = memtable_snap_path_get_left_most_path(&path->snap_path, snap_node, snap_pos_in_parent);
        if(kNotFound == snap_result)
        {
        	return ART_COMPARE_RESULT_ERR;
        }
        return ART_COMPARE_RESULT_GREATER;
    }

    snap_result = memtable_snap_path_get_bigger_or_equal(&path->snap_path, snap_node, snap_pos_in_parent, snapshotid);
    if(ART_COMPARE_RESULT_LESS != snap_result)
    {
        return snap_result;
    }

    ART_ASSERT(check_snap_path_empty(&path->snap_path));
    key_result = memtable_key_path_switch_next_path(type, t, snapshotid, &path->key_path, &snap_node, &snap_pos_in_parent);

	//增加kNotFound错误码判断，向上层返回ART_COMPARE_RESULT_ERR，直接透传
    if(kInternalError == key_result)
    {
    	ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path switch next path error, ret[%d].", key_result);
    	return ART_COMPARE_RESULT_ERR;
    }
    
    if(kArtIteratorEnd == key_result)
    {
        ART_ASSERT(check_key_path_empty(&path->key_path));
        return ART_COMPARE_RESULT_LESS;
    }

    snap_result = memtable_snap_path_get_left_most_path(&path->snap_path, snap_node, snap_pos_in_parent);
    ART_ASSERT(kOk == snap_result);
    if(kNotFound == snap_result)
    {
    	return ART_COMPARE_RESULT_ERR;
    }
    return ART_COMPARE_RESULT_GREATER;
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
int32_t memtable_path_get_equal_key_smallest_snapid(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t* key,
    IN uint32_t key_len)
{
    int32_t result                          = kOk;
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;

    // get path with identical key
    result = memtable_key_path_get_equal(&path->key_path, start_node, pos_in_parent, key, key_len, &snap_node, &snap_pos_in_parent);
    if(kOk != result)
    {
        return result;
    }

    // get smallest snap id path
    result = memtable_snap_path_get_left_most_path(&path->snap_path, snap_node, snap_pos_in_parent);
    assert(kOk == result);
    if(kNotFound == result)
    {
    	return kInternalError;
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
int32_t memtable_path_get_value(
    IN art_path_t *path,
    OUT uint8_t **value, 
    OUT uint32_t *value_len)
{
    void *node                          = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    ART_ASSERT(!check_key_path_empty(&path->key_path) && !check_snap_path_empty(&path->snap_path));
    
    node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
    ART_ASSERT(is_art_leaf_node(node));

    *value = path->snap_path.art_ops->leaf_ops.get_value(node);
    *value_len = path->snap_path.art_ops->leaf_ops.get_value_len(node);

    return kOk;
}


/**
 * get snapid from path
 * @arg art_path path, last node in path is fusion node
 * @arg snap_id. output snapshot id
 * @return kok means found, or other code.
 */
int32_t memtable_path_get_snapid(
    IN art_path_t *path,
    OUT uint32_t *snap_id)
{
    void *node                          = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    ART_ASSERT(!check_key_path_empty(&path->key_path) && !check_snap_path_empty(&path->snap_path));
    
    node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
    ART_ASSERT(is_art_leaf_node(node));

    *snap_id = path->snap_path.art_ops->leaf_ops.get_snap_id(node);

    return kOk;
}


/**
 * check whether the value of op code is to delete
 * @arg art_path path, last node in path is fusion node
 * @arg isdeleted. output check op code is delete
 * @return kok means found, or other code.
 */
int32_t memtable_path_check_is_delete(
    IN art_path_t *path, 
    OUT bool *isdeleted)
{
    void *node                          = NULL;
    
    uint32_t pos_in_parent              = 0;
    uint32_t child_pos                  = 0;
    uint32_t parent_idx                 = 0;

    ART_ASSERT(!check_key_path_empty(&path->key_path) && !check_snap_path_empty(&path->snap_path));
    
    node = get_last_node_in_snap_path(&path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
    ART_ASSERT(is_art_leaf_node(node));

    *isdeleted = (kTypeDeletion == path->snap_path.art_ops->leaf_ops.get_op_type(node));

    return kOk;
}

int32_t memtable_path_switch_next_path_with_condition(
    IN art_tree_t* t,
    IO art_path_t *path,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN bool return_snap_once)
{
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;
    uint32_t snapid                         = 0;
    int32_t result                          = kArtIteratorEnd;
    int32_t key_result                      = ART_COMPARE_RESULT_EQUAL;
    int32_t snap_result                     = ART_COMPARE_RESULT_EQUAL;

    if(false == return_snap_once)
    {
        result = memtable_snap_path_switch_next_path(&path->snap_path);
        if(kInternalError == result)
        {
            return result;
        }

        if(kArtIteratorContinue == result)
        {
            memtable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                return kOk;
            }
        } 
    }
    memtable_clear_snap_path(&path->snap_path);
    key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);
    
    while(key_result == kArtSnapIdNotFound)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            memtable_clear_key_path(&path->key_path);
            return CHECK_KEY_OUT_OF_RANGE;
        }
                
        key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);
    }

    //增加kIOError错误码处理
    if(kInternalError == key_result)
    {
    	ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path switch next path error, key_result[%d].", key_result);
    	return key_result;
    }
    
    while(kArtIteratorContinue == key_result)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            memtable_clear_key_path(&path->key_path);
            return CHECK_KEY_OUT_OF_RANGE;
        }
        snap_result = memtable_snap_path_get_bigger_or_equal(&path->snap_path, snap_node, snap_pos_in_parent, snapid_min);
        if(ART_COMPARE_RESULT_ERR == snap_result)
        {
        	return kInternalError;
        }
        
        if(ART_COMPARE_RESULT_LESS != snap_result)
        {
            (void)memtable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                return kOk;
            }
        }
        memtable_clear_snap_path(&path->snap_path);
        key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);
		
		while (key_result == kArtSnapIdNotFound)
		{
			if (CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
			{
				memtable_clear_key_path(&path->key_path);
				return CHECK_KEY_OUT_OF_RANGE;
			}

			key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);
		}

        //增加kIOError错误码处理
		if(kInternalError == key_result)
		{
			ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path switch next path error, key_result[%d].", key_result);
			return key_result;
		}
    }

    return CHECK_KEY_OUT_OF_RANGE;
}

int32_t memtable_path_get_bigger_or_equal_with_condition(
    IO art_path_t *path,
    IN art_node *start_node,
    IN uint32_t pos_in_parent,
    IN uint8_t *starting_key,
    IN uint32_t starting_key_len,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN art_tree_t* t)
{
    int32_t key_result                      = ART_COMPARE_RESULT_EQUAL;
    int32_t snap_result                     = ART_COMPARE_RESULT_EQUAL;
    art_snap_node *snap_node                = NULL;
    uint32_t snap_pos_in_parent             = 0;
    uint32_t snapid                         = 0;

    key_result = memtable_key_path_get_bigger_or_equal(&path->key_path, start_node, pos_in_parent, starting_key, starting_key_len, GetType, t, snapid_min, &snap_node, &snap_pos_in_parent);

	if(ART_COMPARE_RESULT_ERR == key_result)
	{
		return key_result;
	}
	
    if(ART_COMPARE_RESULT_LESS == key_result)
    {
        assert(check_key_path_empty(&path->key_path));
        return CHECK_KEY_OUT_OF_RANGE;
    }
    key_result = kArtIteratorContinue;
    while(kArtIteratorContinue == key_result)
    {
        if(CHECK_OK != check_condition_key_valid(prefix_key, prefix_key_len, path->key_path.prefix, path->key_path.prefix_len))
        {
            memtable_clear_key_path(&path->key_path);
            return CHECK_KEY_OUT_OF_RANGE;
        }
        snap_result = memtable_snap_path_get_bigger_or_equal(&path->snap_path, snap_node, snap_pos_in_parent, snapid_min);
        if(ART_COMPARE_RESULT_ERR == snap_result)
        {
            return snap_result;
        }

        if(ART_COMPARE_RESULT_LESS != snap_result)
        {
            (void)memtable_path_get_snapid(path, &snapid);
            if(CHECK_OK == check_condition_snapid_valid(snapid_min, snapid_max, snapid))
            {
                return kOk;
            }
        }
        memtable_clear_snap_path(&path->snap_path);
        key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);       
        
        /* if key_result = kArtSnapIdNotFound, so found_snapid is more than maxSnapid */
        while(kArtSnapIdNotFound == key_result)
        {
            key_result = memtable_key_path_switch_next_path(GetType, t, snapid_min, &path->key_path, &snap_node, &snap_pos_in_parent);       
        }

		//增加kNotFound错误码处理
        if(kInternalError == key_result)
        {
        	ART_LOGERROR(ART_LOG_ID_4B00, "memtable key path switch next path error, key_result[%d].", key_result);
        	return key_result;
        }
    }
    return CHECK_KEY_OUT_OF_RANGE;
}

#ifdef __cplusplus
}
#endif
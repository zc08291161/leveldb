#ifndef _SCM_PCLINT_
#ifndef WIN32
#include "logging.h"
#endif
#include "art_path_memtable.h"
#include "art_path_sstable.h"
#include "art_key_path_impl.h"
#include "art_snap_path_impl.h"
#include "art_log.h"
#include "art_iterator_opt.h"
#include "art_key_path_sstable.h"
#include "art_snap_path_sstable.h"
#include "art_sfmea.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
* description               : init path structure.
* key_path                  : pointer to path
* t                         : pointer to artree
* read_args                 : read plog para
*/
void init_art_path(
    IN art_path_t *path,
    IN art_tree_t *t)
{
    path->t = t;
    init_key_path(&path->key_path);
    init_snap_path(&path->snap_path);
}


/*
* description               : init art path operations.
* path                      : pointer to art path
* ops                       : operation set
*/
void init_art_path_ops(
    IO art_path_t *path,
    IN art_path_operations_t *ops)
{
    init_key_path_ops(&path->key_path, ops);
    init_snap_path_ops(&path->snap_path, ops);
}


/*
* description               : assign internal operation set for art path
* path                      : pointer to art path
* ops                       : internal operation set
*/
void assign_art_path_internal_ops(
    IO art_path_t *path,
    IN artree_operations_t *ops)
{
    assign_key_path_internal_ops(&path->key_path, ops);
    assign_snap_path_internal_ops(&path->snap_path, ops);
}


/*
* description               : clear key and snap path.
* art_path                  : pointer to path
*/
void clear_art_path(
	IN art_path_t *art_path,
	IN const char* func,
	IN int32_t line)
{
    sstable_clear_key_path(&art_path->key_path);
    sstable_clear_snap_path(&art_path->snap_path);

    UNREFERENCE_PARAM(func);
    UNREFERENCE_PARAM(line);
}


/*
* description               : get min path.
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_first(
    IN art_path_t *art_path,
    IN void *args)
{
    art_tree_t* artree                  = NULL;
    art_node *root_node                 = NULL;
    int32_t result                      = kOk;

    artree = art_path->t;
    /* empty tree */
    if (check_art_empty(artree))
	{
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree is empty apply_type=%d", artree->apply_type);
        return kOk;
	}
    // first need to clear path
    clear_art_path(art_path, __func__, __LINE__);
    
    if (!CHECK_ARTREE_CRC_VALID(artree))
    {
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree crc check invalid[%llu]", artree->art_crc);
        return kIOError;
    }
    
    LVOS_TP_START(ART_TP_ITERATOR_GET_FIRST_ROOT_READPLOG_FILE_NOT_FOUND, &result)
    LVOS_TP_START(ART_TP_ITERATOR_GET_FIRST_READPLOG_FILE_NOT_FOUND, &result)
    result = iterator_get_root_node(artree, art_path->key_path.iterator, args, (void**)&root_node);
    LVOS_TP_END
    LVOS_TP_END
    if (kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", result);
        return result;
    }
    if(is_memtable_art(artree))
    {
        result = memtable_path_get_left_most_path(art_path, root_node, 0);
    }
    else
    {
        result = sstable_path_get_left_most_path(art_path, (void *)root_node, 0, args);
    }

    return result;
}


/*
* description               : get max path.
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_last(
    IN art_path_t *art_path,
    IN void *args)
{
    art_tree_t* artree                  = NULL;
    art_node *root_node                 = NULL;
    int32_t result                      = kOk;

    artree = art_path->t;
    /* empty tree */
    if (check_art_empty(artree))
	{
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree is empty apply_type=%d", artree->apply_type);
        return kOk;
	}

    // first need to clear path
    clear_art_path(art_path, __func__, __LINE__);

    if (!CHECK_ARTREE_CRC_VALID(artree))
    {
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree crc check invalid[%llu]", artree->art_crc);
        return kIOError;
    }
    
    LVOS_TP_START(ART_TP_ITERATOR_GET_LAST_ROOT_READPLOG_FILE_NOT_FOUND, &result)
    LVOS_TP_START(ART_TP_ITERATOR_GET_LAST_READPLOG_FILE_NOT_FOUND, &result)
    result = iterator_get_root_node(artree, art_path->key_path.iterator, args, (void**)&root_node);
    LVOS_TP_END
    LVOS_TP_END
    if (kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", result);
        return result;
    }
    if(is_memtable_art(artree))
    {
        result = memtable_path_get_right_most_path(art_path, root_node, 0) ;
    }
    else
    {
        result = sstable_path_get_right_most_path(art_path, (void *)root_node, 0, args);
    }

    return result;
}


/*
* description               : get prev path.
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_prev(
    IN art_path_t *art_path,
    IN void *args)
{
    art_tree_t* artree                  = NULL;
    int32_t result                      = kOk;

    artree = art_path->t;
    assert(artree->art_crc == ART_ROOT_CRC);
    if(is_memtable_art(artree))
    {
        result = memtable_path_switch_prev_path(art_path) ;
    }
    else
    {
        LVOS_TP_START(ART_TP_ITERATOR_PRE_READPLOG_FILE_NOT_FOUND, &result)
        result = sstable_path_switch_prev_path(art_path, args);
        LVOS_TP_END
    }

    return result;
}


/*
* description               : get next path.
* art_path                  : pointer to path
* args                      : read plog para
*/
int32_t art_path_get_next(
    IN art_path_t *art_path,
    IN void *args)
{
    art_tree_t* artree                  = NULL;
    int32_t result                      = kOk;

    artree = art_path->t;
    //assert(artree->art_crc == ART_ROOT_CRC);
    if (artree->art_crc != ART_ROOT_CRC)
    {
        return kInvalidArgument;
    }
    if(is_memtable_art(artree))
    {
        result = memtable_path_switch_next_path(art_path) ;
    }
    else
    {
        LVOS_TP_START(ART_TP_ITERATOR_NEXT_READPLOG_FILE_NOT_FOUND, &result)
        result = sstable_path_switch_next_path(art_path, args);
        LVOS_TP_END
    }

    return result;
}

int32_t art_path_get_next_with_condition(
    IN art_tree_t* t,
    IN art_path_t *art_path,
    IN void *args,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN bool return_snap_once)
{
    art_tree_t* artree                  = NULL;
    int32_t result                      = kOk;

    artree = art_path->t;
    if (artree->art_crc != ART_ROOT_CRC)
    {
        return kInvalidArgument;
    }
    if(is_memtable_art(artree))
    {
        result = memtable_path_switch_next_path_with_condition(t, art_path, prefix_key, prefix_key_len, snapid_min, snapid_max, return_snap_once) ;
    }
    else
    {
        LVOS_TP_START(ART_TP_ITERATOR_NEXT_CONDITION_READPLOG_FILE_NOT_FOUND, &result)
        result = sstable_path_switch_next_path_with_condition(t, art_path, args, prefix_key, prefix_key_len, snapid_min, snapid_max, return_snap_once);
        LVOS_TP_END
    }

    return result;
}

/**
 * get a path that key bigger or equal than input.key
 * @arg path pointer to path
 * @arg key
 * @arg key_len
 * @arg snapshotid
 * args read plog para
 * @return ART_COMPARE_RESULT_EQUAL/ART_COMPARE_RESULT_GREATER means key in path equal/bigger than input.key, and if return ART_COMPARE_RESULT_LESS means cannot find a path.
 */
int32_t art_path_get_bigger_or_equal(
    IN art_path_t *art_path,
    IN uint8_t *key,
    IN uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    IN art_tree_t* t,
    IN void *args)
{
    int32_t result                      = kOk;
    int32_t inner_result                = ART_COMPARE_RESULT_ERR;
    art_tree_t* artree                  = NULL;
    art_node *root_node                 = NULL;

    artree = art_path->t;
    /* empty tree */
    if (check_art_empty(artree))
    {
        INDEX_LOG_WARN_LIMIT("artree is empty, apply_type[%d] art_type[%u]", artree->apply_type, artree->art_type);
        return kNotFound;
    }

    // first need to clear path
    clear_art_path(art_path, __func__, __LINE__);
    //assert(artree->art_crc == ART_ROOT_CRC);
    if (artree->art_crc != ART_ROOT_CRC)
    {
        return kInvalidArgument;
    }
    result = iterator_get_root_node(artree, art_path->key_path.iterator, args, (void**)&root_node);
    if (kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", result);
        return result;
    }
    if(is_memtable_art(artree))
    {
        inner_result = memtable_path_get_bigger_or_equal(art_path, root_node, 0, key, key_len, snapshotid, type, t);
        if(ART_COMPARE_RESULT_ERR == inner_result)
        {
        	result = kInternalError;
        }
    }
    else
    {
        result = sstable_path_get_bigger_or_equal(art_path, (void *)root_node, 0, key, key_len, snapshotid, type, t, args, &inner_result);
    }

    if (result != kOk)
    {
        return result;
    }

    return ART_COMPARE_RESULT_LESS != inner_result ? kOk : kNotFound;
}

int32_t art_path_get_bigger_or_equal_with_condition(
    IN art_path_t *art_path,
    IN art_tree_t* t,
    IN uint8_t *starting_key,
    IN uint32_t starting_key_len,
    IN uint8_t *prefix_key,
    IN uint32_t prefix_key_len,
    IN uint32_t snapid_min,
    IN uint32_t snapid_max,
    IN void *args)
{
    int32_t result                      = kOk;
    int32_t inner_result                = CHECK_STATUS_BUFF;
    art_tree_t* artree                  = NULL;
    art_node *root_node                 = NULL;

    artree = art_path->t;

    /* empty tree */
    if (check_art_empty(artree))
	{
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree is empty");
        return kOk;
	}

    // first need to clear path
    clear_art_path(art_path, __func__, __LINE__);
    if (artree->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "art crc check error!");
        return kInvalidArgument;
    }

    LVOS_TP_START(ART_TP_ITERATOR_GET_FIRST_ROOT_READPLOG_FILE_NOT_FOUND, &result)
    result = iterator_get_root_node(artree, art_path->key_path.iterator, args, (void**)&root_node);
    LVOS_TP_END
    if (kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", result);
        return result;
    }
    if(is_memtable_art(artree))
    {
        inner_result = memtable_path_get_bigger_or_equal_with_condition(art_path, root_node, 0, starting_key, starting_key_len,
            prefix_key, prefix_key_len, snapid_min, snapid_max, t);
        if(ART_COMPARE_RESULT_ERR == inner_result)
        {
        	result = kInternalError;
        }
    }
    else
    {
        result = sstable_path_get_bigger_or_equal_with_conditon(art_path, (void *)root_node, 0, starting_key, starting_key_len,
            prefix_key, prefix_key_len, snapid_min, snapid_max, t, args, &inner_result);
    }

    if (result != kOk)
    {
        clear_art_path(art_path, __func__, __LINE__);
        return result;
    }

    /* 当get first失败时，返回kOk，由外部调用is_valid判断是否找到内容*/
    if(CHECK_KEY_OUT_OF_RANGE == inner_result)
    {
        clear_art_path(art_path, __func__, __LINE__);
    }

    return kOk;
}


/**
 * get a path that key equal than input.key, and with smallest snap id
 * @arg path pointer to path
 * @arg key
 * @arg key_len
 * @arg read plog para
 * @return return kok means found, or other code.
 */
int32_t art_path_get_equal_key_smallest_snapid(
    IO art_path_t *art_path,
    IN uint8_t* key,
    IN uint32_t key_len,
    IN void *args)
{
    int32_t result                      = kOk;
    art_tree_t* artree                  = NULL;
    art_node *root_node                 = NULL;

    artree = art_path->t;
    /* empty tree */
    if (check_art_empty(artree))
	{
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree is empty");
        return kNotFound;
	}

    // first need to clear path
    clear_art_path(art_path, __func__, __LINE__);
    assert(artree->art_crc == ART_ROOT_CRC);
    result = iterator_get_root_node(artree, art_path->key_path.iterator, args, (void**)&root_node);
    if (kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", result);
        return result;
    }
    if(is_memtable_art(artree))
    {
        result = memtable_path_get_equal_key_smallest_snapid(art_path, root_node, 0, key, key_len);
    }
    else
    {
        result = sstable_path_get_equal_key_smallest_snapid(art_path, root_node, 0, key, key_len, args);
    }

    return result;
}


/**
 * get key from path
 * @arg path pointer to path
 * @out key
 * @out key_len
 * @return return kok means found, or other code.
 */
int32_t art_path_get_key(
    IO art_path_t *art_path,
    IN uint8_t** key,
    IN uint32_t* key_len)
{
    *key = art_path->key_path.prefix;
    *key_len = get_prefix_len_in_key_path(&art_path->key_path);
    return kOk;
}


/**
 * get value from path
 * @arg path pointer to path
 * @out value
 * @out value_len
 * @return return kok means found, or other code.
 */
int32_t art_path_get_value(
    IO art_path_t *art_path,
    OUT uint8_t** value,
    OUT uint32_t* value_len)
{
    int32_t result                      = kOk;

    if(is_memtable_art(art_path->t))
    {
        result = memtable_path_get_value(art_path, value, value_len);
    }
    else
    {
        result = sstable_path_get_value(art_path, value, value_len);
    }

    return result;
}


/**
 * get snapid from path
 * @arg path pointer to path
 * @out snapid
 * @return return kok means found, or other code.
 */
int32_t art_path_get_snapid(
    IO art_path_t *art_path,
    OUT uint32_t* snapid)
{
    int32_t result                      = kOk;

    if(is_memtable_art(art_path->t))
    {
        result = memtable_path_get_snapid(art_path, snapid);
    }
    else
    {
        result = sstable_path_get_snapid(art_path, snapid);
    }

    return result;
}


/**
 * check whether the value of op code is to delete
 * @arg path pointer to path
 * @out isdelete
 * @return return kok means found, or other code.
 */
int32_t art_path_check_is_delete(
    IO art_path_t *art_path,
    OUT bool* isdelete)
{
    int32_t result                      = kOk;

    if(is_memtable_art(art_path->t))
    {
        result = memtable_path_check_is_delete(art_path, isdelete);
    }
    else
    {
        result = sstable_path_check_is_delete(art_path, isdelete);
    }

    return result;
}


/**
 * check whether check if the path is valid.
 * @arg path pointer to path
 * @out isvalid
 * @return return kok means found, or other code.
 */
int32_t art_path_check_is_valid(
    IO art_path_t *art_path,
    OUT bool* isvalid)
{
    *isvalid = !check_key_path_empty(&art_path->key_path);

    return kOk;
}

/*
* description               : clear batch get path.
* get_path                  : batch get struct, contains artree and path
*/
void art_clear_path(batch_get_path_t *get_path)
{
    void *node                              = NULL;
    art_path_t *art_path                    = &get_path->art_path;
    art_tree_t *artree                      = get_path->artree;
    bool is_sstable                         = is_sstable_art(artree);
    artree_ext_operations_t *ext_ops        = &artree->ops;

    while(!check_key_path_empty(&art_path->key_path))
    {
        node = pop_node_from_key_path(&art_path->key_path);

        is_sstable ? art_release_node(ext_ops->release_buffer, node) : 0;
    }

	while (!check_snap_path_empty(&art_path->snap_path))
	{
		node = pop_node_from_snap_path(&art_path->snap_path);

		is_sstable ? art_release_node(ext_ops->release_buffer, node) : 0;
	}

	clear_key_path_prefix(&art_path->key_path);
	clear_snap_path_prefix(&art_path->snap_path);

    ART_LOGDEBUG(ART_LOG_ID_4B00, "art clear path clear succ, key_path_index[%d] snap_path_index[%d].",
                                    art_path->key_path.path_index, art_path->snap_path.path_index);
}

#ifdef __cplusplus
}
#endif

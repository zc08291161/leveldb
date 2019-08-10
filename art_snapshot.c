#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#ifndef _SCM_PCLINT_
#include "art_log.h"
#include "art_iterator.h"
#include "art_snapshot_memtable.h"
#include "art_memtable_io.h"
#include "art_sstable_io.h"
#endif
#include "art_snapshot_sstable.h"

//MODULE_ID(PID_ART);

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************************/

int snap_get_min(IN int a, IN int b)
{
    return (a < b) ? a : b;
}

int snap_check_prefix(
    IN const art_snap_node* n,
    IN const uint32_t snapid,
    IN int snapid_len,
    IN int depth)
{
    int max_cmp     = snap_get_min(n->partial_len, snapid_len - depth);
    int idx         = 0;
    int begin_idx   = SNAPID_LEN - depth - 1;

    /* node->partial compare with snapid->partial */
    for (;idx < max_cmp;idx++, begin_idx--)
    {
        if (n->partial[idx] != GET_CHAR_FROM_INT(snapid, begin_idx))
        {
            return idx;
        }
    }

    return idx;
}

void snap_copy_partial(
    IN art_snap_node* new_node,
    IN const uint32_t snapid,
    IN uint32_t depth,
    IN uint32_t longest_prefix)
{
    uint32_t idx       = 0;
    uint32_t begin_idx = SNAPID_LEN - depth - 1;
    for (;idx < longest_prefix;idx++, begin_idx--)
    {
        new_node->partial[idx] = GET_CHAR_FROM_INT(snapid, begin_idx);
    }
    new_node->partial_len = (uint8_t)longest_prefix;
    return;
}

int32_t snap_compare_prefix_and_snapid(
    IN const art_snap_node* n,
    IN const uint32_t snapid,
    IN uint32_t snapid_len,
    IN uint32_t depth)
{
    int max_cmp        = snap_get_min(n->partial_len, (int)(snapid_len - depth));    
    int idx            = 0;
    uint32_t begin_idx = snapid_len - depth - 1;

    /* judge whether snap_id is same as node->partial */
    for (; idx < max_cmp; idx++,begin_idx--)
    {
        if (n->partial[idx] == GET_CHAR_FROM_INT(snapid, begin_idx))
        {
            continue;
        }

        return (n->partial[idx] > GET_CHAR_FROM_INT(snapid, begin_idx)) ? PathGreaterToSnapId : PathLessToSnapId;
    }

    return PathEqualToSnapId;
}

uint8_t snap_get_key_by_idx_inner(
    IN art_snap_node* node,
    IN int idx)
{
    union
    {
        memtable_art_snap_node4* p1;
        memtable_art_snap_node16* p2;
        memtable_art_snap_node48* p3;
        memtable_art_snap_node256* p4;
        sstable_art_snap_node4* p5;
        sstable_art_snap_node16* p6;
        sstable_art_snap_node48* p7;
        sstable_art_snap_node256* p8;
    }p;
    
    /* judge node_type to select node by index */
    switch (node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:            
            p.p1 = (memtable_art_snap_node4*)node;
            return p.p1->keys[idx];

        case MEMTABLE_ART_SNAPNODE16:
            p.p2 = (memtable_art_snap_node16*)node;
            return p.p2->keys[idx];

        case MEMTABLE_ART_SNAPNODE48:            
            p.p3 = (memtable_art_snap_node48*)node;
            return p.p3->keys[idx];

        case MEMTABLE_ART_SNAPNODE256:
            return (uint8_t)idx;
            
        case SSTABLE_ART_SNAPNODE4:            
            p.p5 = (sstable_art_snap_node4*)node;
            return p.p5->keys[idx];
        
        case SSTABLE_ART_SNAPNODE16:
            p.p6 = (sstable_art_snap_node16*)node;
            return p.p6->keys[idx];
        
        case SSTABLE_ART_SNAPNODE48:            
            p.p7 = (sstable_art_snap_node48*)node;
            return p.p7->keys[idx];
        
        case SSTABLE_ART_SNAPNODE256:
            return (uint8_t)idx;            

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type); 
            ART_ASSERT(0);
            return kIOError;
    }
    
    return kOk;
}
        

/**
 * Checks if a leaf matches
 * @return 0 on success.
 */
bool snap_leaf_matches(IN art_tree_t* t, IN void* leaf, IN uint32_t snapid)
{
    art_leaf_operations_t  *leaf_ops = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(t)].leaf_ops;
    /* Compare leaf node snapid with snapid*/
    return  leaf_ops->get_snap_id(leaf) == snapid;
}

//begin_idx == 256
int32_t check_and_get_idx_in_node256(    
    IN art_snap_node* node,    
    IN uint32_t snapid,
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    OUT int* child_idx)
{
    UNREFERENCE_PARAM(begin_idx);
    int32_t  ret    = PathEqualToSnapId;
    uint32_t depth  = iterator->snapid_length;
    int32_t  idx    = 0;
    uint8_t  key    = 0;
    
    ret = snap_compare_prefix_and_snapid(node, snapid, SNAPID_LEN, iterator->snapid_length);
    if (PathEqualToSnapId != ret)
    {
        /* return PathLessToSnapId(directly use default child idx) or PathGreaterToSnapId(the current path is error) */
        return ret;
    }
    
    depth += node->partial_len;
    key = GET_CHAR_FROM_INT(snapid, (SNAPID_LEN - depth - 1));
    for (idx = key; idx >= 0; idx--)
    {
        /* find the idx which is <= snapid */
        if (check_art_snap_node256_valid((void*)iterator->t, node, (uint8_t)idx))
        {
           break;
        }
    }

    if (idx == key)
    {        
        /* means is equal */
        *child_idx = idx;
        return PathEqualToSnapId;
    }
    else if (idx < 0)
    {
        return PathGreaterToSnapId;
    }
    else
    {        
        *child_idx = idx;
        return PathLessToSnapId;
    }
}

//begin_idx == 256
int32_t check_and_get_idx_in_node48(    
    IN art_snap_node* node,    
    IN uint32_t snapid,
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    OUT int* child_idx)
{
    UNREFERENCE_PARAM(begin_idx);
    int32_t  ret    = PathEqualToSnapId;
    uint32_t depth  = iterator->snapid_length;
    int32_t  idx    = 0;
    uint8_t  key    = 0;
    
    ret = snap_compare_prefix_and_snapid(node, snapid, SNAPID_LEN, iterator->snapid_length);
    if (PathEqualToSnapId != ret)
    {
        /* return PathLessToSnapId(directly use default child idx) or PathGreaterToSnapId(the current path is error) */
        return ret;
    }
    
    depth += node->partial_len;
    key = GET_CHAR_FROM_INT(snapid, (SNAPID_LEN - depth - 1));
    for (idx = key; idx >= 0; idx--)
    {
        /* find the idx which is <= snapid */
        if (snap_get_key_by_idx_inner(node, idx) != 0)
        {
           break;
        }
    }

    if (idx == key)
    {        
        /* means is equal */
        *child_idx = idx;
        return PathEqualToSnapId;
    }
    else if (idx < 0)
    {
        return PathGreaterToSnapId;
    }
    else
    {
        *child_idx = idx;
        return PathLessToSnapId;
    }
}

int32_t check_and_get_idx_in_node16(    
    IN art_snap_node* node,    
    IN uint32_t snapid,
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    OUT int* child_idx)
{
    int32_t  ret     = PathEqualToSnapId;
    uint32_t depth   = iterator->snapid_length;
    int32_t  idx     = 0;
    uint8_t  key1    = 0;
    uint8_t  key2    = 0;
    
    ret = snap_compare_prefix_and_snapid(node, snapid, SNAPID_LEN, iterator->snapid_length);
    if (PathEqualToSnapId != ret)
    {
        /* return PathLessToSnapId(directly use default child idx) or PathGreaterToSnapId(the current path is error) */
        return ret;
    }
    
    depth += node->partial_len;
    for (idx = begin_idx; idx >= 0; idx--)
    {
        key1 = snap_get_key_by_idx_inner(node, idx);
        key2 = GET_CHAR_FROM_INT(snapid, (SNAPID_LEN - depth - 1));
        
        if (key1 > key2)
        {
           /* this key is not conform the condition */
           continue;
        }
        
        /* find the idx which is <= snapid */
        *child_idx = idx;

        return (key1 == key2) ? PathEqualToSnapId : PathLessToSnapId;
    }

    return PathGreaterToSnapId;
}

int32_t check_and_get_idx_in_node4(    
    IN art_snap_node* node,    
    IN uint32_t snapid,
    IN int begin_idx,
    IN uint32_t depth,
    OUT int* child_idx)
{
    int32_t ret     = PathEqualToSnapId;
    int32_t idx     = 0;
    uint8_t key1    = 0;
    uint8_t key2    = 0;
    
    ret = snap_compare_prefix_and_snapid(node, snapid, SNAPID_LEN, depth);
    if (PathEqualToSnapId != ret)
    {
        /* return PathLessToSnapId(directly use default child idx) or PathGreaterToSnapId(the current path is error) */
        return ret;
    }
    
    depth += node->partial_len;
    for (idx = begin_idx; idx >= 0; idx--)
    {
        key1 = snap_get_key_by_idx_inner(node, idx);
        key2 = GET_CHAR_FROM_INT(snapid, (SNAPID_LEN - depth - 1));
        if (key1 > key2)
        {
            /* this key is not conform the condition*/
           continue;
        }

        /* find the idx which is <= snapid */
        *child_idx = idx;
        return (key1 == key2) ? PathEqualToSnapId : PathLessToSnapId;
    }

    return PathGreaterToSnapId;
}

int32_t snap_seek_get_path_node256_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para)
{
    void* child	            	= NULL;
    art_tree_t* t				= iterator->t;
    int idx						= begin_idx;
    int current_flag			= PathLessToSnapId;
    int flag					= iterator->path[iterator->path_index].flag;
    art_snap_node* node			= (art_snap_node*)get_buff_from_origin_node((void*)iterator->t, origin_node);
    int32_t ret                 = kArtSnapIdNotFound;
    
    /* directly use the begin_idx */
    if (PathEqualToSnapId == flag)
    {
        /* get the current_flag and idx of node */
        current_flag = check_and_get_idx_in_node256(node, seek_para->snapshotid, begin_idx, iterator, &idx);
        if (PathGreaterToSnapId == current_flag)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap seek node256 the current snapid more than snapid[%d] of snap_node[%p], return notFound.",
                         seek_para->snapshotid, node);

            if(is_sstable_art(t))
            {
                art_release_node(t->ops.release_buffer, origin_node);
            }
            return kArtSnapIdNotFound;
        }
        if (PathLessToSnapId == current_flag && idx == begin_idx)
        {
            while (!check_art_snap_node256_valid(t, node, (uint8_t)idx)) 
            { 
                idx--;
            }
        }
    }
    else
    {   
        while (!check_art_snap_node256_valid(t, node, (uint8_t)idx)) 
        { 
            idx--;
        }
    }
    
    iterator->snapid_length += node->partial_len;
    
    /* move the parent_node to path */
    /*sstable:need to record origin_node, and memtable: node == origin_node; */    
    //iterator->path[iterator->path_index].node = node;
    iterator->path[iterator->path_index].node = origin_node;
    iterator->path_index++;

    /* get snap child by index */
    ret = get_snap_child_addr_by_index(node, idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node256 get snap child is NULL,idx=[%d].", idx);
        return ret;
    }
    /* record the pos_in_parentNode and flag in child position */
    iterator->path[iterator->path_index].pos_in_parentNode = idx;
    iterator->path[iterator->path_index].flag = (SNAP_COMPARE_CODE_TYPE)current_flag;

    iterator->snapid_length++;

    if (is_memtable_art(t))
    {
        /* seek get snap path in memtable */
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        /* seek get snap path in sstable */
        return sstable_snap_seek_get_path(iterator, (void*)child, seek_para);
    }
}

int32_t snap_seek_get_path_node48_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para)
{
    void* child		            = NULL;
    art_tree_t* t				= iterator->t;
    int idx						= 0;
    int current_flag			= PathLessToSnapId;
    int flag					= iterator->path[iterator->path_index].flag;
    int key_idx					= begin_idx;
    art_snap_node* node			= (art_snap_node*)get_buff_from_origin_node((void*)iterator->t, origin_node);
    int32_t ret                 = kArtSnapIdNotFound;

    if (begin_idx >= NODE_256_MAX_CHILDREN_NUM)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node48 get begin index[%d] is invalid.", begin_idx);
        return kArtSnapIdNotFound;
    }
    
    /* directly use the begin_idx */
    if (PathEqualToSnapId == flag)
    {
        /* get the current_flag and idx of node */
        current_flag = check_and_get_idx_in_node48(node, seek_para->snapshotid, begin_idx, iterator, &key_idx);
        if (PathGreaterToSnapId == current_flag)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap seek node48 the current snapid more than snapid[%d] of snap_node[%p], return notFound.",
                         seek_para->snapshotid, node);
            
            if(is_sstable_art(t))
            {
                art_release_node(t->ops.release_buffer, origin_node);
            }
            return kArtSnapIdNotFound;
        }
        if (PathLessToSnapId == current_flag && key_idx == begin_idx)
        {
            while (!snap_get_key_by_idx_inner(node, key_idx)) 
            { 
                key_idx--;
            }
        }
    }
    else
    {        
        while (!snap_get_key_by_idx_inner(node, key_idx)) 
        { 
            key_idx--;
        }
    }
    
    iterator->snapid_length += node->partial_len;

    /* move the parent_node to path */
    /*sstable:need to record origin_node, and memtable: node == origin_node; */    
    //iterator->path[iterator->path_index].node = node;
    iterator->path[iterator->path_index].node = origin_node;

    iterator->path_index++;

    idx = snap_get_key_by_idx_inner(node, key_idx) - 1;
    
    /* get snap child by index */
    ret = get_snap_child_addr_by_index(node, idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node48 get snap child is NULL,idx=[%d].", idx);
        return ret;
    }
    /* record the pos_in_parentNode and flag in child position */
    iterator->path[iterator->path_index].pos_in_parentNode = key_idx;
    iterator->path[iterator->path_index].flag = (SNAP_COMPARE_CODE_TYPE)current_flag;

    iterator->snapid_length++;

    if (is_memtable_art(t))
    {
        /* seek get snap path in memtable */
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        /* seek get snap path in sstable */
        return sstable_snap_seek_get_path(iterator, (void*)child, seek_para);
    }
}

int32_t snap_seek_get_path_node16_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para)
{
    void* child	            	= NULL;
    art_tree_t* t				= iterator->t;
    int idx						= begin_idx;
    int current_flag			= PathLessToSnapId;
    int flag					= iterator->path[iterator->path_index].flag;
    art_snap_node* node			= (art_snap_node*)get_buff_from_origin_node((void*)iterator->t, origin_node);
    int32_t ret                 = kArtSnapIdNotFound;

    if (begin_idx >= NODE_256_MAX_CHILDREN_NUM)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node256 get begin index[%d] is invalid.", begin_idx);
        return kArtSnapIdNotFound;
    }
    
    /* directly use the begin_idx */
    if (PathEqualToSnapId == flag)
    {
    
        /* get the current_flag and idx of node */
        current_flag = check_and_get_idx_in_node16(node, seek_para->snapshotid, begin_idx, iterator, &idx);
        if (PathGreaterToSnapId == current_flag)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap seek node16 the current snapid more than snapid[%d] of snap_node[%p], return notFound.",
                         seek_para->snapshotid, node);
            
            if(is_sstable_art(t))
            {
                art_release_node(t->ops.release_buffer, origin_node);
            }
            return kArtSnapIdNotFound;
        }
    }
    
    iterator->snapid_length += node->partial_len;
    
    /* move the parent_node to path */
    /*sstable:need to record origin_node, and memtable: node == origin_node; */    
    //iterator->path[iterator->path_index].node = node;
    iterator->path[iterator->path_index].node = origin_node;
    iterator->path_index++;

    /* get snap child by index */
    ret = get_snap_child_addr_by_index(node, idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node16 get snap child is NULL,idx=[%d].", idx);
        return ret;
    }
    /* record the pos_in_parentNode and flag in child position */
    iterator->path[iterator->path_index].pos_in_parentNode = idx;
    iterator->path[iterator->path_index].flag = (SNAP_COMPARE_CODE_TYPE)current_flag;

    iterator->snapid_length++;

    if (is_memtable_art(t))
    {
        /* seek get snap path in memtable */
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        /* seek get snap path in sstable */
        return sstable_snap_seek_get_path(iterator, (void*)child, seek_para);
    }
}

int32_t snap_seek_get_path_node4_implement(
    IN void* origin_node,    
    IN int begin_idx,
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para)
{
    void* child	            	= NULL;
    art_tree_t* t				= iterator->t;
    int idx						= begin_idx;
    int current_flag			= PathLessToSnapId;
    int flag					= iterator->path[iterator->path_index].flag;
    art_snap_node* node			= (art_snap_node*)get_buff_from_origin_node((void*)iterator->t, origin_node);
    int32_t ret                 = kArtSnapIdNotFound;
    
    /* directly use the begin_idx */
    if (PathEqualToSnapId == flag)
    {
        /* get the current_flag and idx of node */
        current_flag = check_and_get_idx_in_node4(node, seek_para->snapshotid, begin_idx, iterator->snapid_length, &idx);
        if (PathGreaterToSnapId == current_flag)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap seek node4 the current snapid more than snapid[%d] of snap_node[%p], return notFound.",
                         seek_para->snapshotid, node);

            if(is_sstable_art(t))
            {
                art_release_node(t->ops.release_buffer, origin_node);
            }
            return kArtSnapIdNotFound;
        }
    }
    
    iterator->snapid_length += node->partial_len;

    /* move the parent_node to path */
    /*sstable:need to record origin_node, and memtable: node == origin_node; */    
    //iterator->path[iterator->path_index].node = node;
    iterator->path[iterator->path_index].node = origin_node;
    iterator->path_index++;   

    /* get snap child by index */
    ret = get_snap_child_addr_by_index(node, idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node4 get snap child is NULL,idx=[%d].", idx);
        return ret;
    }
    /* record the pos_in_parentNode and flag in child position */
    iterator->path[iterator->path_index].pos_in_parentNode = idx;
    iterator->path[iterator->path_index].flag = (SNAP_COMPARE_CODE_TYPE)current_flag;

    iterator->snapid_length++;

    if (is_memtable_art(t))
    {
        /* seek get snap path in memtable */
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        /* seek get snap path in sstable */
        return sstable_snap_seek_get_path(iterator, (void*)child, seek_para);
    }
}

void update_path(IN art_snap_iterator* iterator, IN int child_idx)
{
    /* move child into path and get child of child */
    iterator->path[iterator->path_index].flag = PathLessToSnapId;
    iterator->path[iterator->path_index].pos_in_parentNode = child_idx;
    
    /* update depth */
    iterator->snapid_length++;
}

int32_t snap_seek_update_path_implement(
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node, 
    IN artree_seek_para_t *seek_para)
{
    art_tree_t* t       = iterator->t;
    void* origin_node   = NULL;
    
    iterator->path_index--;
    iterator->snapid_length -= parent_node->partial_len;

    if (is_memtable_art(t))
    {
        return memtable_snap_seek_update_path((void*)iterator, seek_para);
    }else
    {        
        origin_node = iterator->path[iterator->path_index].node;        
        assert((art_get_buff(t->ops.get_buffer, origin_node) == (char*)parent_node));/*lint !e666*/
        art_release_node(t->ops.release_buffer, origin_node);
        
        return sstable_snap_seek_update_path(iterator, seek_para);
    }
}

int32_t snap_seek_update_node256(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para)
{
    void* child = NULL; 
    int32_t ret = kArtSnapIdNotFound;
    
    while ((child_idx >= 0) && !check_art_snap_node256_valid(t, parent_node, (uint8_t)child_idx)) 
    { 
        child_idx--; 
    }

    if (child_idx < 0)
    {
        /* parent_node is invalid too, remove parent_node and continue */
        return snap_seek_update_path_implement(iterator, parent_node, seek_para);
    }
    
    /* the child node must be less than snapid, no need check */ 
    ret = get_snap_child_addr_by_index(parent_node, child_idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node256 get snap child is NULL,idx=[%d].", child_idx);
        return ret;
    }
    /* move child into path and get child of child, and update depth */
    update_path(iterator, child_idx);
    
    if (is_memtable_art(t))
    {
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        return sstable_snap_seek_get_path(iterator, child, seek_para);
    }
}

int32_t snap_seek_update_node48(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para)
{
    void* child = NULL; 
    int32_t ret = kArtSnapIdNotFound;
    uint8_t idx = 0;

    while ((child_idx >= 0) && !snap_get_key_by_idx_inner(parent_node, child_idx)) 
    {
        child_idx--; 
    }
    
    if (child_idx < 0)
    {
        /* parent_node is invalid too, remove parent_node and continue */
        return snap_seek_update_path_implement(iterator, parent_node, seek_para);
    }
    
    idx = snap_get_key_by_idx_inner(parent_node, child_idx) - 1;
    
    /* the child node must be less than snapid, no need check */
    ret = get_snap_child_addr_by_index(parent_node, idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node48 get snap child is NULL,idx=[%d].", idx);
        return ret;
    }

    /* move child into path and get child of child, and update depth */
    update_path(iterator, child_idx);
    
    if (is_memtable_art(t))
    {
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        return sstable_snap_seek_get_path(iterator, child, seek_para);
    }
}

int32_t snap_seek_update_node16(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para)
{
    void* child = NULL; 
    int32_t ret = kArtSnapIdNotFound;  
    
    if (child_idx < 0)
    {
        /* parent_node is invalid too, remove parent_node and continue */
        return snap_seek_update_path_implement(iterator, parent_node, seek_para);
    }
    
    /* the child node must be less than snapid, no need check */
    ret = get_snap_child_addr_by_index(parent_node, child_idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node16 get snap child is NULL,idx=[%d].", child_idx);
        return ret;
    }

    /* move child into path and get child of child, and update depth */
    update_path(iterator, child_idx);
    
    if (is_memtable_art(t))
    {
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        return sstable_snap_seek_get_path(iterator, child, seek_para);
    }
}

int32_t snap_seek_update_node4(
    IN art_tree_t* t,
    IN art_snap_iterator* iterator, 
    IN art_snap_node* parent_node,
    IN int child_idx,
    IN artree_seek_para_t *seek_para)
{
    void* child = NULL; 
    int32_t ret = kArtSnapIdNotFound;  
    
    if (child_idx < 0)
    {
        /* parent_node is invalid too, remove parent_node and continue */
        return snap_seek_update_path_implement(iterator, parent_node, seek_para);
    }
    
    /* the child node must be less than snapid, no need check */
    ret = get_snap_child_addr_by_index(parent_node, child_idx, t->ops.read, seek_para->file, &child);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable node4 get snap child is NULL,idx=[%d].", child_idx);
        return ret;
    }

    /* move child into path and get child of child, and update depth */
    update_path(iterator, child_idx);
    
    if (is_memtable_art(t))
    {
        return memtable_snap_seek_get_path(iterator, (art_snap_node*)child, seek_para);
    }else
    {
        return sstable_snap_seek_get_path(iterator, child, seek_para);
    }
}

bool check_art_snap_node256_valid(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN uint8_t child_index)
{
    memtable_art_snap_node256* memtable_node = (memtable_art_snap_node256*) node;
    sstable_art_snap_node256* sstable_node   = (sstable_art_snap_node256*) node;
    
    if (t->art_type == MEMTABLE_ART_TYPE)
    {
        return (memtable_node->children[child_index].mem_addr != NULL);
    }
    else if (t->art_type == SSTABLE_ART_TYPE)
    {
        return check_art_snap_node256_child_valid(sstable_node, child_index);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown art type %d.", t->art_type);        
        ART_ASSERT(0);
        return false;
    }
}

uint8_t snap_get_key_by_idx(
    IN art_snap_node* node,
    IN int idx)
{
    union
    {
        memtable_art_snap_node4* p1;
        memtable_art_snap_node16* p2;
        memtable_art_snap_node48* p3;
        memtable_art_snap_node256* p4;
        sstable_art_snap_node4* p5;
        sstable_art_snap_node16* p6;
        sstable_art_snap_node48* p7;
        sstable_art_snap_node256* p8;
    }p;
    
    /* judge node_type to select node by index */
    switch (node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:            
            p.p1 = (memtable_art_snap_node4*)node;
            return p.p1->keys[idx];

        case MEMTABLE_ART_SNAPNODE16:
            p.p2 = (memtable_art_snap_node16*)node;
            return p.p2->keys[idx];

        case MEMTABLE_ART_SNAPNODE48:            
            return (uint8_t)idx;

        case MEMTABLE_ART_SNAPNODE256:
            return (uint8_t)idx;
            
        case SSTABLE_ART_SNAPNODE4:            
            p.p5 = (sstable_art_snap_node4*)node;
            return p.p5->keys[idx];
        
        case SSTABLE_ART_SNAPNODE16:
            p.p6 = (sstable_art_snap_node16*)node;
            return p.p6->keys[idx];
        
        case SSTABLE_ART_SNAPNODE48:            
            return (uint8_t)idx;
        
        case SSTABLE_ART_SNAPNODE256:
            return (uint8_t)idx;            

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);            
            ART_ASSERT(0);
            return kIOError;
    }
    
    return kOk;
}


bool is_snap_inner_node(IN art_snap_node* node)
{
    return (((node->node_type >= MEMTABLE_ART_SNAPNODE4) && (node->node_type <= MEMTABLE_ART_SNAPNODE256))
        || ((node->node_type >= SSTABLE_ART_SNAPNODE4) && (node->node_type <= SSTABLE_ART_SNAPNODE256)));
}

#ifdef __cplusplus
}
#endif

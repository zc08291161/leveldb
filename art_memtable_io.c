#include "art_log.h"
#include "art_ftds.h"
#ifndef _SCM_PCLINT_
#include "art_snapshot_memtable.h"
#include "art_epoch.h"
#include "art_memtable_io.h"
#include "art_sstable_io.h"
#include "art_iterator.h"
#include "art_snap_path_impl.h"
#include "art_key_path_impl.h"
#include "art_stat.h"
#include "art_sfmea.h"
#endif

#ifndef ARTREE_DEBUG
#ifndef _SCM_PCLINT_
#include "../../../../debug/common_ftds.h"
#endif
#endif

#define ART_MEMTABLE_IO_CHECK_ART_INNER_NODE(node, errcode) \
    if(!is_art_inner_node((void *)node))    \
    {   \
        ART_LOGERROR(ART_LOG_ID_4B00, "unexpected input, inner_node [%u.%u.%u].", node->node_type, node->num_children, node->partial_len);    \
        return errcode;    \
    }
    

#ifdef __cplusplus
extern "C" {
#endif


#if DESC("memtable for batch get interface")
/*
* description               : batch search one key in memtable.
* node                      : root node
* get_path                  : batch get struct, contains artree and path
* key                       : keys which need to get
* key_len                   : length of key, the len of every key is same
* snapshotid                : snapids which need to get
* index                     : the serial number of key which is get
* return                    : the first node of next get
*/
art_node* memtable_get_first_node_for_batch_search(art_node *node, batch_get_path_t *get_path, uint8_t *key, uint32_t key_len, uint8_t *snapid, uint32_t index)
{
    art_node *return_node      = NULL;
    
    if(index > 0)
    {
        /* path reuse */
        return_node = (art_node*)reuse_node_impl(get_path, node, key, key_len, snapid);
    }
    else
    {
        /* first key search */
        return_node = node;
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get first node succ, return_node[%p] key[%p] key_len[%d]", 
                 return_node, key, key_len);
    return return_node;
}


/*
* description               : get and push snap node in memtable.
* get_path                  : batch get struct, contains artree and path
* parent_node               : the root node of snap tree
* snapshotid                : the snapid which is found
* return                    : if succ return kOk, otherwise return kNotFound
*/
int32_t memtable_snap_get_valid_path(batch_get_path_t *get_path, art_snap_node *parent_node, uint32_t snapshotid)
{
    art_snap_node *cur_node                 = NULL;
    art_snap_node *next_node                = NULL;
    uint32_t pos_in_parent                  = 0;
    uint32_t idx_in_parent                  = 0;
    uint8_t key                             = 0;
    uint32_t depth                          = 0;
    uint32_t found_snap_id                  = 0;
    int32_t ret                             = kOk; 
    art_tree_t *artree                      = get_path->artree;
    art_leaf_operations_t *leaf_ops         = &artree->art_ops->leaf_ops;
   
    art_path_t *art_path = &get_path->art_path;
    cur_node             = parent_node;
    
    /* search in snap tree */
    while(is_snap_inner_node(cur_node))
    {
        pos_in_parent = get_last_node_child_pos_in_snap_path(&art_path->snap_path);
        depth = get_prefix_len_in_snap_path(&art_path->snap_path);

        /* get snap node to path */
        ret = memtable_snap_search_node_equal(cur_node, snapshotid, SNAPID_LEN, depth, &next_node, &idx_in_parent);
        if(kOk != ret)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get snap node fail, cur_node[%p] snapshotid[%d] depth[%d].", 
                         cur_node, snapshotid, depth);
            break;
        }
        
        /* add current node and partial to snap path */
        push_node_into_snap_path(&art_path->snap_path, (void *)cur_node, pos_in_parent);
        push_key_into_snap_path(&art_path->snap_path, cur_node->partial, cur_node->partial_len);
    
        /* set next node child_pos and key prefix */
        set_last_child_pos_in_snap_path(&art_path->snap_path, idx_in_parent);
        key = snap_get_key_by_idx(cur_node, idx_in_parent);
        push_key_into_snap_path(&art_path->snap_path, &key, 1);

        cur_node = next_node;
        pos_in_parent = idx_in_parent;
    }

    if(kOk != ret)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get key in snap tree fail, cur_node[%p] key[%p] key_len[%d] snapshotid[%d] snapid_prefix_len[%d] ret[%d].", 
                     cur_node, art_path->key_path.prefix, art_path->key_path.prefix_len, snapshotid, art_path->snap_path.prefix_len, ret);
        return ret;
    }

    assert(is_art_leaf_node(cur_node));
    found_snap_id = leaf_ops->get_snap_id(cur_node);
    if(found_snap_id == snapshotid)
    {
        /* get key succ */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get leaf succ(snapid is same), key[%p] snapshotid[%d] ret[%d].", 
                     art_path->key_path.prefix, snapshotid, ret);
        
        push_node_into_snap_path(&art_path->snap_path, (void *)cur_node, pos_in_parent);
        ret = kOk;
    }
    else
    {
        /* get key fail */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get leaf fail(snapid is diff), key[%p] found_snapid[%d] snapid[%d] ret[%d].", 
                     art_path->key_path.prefix, found_snap_id, snapshotid, ret);

        ret = kNotFound;
    }

    return ret; 
}

/*
* description               : get and push node in memtable.
* cur_node                  : the first node which is found 
* get_path                  : batch get struct, contains artree and path
* key                       : key which need to get
* key_len                   : length of key
* snapid                    : snapid which need to get
* return                    : if succ return kOk, otherwise return kNotFound
*/
int32_t memtable_get_valid_path(art_node *cur_node, batch_get_path_t *get_path, uint8_t *key, uint32_t key_len, uint32_t snapid)
{
    int32_t ret                 = kNotFound;
    uint32_t idx_in_parent      = 0;
    uint32_t pos_in_parent      = 0;    
    uint8_t c                   = 0;
    uint32_t depth              = 0;
    art_node *next_node         = NULL;    
    art_path_t *art_path        = &(get_path->art_path);
    
   /* cur node is snap node */
   if(!is_art_inner_node(cur_node))
   {
       /* search in snap tree */
       ret = memtable_snap_get_valid_path(get_path, (art_snap_node *)cur_node, snapid);

       return ret;
   }
   
   while(is_art_inner_node(cur_node))
   {
        /* get node child pos */
    	pos_in_parent = get_last_node_child_pos_in_key_path(&art_path->key_path);

        /* get key depth before the node is pushed to path */
        depth = get_prefix_len_in_key_path(&art_path->key_path);
        
        /* get equal child */
        ret = memtable_search_node_equal(cur_node, key, key_len, depth, &next_node, &idx_in_parent);
        if(kOk != ret)
        {   
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable search child node fail, cur_node[%p] key[%p] key_len[%d] depth[%d].", 
                         cur_node, key, key_len, depth);
            break;
        }

        /* add current node and partial to key path */
        push_node_into_key_path(&art_path->key_path, (void *)cur_node, pos_in_parent);
        push_key_into_key_path(&art_path->key_path, cur_node->partial, cur_node->partial_len);
    
        /* set next node child_pos and key prefix if not end node */
        set_last_child_pos_in_key_path(&art_path->key_path, idx_in_parent);
        
        if(END_POSITION_INDEX != idx_in_parent)
        {
            c = art_get_key_by_idx(cur_node, idx_in_parent);
            push_key_into_key_path(&art_path->key_path, &c, 1);
        }
      
        cur_node = next_node;
        pos_in_parent = idx_in_parent; 
   }

   if(kOk == ret)
   {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get key valid path succ, cur_node[%p] key[%p] key_len[%d] snapid[%d].", 
                     cur_node, key, key_len, snapid);
        
        /* search in snap tree */
       ret = memtable_snap_get_valid_path(get_path, (art_snap_node *)cur_node, snapid);
   }
   
   return ret;
}

/*
* description               : batch search in memtable.
* get_path                  : batch get struct, contains artree and path
* parent_node               : root node
* para                      : the param of batch get
* get_callback              : return value to upper
* return                    : kOk or kNotFound
*/
int32_t memtable_batch_get(batch_get_path_t *get_path, art_node *parent_node, artree_batch_get_para_t *para, ARTREE_GET_CALLBACK get_callback)
{
    art_node *cur_node            = NULL;
    int32_t ret                   = kNotFound;
    uint32_t index                = 0;
    uint8_t *key                  = NULL;
    uint8_t snapid[SNAPID_LEN]    = {0};

    art_tree_t *artree            = get_path->artree;
    art_path_t *art_path          = &(get_path->art_path);
    uint32_t number               = para->keys_num;
    uint32_t key_len              = para->key_len;

    uint8_t *value                = NULL;
    uint32_t value_len            = 0;

    uint64_t time                 = 0;
    
    /* reuse path to get multiple keys */
    while(index < number)
    {
       start_comm_ftds(ART_BATCH_GET_MEM_ONE_KEY, &time);
        
       /* get key,key_len,snapid from para */
       key        = para->keys[index];
       ART_MEMCPY(snapid, &para->snapshotid[index], sizeof(uint32_t));

       /* get first node for search */
       cur_node = memtable_get_first_node_for_batch_search(parent_node, get_path, key, key_len, snapid, index);

       /* search key in artree and push node to path */
       ret = memtable_get_valid_path(cur_node, get_path, key, key_len, para->snapshotid[index]);

       if(kOk == ret)
       {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get key succ, artree[%p] key[%p] key_len[%d] snapid[%d] index[%d].", 
                         artree, key, key_len, para->snapshotid[index], index);

            get_value_callback(artree, art_path, para->args, para->instance, index, get_callback);
       }
       else
       {
            //处理异常情况:kIoError(底层read失败)
            if((kNotFound != ret) &&(kArtSnapIdNotFound != ret)) 
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree memtable get fail, read_plog node is null ret[%d] artree[%p] key[%p] key_len[%d] snapid[%d] index[%d].", 
                            ret, artree, key, key_len, para->snapshotid[index], index);
                return ret;
            }
            
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree memtable get key fail, artree[%p] key[%p] key_len[%d] snapid[%d] index[%d] value[%p] value_len[%d] ret[%d].", 
                         artree, key, key_len, para->snapshotid[index], index, value, value_len, ret);
            get_callback(para->instance, para->args, index, value, value_len, KNotExist, NULL, NULL);
       }

       end_comm_ftds(ART_BATCH_GET_MEM_ONE_KEY, (uint32_t)ret, time);
       
       /* search next key */
       index++;
    }

    /* TODO 暂时均返回kOk, 后期将故障场景考虑进来再修改 */
    return kOk;
}

#endif

#if DESC("insert IO interface")

int32_t add_snap_child4_2_new_node(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN int prefix_diff,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    art_node* new_node4     = NULL;
    void*     new_leaf      = NULL;
    int32_t   ret           = kOk;
    art_snap_node* new_node = NULL;
    
    ret = add_new_leaf_2_new_snap_node(t, ref, insert_para, depth, prefix_diff, &new_node4, &new_leaf, arena_alloc_callback, pcls);
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "add new leaf 2 new snap node fail ret = %d.", ret);
        return ret;
    }
    
#ifndef SUPPORT_RW_CONCURRENCY
    // Adjust the prefix of the old node
    //assert(node->partial_len <= SNAPID_LEN);
    add_snap_child4_in_new_node(t, new_node4, (void**)ref, GET_CHAR_FROM_INT(node->partial, prefix_diff), node, arena_alloc_callback, pcls);
    node->partial_len -= (prefix_diff + 1);
    ART_MEMMOVE(node->partial, node->partial + prefix_diff + 1, node->partial_len);
    
    *ref = (art_snap_node*)new_node4;
#else
    // Create a new node
    LVOS_TP_START(ART_TP_INSERT_SNAP_ADD_NEW_INNERNODE_ROW, &new_node)
    LVOS_TP_START(ART_TP_INSERT_SNAP_INNERNODE_ALLOC_MEM, &new_node)
    new_node = (art_snap_node*)alloc_node(t, node->node_type, arena_alloc_callback, pcls); /*lint !e740*/
    LVOS_TP_END
    LVOS_TP_END    
    if (NULL == new_node)
    {
        //malloc fail
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc new node fail.");
        reset_free_node(t, new_node4);
        reset_free_node(t, (art_node*)new_leaf);
        return kOutOfMemory;
    }
    
    copy_node_buff((art_node*)(void*)new_node, (art_node*)(void*)node); /*lint !e740*/ //get new_node same with node
    
    (void)add_snap_child4_in_new_node(new_node4, GET_CHAR_FROM_INT(new_node->partial, prefix_diff), new_node); /*lint !e545*/
    new_node->partial_len -= (uint8_t)(prefix_diff + 1);
    ART_MEMMOVE(new_node->partial, new_node->partial + prefix_diff + 1, new_node->partial_len);
    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, (art_snap_node*)new_node4);
    
    art_mem_reclamation(t, (art_node*)(void*)node);
    
#endif
    atomic64_inc(&art_mem_stat[t->apply_type].insert_leaf_num);

    return kOk;
}

static void* split_snap_leaf(
    IN art_tree_t* t,
    IN void* leaf,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
 {
    art_node* new_node4    = NULL;
    int32_t longest_prefix = 0;
    void*   new_leaf       = NULL;   
    uint8_t old_pos_value  = 0;
    uint8_t new_pos_value  = 0;
    uint32_t old_snapid    = 0;
    uint32_t new_snapid    = 0;
    art_leaf_operations_t  *leaf_ops = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(t)].leaf_ops;

    // New value, we must split the leaf into a snap node4
    LVOS_TP_START(ART_TP_INSERT_SNAP_INNERNODE_ALLOC_MEM, &new_node4)
    new_node4 = alloc_node(t, MEMTABLE_ART_SNAPNODE4, arena_alloc_callback, pcls); /*lint !e740 !e826*/
    LVOS_TP_END
    if (NULL == new_node4)
    {
        //malloc fail
        ART_LOGERROR(ART_LOG_ID_4B00, "saplit snap leaf alloc snap node4 fail.");
        return NULL;
    }

    // Create a new leaf
    LVOS_TP_START(ART_TP_INSERT_SNAP_SPLIT_SNAP_LEAF, &new_leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &new_leaf)
    new_leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, pcls);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == new_leaf)
    {
        //malloc fail
        ART_LOGERROR(ART_LOG_ID_4B00, "saplit snap leaf alloc snap leaf fail.");
        reset_free_node(t, new_node4);
        return NULL;
    }

    // Determine longest prefix
    longest_prefix = longest_common_prefix(t, leaf, new_leaf, depth);
    snap_copy_partial((art_snap_node*)new_node4, insert_para->snapid, (uint32_t)depth, (uint32_t)longest_prefix);
    //old_snapid = GET_LEAF_SNAPID(t, leaf);
    //new_snapid = GET_LEAF_SNAPID(t, new_leaf);
    old_snapid = leaf_ops->get_snap_id(leaf);
    new_snapid = leaf_ops->get_snap_id(new_leaf);
    /*get the value of the different byte*/
    old_pos_value = GET_CHAR_FROM_INT(old_snapid, (SNAPID_LEN - depth - 1 - longest_prefix));
    new_pos_value = GET_CHAR_FROM_INT(new_snapid, (SNAPID_LEN - depth - 1 - longest_prefix));

    //  avoid mommove
    if (old_pos_value < new_pos_value)
    {
        (void)add_snap_child4_in_new_node(new_node4, old_pos_value, leaf);
        (void)add_snap_child4_in_new_node(new_node4, new_pos_value, new_leaf);
    }
    else
    {    
        (void)add_snap_child4_in_new_node(new_node4, new_pos_value, new_leaf);
        (void)add_snap_child4_in_new_node(new_node4, old_pos_value, leaf);
    }

    return (void*)new_node4;
 }
/**
 * repalce a new snap node to a exist leaf
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert successfully, otherwise
 * means failed.
 */
int32_t snap_insert_replace_exist_leaf(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
 {
    art_snap_node* new_node4 = NULL;
    void* new_leaf           = NULL;
    void* leaf               = NULL;    
    
    leaf = (void*)node;

    // Check if we are updating an existing value
    if (snap_leaf_matches(t, leaf, insert_para->snapid))
    {
        /*Write coverage,snapid_num--*/
        t->statistical_data.snapid_num--;
        LVOS_TP_START(ART_TP_INSERT_SNAP_EXIST_LEAF, &new_leaf)
        LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &new_leaf)    
        new_leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, pcls);
        LVOS_TP_END
        LVOS_TP_END
        if (NULL == new_leaf)
        {
            //malloc fail
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap leaf fail when replace exist leaf.");
            return kOutOfMemory;
        }
    
#ifndef SUPPORT_RW_CONCURRENCY
    
        *ref = (art_snap_node*)new_leaf;
        reset_free_node(t, (art_node*)leaf);
#else
        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, (art_snap_node*)new_leaf); /*lint !e740*/
    
#endif
        atomic64_inc(&art_mem_stat[t->apply_type].replace_leaf_num);

        ART_LOGDEBUG(ART_LOG_ID_4B00, "insert a same snapid[%u] node then repalce it.", insert_para->snapid);
        return kOk;
    }
    // New value, we must split the leaf into a snap node4
    new_node4 = (art_snap_node*)split_snap_leaf(t, leaf, ref, insert_para, depth, arena_alloc_callback, pcls);
    if (NULL == new_node4)
    {
        //malloc fail
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node4 fail when replace exist leaf.");
        return kOutOfMemory;
    }

#ifndef SUPPORT_RW_CONCURRENCY
    // Add the leafs to the new node4
    *ref = new_node4;
#else
    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node4);

#endif
    atomic64_inc(&art_mem_stat[t->apply_type].insert_leaf_num);

    return kOk;
 }

/**
 * make a new snap leaf and add it
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert successfully, otherwise
 * means failed.
 */
int32_t make_new_snap_leaf_and_add(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    void* new_leaf      = NULL;
    art_node** tmp_node = NULL;

    LVOS_TP_START(ART_TP_INSERT_SNAP_ADD_NEW_LEAF_ON_EXIST_INNERNODE, &new_leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &new_leaf)
    new_leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, pcls);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == new_leaf)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap leaf fail.");
        return kOutOfMemory;
    }

    LVOS_TP_START(ART_TP_INSERT_SNAP_ADD_NEW_LEAF_EXPAND_EXIST_INNERNODE, &tmp_node)
    tmp_node = add_child(t, (void*)node, (void**)ref, GET_CHAR_FROM_INT(insert_para->snapid, (SNAPID_LEN - depth -1)), new_leaf, arena_alloc_callback, pcls);
    LVOS_TP_END
    if (NULL == tmp_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "add leaf to node[%d] fail.", node->node_type);
        return kOutOfMemory;
    }
    atomic64_inc(&art_mem_stat[t->apply_type].insert_leaf_num);
    return kOk;
}

/**
 * make a new snap node4 and a leaf,then add leaf to node4
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg prefix_diff The depth of insert
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert successfully, otherwise
 * means failed.
 */
int32_t add_new_leaf_2_new_snap_node(
    IN art_tree_t* t,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN int prefix_diff,
    OUT art_node** new_node4,
    OUT void** new_leaf,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    art_node* node4 = NULL;
    void* leaf      = NULL;
    
    // Create a new node
	LVOS_TP_START(ART_TP_INSERT_SNAP_ADD_SNAP_INNER_NODE, &node4)
    LVOS_TP_START(ART_TP_INSERT_SNAP_INNERNODE_ALLOC_MEM, &node4)
    node4 = alloc_node(t, MEMTABLE_ART_SNAPNODE4, arena_alloc_callback, pcls); /*lint !e740 !e826*/
	LVOS_TP_END
    LVOS_TP_END    
    if (NULL == node4)
    {
        //malloc fail
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap node 4 fail.");
        return kOutOfMemory;
    }

    snap_copy_partial((art_snap_node*)node4, insert_para->snapid, (uint32_t)depth, (uint32_t)prefix_diff);
    
	LVOS_TP_START(ART_TP_INSERT_SNAP_ADD_SNAP_LEAF, &leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &leaf)    
    leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, pcls);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == leaf)
    {
        //malloc fail
        reset_free_node(t, (art_node*)(void*)node4);
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc snap leaf fail.");
        return kOutOfMemory;
    }
    (void)add_snap_child4_in_new_node(node4,  
                                      GET_CHAR_FROM_INT(insert_para->snapid,(SNAPID_LEN - depth -1 - prefix_diff)),
                                      leaf);
    *new_node4 = node4;
    *new_leaf  = leaf;
    return kOk;
}

/*填充leaf节点*/
void fill_leaf_node(
    IN art_tree_t* t,
    IN void* leaf,
    IN art_insert_para_t* insert_para)
{
    art_leaf*        v_leaf = NULL;
    art_svalue_leaf* f_leaf = NULL;

    if (IS_LUNMAP_ARTREE(t))
    {
        f_leaf = (art_svalue_leaf*)leaf;
        
        f_leaf->node_type = MEMTABLE_ART_LEAF;
        f_leaf->op_type   = insert_para->type;
        f_leaf->snapid    = insert_para->snapid;
        f_leaf->value_len = (uint8_t)insert_para->value_len;
        ART_MEMCPY(f_leaf->key_value, insert_para->value, insert_para->value_len);

#ifdef ARTREE_DEBUG
        f_leaf->check_rr_flag = RELEASE_STATUS;
#endif
    }
    else
    {
        v_leaf = (art_leaf*)leaf;
        v_leaf->key_len   = 0;
        v_leaf->node_type = MEMTABLE_ART_LEAF;
        v_leaf->op_type   = insert_para->type;
        v_leaf->snapid    = insert_para->snapid;
        v_leaf->value_len = insert_para->value_len;
        ART_MEMCPY(v_leaf->key_value, insert_para->value, insert_para->value_len);

#ifdef ARTREE_DEBUG
        v_leaf->check_rr_flag = RELEASE_STATUS;
#endif
    }
    
    return;
}


/**
 * make a leaf node
 * @arg t The tree
 * @arg insert_para The parameters of operation
 * @arg snapid_len The length of snapid,not used.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg pcls The function arg.
 * @return NULL means alloc failed, otherwise
 * means successfully.
 */
void* make_snap_leaf(    
    IN art_tree_t* t,
    IN art_insert_para_t* insert_para,
    IN int snapid_len,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    uint32_t  epoch_size = 0;
    void*     leaf       = NULL;
    uint32_t  leaf_len   = 0;
    
    
#ifndef SUPPORT_RW_CONCURRENCY
    epoch_size = 0;   
#else
    epoch_size = sizeof(epoch_head_t);
#endif
    leaf_len = epoch_size + g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(t)].leaf_ops.calc_leaf_size(insert_para->value_len, insert_para->key_len);
    leaf     = arena_alloc_callback(pcls, leaf_len);
    if (NULL != leaf)
    {
        ART_MEMSET(leaf, 0, leaf_len);
        leaf = GET_ART_HEAD(leaf, epoch_size); 
        fill_leaf_node(t, leaf, insert_para);
    }
 
    return leaf;
} /*lint !e715*/

void** insert_2_end_child(
    IN art_tree_t* t,
    IN art_node** node,
    IN art_insert_para_t* insert_para,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    void**    child = NULL;
    void*     leaf  = NULL;

    child = memtale_get_end_child(*node);
    if (NULL != *child)
    {
        /*end child is leaf node or snap_inner_node*/
        return child;
    }

    /*直接新增leaf*/
    LVOS_TP_START(ART_TP_INSERT_ENDLEAF_ALLOC_NOT_EMPTY_TREE, &leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &leaf)
    leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, args);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == leaf)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc leaf node fail when insert end child node.");
        return NULL;
    }

    memtable_node_set_end_child(*node, leaf);

    ART_MEMORY_BARRIER();
    (*node)->end_valid = 1;
    child = (void**)node;
    return child;
}

/*
*   insert inner node,for partial_len has max len
*   1)ref is the pasition ptr of parent node
*   2)out_node is the last inner node 
*   3)return tmp is the out_node's position ptr in its parent node
*/
void** insert_inner_node(
    IN art_tree_t* t,
    IN art_node* n,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    IN uint32_t depth,
    IN art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    uint32_t            length      = insert_para->key_len - depth;
    uint32_t            partial_len = 0;
    art_node*           parent_node = n;
    art_node**          tmp         = ref;
    art_node*           first_node  = n;
    void*               leaf        = NULL;
    void**              child       = NULL;
    art_node*           new_node    = NULL;
    
    /*如果剩余要插入的length为0，则直接插入end child*/
    if (0 == length)
    {
        child = insert_2_end_child(t, &parent_node, insert_para, arena_alloc_callback, args);
        *out_node = *child;
        return child;
    }
     
    while (length > 1)
    {
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_EMPTY_TREE_KEY_LONG, &new_node)
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_MEM, &new_node)
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        LVOS_TP_END
        LVOS_TP_END
        if (NULL == new_node)
        {
            //error printf
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node4 fail when insert inner node.");
            return NULL;
        }

        if (NULL != parent_node)
        {
            /*add the new_node to node, then return the new_node*/
            (void)add_child_in_new_node((void*)parent_node, insert_para->key[depth], new_node);
            
            art_update_node_max_snapid(parent_node, insert_para->snapid);
            
            depth++;
            length--;
        }
        else
        {
            first_node = new_node;        
        }

        partial_len = (length > MAX_PREFIX_LEN) ? MAX_PREFIX_LEN : (length - 1);
      
        memtable_new_node_set_partial(new_node, (uint8_t*)(insert_para->key + depth), partial_len);
        
        length -= partial_len;
        depth  += partial_len;

        parent_node = new_node;
    }

    /*直接新增leaf*/
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_EMPTY_TREE_KEY_LONG, &leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &leaf)
    leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, args);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == leaf)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc leaf node fail when insert inner node.");
        return NULL;
    }

    // Add the leafs to the new node
    (void)add_child_in_new_node((void*)parent_node, insert_para->key[insert_para->key_len - 1], leaf);

    art_update_node_max_snapid(parent_node, insert_para->snapid);
    
    if (NULL == n)
    {
        /*root == NULL,in art_insert_create_root*/
        *out_node = first_node;

    }else
    {
        /*in diffrent with inner node process*/
        *out_node = parent_node;
    }

    return (void**)tmp;
}


/**
 * Insert a new snap node to the ART tree
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
int32_t snap_insert(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    art_snap_node* tmp_node     = NULL;
    int prefix_diff             = 0; 
    art_snap_node** child       = NULL;
    int32_t         child_idx   = 0;

    /*
    if (unlikely(NULL == node))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert snap node is null.");
        return kInvalidArgument;
    }*/
	/*snapid_num++*/
	t->statistical_data.snapid_num++;
    tmp_node = node;
    for (;;)
    {        
        // If we are at a leaf, we need to replace it with a node
        if (is_art_leaf_node((void*)tmp_node))
        {
            return snap_insert_replace_exist_leaf(t, tmp_node, ref, insert_para, depth, arena_alloc_callback, pcls);
        }
        // Check if given node has a prefix
        if (tmp_node->partial_len)
        {
            // Determine if the prefixes differ, since we need to split
            prefix_diff = snap_check_prefix(tmp_node, insert_para->snapid, SNAPID_LEN, depth);
            if ((uint8_t)prefix_diff == tmp_node->partial_len)
            {
                depth += tmp_node->partial_len;
                // Find a child to recurse to
                child = memtable_snap_find_child(t, tmp_node, GET_CHAR_FROM_INT(insert_para->snapid, (SNAPID_LEN - depth - 1)), &child_idx);
                if (child)
                {
                    depth++;
                    tmp_node = *child;
                    ref      = child;
                    continue;
                }
                return make_new_snap_leaf_and_add(t, tmp_node, ref, insert_para, depth, arena_alloc_callback, pcls);
            }

            return add_snap_child4_2_new_node(t, tmp_node, ref, insert_para, depth, prefix_diff, arena_alloc_callback, pcls);
        }

        // Find a child to recurse to
        child = memtable_snap_find_child((void*)t, tmp_node, GET_CHAR_FROM_INT(insert_para->snapid, (SNAPID_LEN - depth - 1)), &child_idx);
        if (child)
        {
            depth++;
            tmp_node = *child;
            ref      = child;
            continue;
        }
        return make_new_snap_leaf_and_add(t, tmp_node, ref, insert_para, depth, arena_alloc_callback, pcls);
    }
}

/**
 * create a root when the tree is null
 * @arg t The tree
 * @arg node The node maybe to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg out_node The node to be inserted finally.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
static void** art_insert_create_root(
    IN art_tree_t* t,
    IN art_node* node,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    void*     leaf     = NULL;
    art_node* new_node = NULL;
    void**    ret      = NULL;
    
    /*when the key's length is 1, insert it directly*/
    if (unlikely(insert_para->key_len == 1))
    {
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_EMPTY_TREE_KEY_SHORT, &new_node)
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_MEM, &new_node)    
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        LVOS_TP_END
        LVOS_TP_END
        if (NULL == new_node)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc memtable node4 fail when create root.");
            return NULL;
        }
    
        /*make a snap leaf*/
        LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_EMPTY_TREE_KEY_SHORT, &leaf)
        LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &leaf)
        leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, args);
        LVOS_TP_END
        LVOS_TP_END
        if (NULL == leaf)
        {
            reset_free_node(t, new_node);
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc memtable leaf fail when create root.");
            return NULL;
        }
    
        // Add the leafs to the new node
        (void)add_child_in_new_node((void*)new_node, insert_para->key[0], leaf);

        art_update_node_max_snapid(new_node, insert_para->snapid);
        
#ifndef SUPPORT_RW_CONCURRENCY
        *ref = new_node;
#else
        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node);
#endif

        *out_node = new_node;
        return (void**)ref;
    }
    else
    {
        ret = insert_inner_node(t, node, ref, insert_para, 0, out_node, arena_alloc_callback, args);/*空树插入包含key的中间节点*/
        
        if(NULL == ret)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "insert inner node alloc memory failed, apply_type[%d] key_len[%u] snapid[%u] type[%d].",
                        t->apply_type, insert_para->key_len, insert_para->snapid, insert_para->type);
        }
        else
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "insert a new tree apply_type[%d] key_len[%u] snapid[%u] type[%d] succeed.",
                         t->apply_type, insert_para->key_len, insert_para->snapid, insert_para->type);
        }
#ifndef SUPPORT_RW_CONCURRENCY
        *ref = *out_node;
#else
        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, *out_node);
#endif
        return ret;
    }
}


static int32_t handle_find_child(
    IN art_tree_t* t,
    IN art_node*  node,
    IN art_node** child,
    IN int32_t child_idx,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    IN uint32_t depth,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
 {
    art_node** tmp_child   = NULL;
    
    if (depth == insert_para->key_len)
    {
        /*已经到了叶子或者snap节点*/
        if (!is_art_inner_node(*child))
        {
            *out_node = *child;
            return kOk;
        }
        ref = child;
        return kNotFound;
    }
    else
    {
        /*已经到了叶子或者snap节点*/
        if (!is_art_inner_node(*child))
        {
            tmp_child = (art_node **)art_replace_old_child_2_new_node(t, node, child_idx, ref, *child, insert_para, depth, out_node, arena_alloc_callback, args);
            *out_node = *tmp_child;
            return kOk;
        }
        ref      = child;
        return kNotFound;
    }
 }


/**
 * Insert a child
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg out_node The node to be inserted finally.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
void** art_insert_child(
    IN art_tree_t* t,
    IN art_node* n,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    IN uint32_t depth,
    IN art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    art_node*           new_node     = NULL;
    art_node*           first_node   = NULL;
    uint32_t            length       = insert_para->key_len - depth;
    uint32_t            origin_depth = depth;
    art_node*           node         = n;
    uint32_t            partial_len  = 0;
    art_node**          tmp          = ref;
    void*               leaf         = NULL;

    while (length > 1)
    {
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_NOT_EMPTY_TREE_NO_COMMON_PREFIX, &new_node)
        LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_MEM, &new_node)
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        LVOS_TP_END
        LVOS_TP_END
        if (NULL == new_node)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node fail when insert child node.");
            return NULL;
        }

        if (NULL != first_node)
        {
            (void)add_child_in_new_node((void*)node, insert_para->key[depth], new_node);
            art_update_node_max_snapid(node, insert_para->snapid);
        }
        else
        {
            first_node = new_node;
        }

        depth++;
        length--;

        partial_len = (length > MAX_PREFIX_LEN) ? MAX_PREFIX_LEN : (length - 1);
         
        memtable_new_node_set_partial(new_node, (uint8_t*)(insert_para->key + (depth)), partial_len);
        length -= partial_len;
        depth += partial_len;

        node = new_node;
    }

    /*直接新增leaf*/
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_NOT_EMPTY_TREE_NO_COMMON_PREFIX, &leaf)
    LVOS_TP_START(ART_TP_INSERT_LEAF_ALLOC_MEM, &leaf)
    leaf = make_snap_leaf(t, insert_para, SNAPID_LEN, arena_alloc_callback, args);
    LVOS_TP_END
    LVOS_TP_END
    if (NULL == leaf)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc leaf node fail when insert child node.");
        return NULL;
    }

    // Add the leafs to the new node
    if (NULL == first_node)
    {
        //  directly insert leaf to parent_node
        if (NULL == add_child(t, n, (void**)ref, insert_para->key[insert_para->key_len-1], leaf, arena_alloc_callback, args))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "add child by key[%d] key_len[%u] fail.",
                         insert_para->key[insert_para->key_len-1], insert_para->key_len);
            return NULL;
        }

        *out_node = (art_node*)*ref;// the last node
    }
    else
    {
        //  general inner node, insert leaf to last node
        (void)add_child_in_new_node((void*)node, insert_para->key[insert_para->key_len-1], leaf);
        art_update_node_max_snapid(node, insert_para->snapid);
        //  then insert path to parent_node
        if (NULL == add_child(t, n, (void**)ref, insert_para->key[origin_depth], (void*)first_node, arena_alloc_callback, args))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "add child by key[%d] key_len[%u] fail.",
                         insert_para->key[origin_depth], insert_para->key_len);
            return NULL;
        }

        *out_node = node;// the last node
    }

    /*update the input node, maybe it has replaced with a new node, so use ref here */
    art_update_node_max_snapid(*ref, insert_para->snapid);
    
    return (void**)tmp;
}


/*替换老的节点*/
void** art_replace_old_child_2_new_node(
    IN art_tree_t* t,
    IN art_node* node,
    IN int32_t child_idx,
    IN art_node** ref,
    IN art_node* old_child,
    IN art_insert_para_t* insert_para,
    IN uint32_t depth,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
 {
    art_node* new_node = NULL;
    void**     child   = NULL;
    
    new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
    if (NULL == new_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "alloc node fail when replace old child.");
        return NULL;
    }
    child = art_insert_child(t, new_node, ref, insert_para, depth, out_node, arena_alloc_callback, args);
    if (NULL == child)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert child fail when replace old child.");
        reset_free_node(t, new_node);
        return NULL;
    }

    /*将老的叶子节点插入end*/
    memtable_node_set_end_child(new_node, old_child);
    ART_MEMORY_BARRIER();
    memtable_new_node_set_end_valid(new_node, 1);
    
    /*replace old leaf node with new node*/
    memtable_node_set_child(node, child_idx, new_node);

    return (void**)out_node;
 }


static void** memtable_insert_handle(
    IN art_tree_t* t,
    IN art_node* tmp_node,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    IN uint32_t depth,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args,
    uint32_t prefix_diff)
{
    art_node*           new_node4 = NULL;
    art_node*           new_node  = NULL;
    void**              ret       = NULL;
    uint32_t            snapid    = art_get_node_max_snapid(tmp_node);
    // Create a new node
    LVOS_TP_START(ART_TP_INSERT_NOT_EMPTY_TREE_NO_COMMON_PREFIX_ALLOC_FIRST, &new_node4)
    LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_MEM, &new_node4)
    new_node4 = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
    LVOS_TP_END
    LVOS_TP_END
    if (new_node4 == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert memtable alloc node4 fail.");
        return NULL;
    }

    memtable_new_node_set_partial(new_node4, tmp_node->partial, prefix_diff);
    art_update_node_max_snapid(new_node4, snapid);
    
#ifndef SUPPORT_RW_CONCURRENCY         
// Adjust the prefix of the old node
    add_child_in_new_node((void*)new_node4, tmp_node->partial[prefix_diff], tmp_node);
    tmp_node->partial_len -= (prefix_diff + 1);
    ART_MEMMOVE(tmp_node->partial, tmp_node->partial + prefix_diff + 1, tmp_node->partial_len);

    // Insert the new leaf
    depth += prefix_diff;

    assert((uint32_t)depth < insert_para->key_len);
    
    *ref = new_node4;
    return insert_inner_node(t, new_node4, ref, insert_para, depth, out_node, arena_alloc_callback, args);
#else
    LVOS_TP_START(ART_TP_INSERT_NOT_EMPTY_TREE_NO_COMMON_PREFIX_ALLOC_SECOND, &new_node)
    LVOS_TP_START(ART_TP_INSERT_INNERNODE_ALLOC_MEM, &new_node)
    new_node = (art_node*)alloc_node(t, tmp_node->node_type, arena_alloc_callback, args);
    LVOS_TP_END
    LVOS_TP_END
    if (new_node == NULL)
    {    
        reset_free_node(t, new_node4);
        ART_LOGERROR(ART_LOG_ID_4B00, "insert memtable alloc node[%d] alloc new node fail.", tmp_node->node_type);
        return NULL;
    }

    copy_node_buff(new_node, tmp_node);//get new_node same with node

    (void)add_child_in_new_node((void*)new_node4, new_node->partial[prefix_diff], new_node);

    new_node->partial_len -= (uint8_t)(prefix_diff + 1);
    ART_MEMMOVE(new_node->partial, new_node->partial + prefix_diff + 1, (uint32_t)new_node->partial_len);
    
    depth += prefix_diff;

    ret = insert_inner_node(t, new_node4, ref, insert_para, depth, out_node, arena_alloc_callback, args);

    ART_MEMORY_BARRIER();
    ART_ATOMIC_SET_PTR(ref, new_node4);

    art_mem_reclamation(t, tmp_node);

    return ret;
#endif
}


/**
 * Insert a new value into the ART tree
 * @arg t The tree
 * @arg node The node maybe to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg out_node The node to be inserted finally.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
static void** insert_memtable(
    IN art_tree_t* t,
    IN art_node* node,
    IN art_node** ref,
    IN art_insert_para_t* insert_para,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args,
    OUT bool* last_node_expand)
{
    art_node*  tmp_node    = NULL;
    art_node** child       = NULL;
    int32_t    child_idx   = 0;
    uint32_t   prefix_diff = 0;
    uint32_t   depth       = 0;

    tmp_node = node;
    // If we are at a NULL node, inject a leaf
    if (NULL == tmp_node)
    {
        return art_insert_create_root(t, tmp_node, ref, insert_para, out_node, arena_alloc_callback, args);
    }

    for (;;)
    {
        /*if given node has no prefix*/
        if (0 == tmp_node->partial_len)
        {
            /*当前的depth和插入的key长度一样，则需要插入到end*/
            if(depth == insert_para->key_len)
            {
                child = (art_node **)insert_2_end_child(t, &tmp_node, insert_para, arena_alloc_callback, args);
                *out_node = *child;

                //ART_LOGDEBUG(ART_LOG_ID_4B00, "insert an end child to artree succeed.");
                return (void**)child;
            }
            
            // Find a child to insert
            child = memtable_node_find_child(tmp_node, insert_para->key[depth], &child_idx);
            if (NULL != child)
            {
                *last_node_expand = false;
                depth++;
                if (kOk == handle_find_child(t, tmp_node, child, child_idx, ref, insert_para, depth, out_node, arena_alloc_callback, args))
                {
                    art_update_node_max_snapid(tmp_node, insert_para->snapid);                    
                    return (void**)child;
                }
                art_update_node_max_snapid(tmp_node, insert_para->snapid);
                tmp_node = *child;
                ref = child;
                continue;
            }
            // No child, node goes within us
            return art_insert_child(t, tmp_node, ref, insert_para, depth, out_node, arena_alloc_callback, args);
        }
        // Has  prefixes ,then determine if the prefixes differ, since we need to split
        prefix_diff = (uint32_t)check_prefix(tmp_node, insert_para->key, insert_para->key_len, depth);
        /*支持变长需修改如下
          1)prefix_diff == n->partial_len,depth += n->partial_len,goto查找子节点
                  1.1 depth < key_len:find child
                  1.2 depth = key_len:insert to [0]
          2)其它情况,放入create a new node里面，判断
                  2.1 prefix_diff + depth == key_len,new leaf插入到new node[0]处
                2.2 否则根据字符key[depth]插入到new node
        */

        if(prefix_diff == tmp_node->partial_len)
        {
            depth += tmp_node->partial_len;
            /*当前的depth和插入的key长度一样，则需要插入到end*/
            if(depth == insert_para->key_len)
            {
                child = (art_node **)insert_2_end_child(t, &tmp_node, insert_para, arena_alloc_callback, args);
				if (NULL == child)
				{
					return (void**)child;
				}
                *out_node = *child;
                return (void**)child;
            }
            // Find a child to insert
            child = memtable_node_find_child(tmp_node, insert_para->key[depth], &child_idx);
            if (NULL != child)
            {
                *last_node_expand = false;
                depth++;
                if (kOk == handle_find_child(t, tmp_node, child, child_idx, ref, insert_para, depth, out_node, arena_alloc_callback, args))
                {
                    art_update_node_max_snapid(tmp_node, insert_para->snapid);
                    return (void**)child;
                }
                art_update_node_max_snapid(tmp_node, insert_para->snapid);
                tmp_node = *child;
                ref = child;
                continue;
            }
            // No child, node goes within us
            return art_insert_child(t, tmp_node, ref, insert_para, depth, out_node, arena_alloc_callback, args);
        }

        /*has different prefix, spilt the node*/
        return memtable_insert_handle(t, tmp_node, ref, insert_para, depth, out_node, 
                                       arena_alloc_callback, args, prefix_diff);
    }
    /*should not go here, because depth is init to 0, and the length of key cannot be 0*/
    ART_LOGERROR(ART_LOG_ID_4B00, "insert memtable fail depth=%u, length of key = %u.", depth, insert_para->key_len);
    return NULL;
}

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg snapid snapshot Id
 * @arg type The type of value,see ValueType
 * @arg value Opaque value.
 * @arg value_len The length of the value.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return kOk means insert seccessfully, otherwise
 * insert failed.
 */
int32_t art_insert(
    IN art_tree_t* t,
    IN const uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t snapid,
    IN uint8_t type,
    IN const uint8_t* value,
    IN uint32_t value_len,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    art_node*         node        = NULL;
    void**            ref         = NULL;
    art_insert_para_t insert_para = {0};
    int32_t           ret         = kOk;
    bool              flag        = false;
    insert_para.key       = key;
    insert_para.key_len   = key_len;
    insert_para.value     = value;
    insert_para.value_len = value_len;
    insert_para.snapid    = snapid;
    insert_para.type      = type;

    /*插入包含key的中间节点,返回包含snap子树root的node*/
    ref = insert_memtable(t, (art_node *)(t->root.mem_addr), (art_node **)&(t->root.mem_addr), &insert_para, &node, arena_alloc_callback, args, &flag);
    if (NULL == node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert artree memtable fail key_len[%u] value_len[%u] apply_type[%d] snapid[%u].",
                     key_len, value_len, t->apply_type, snapid);
        return kOutOfMemory;
    }
    
    if (!is_art_inner_node(node))
    {
        /*have same key,need to insert snapId*/
        /*处理情况(2)snap inner node; (3)leaf node*/
        ret = snap_insert(t, (art_snap_node*)(void*)node, (art_snap_node**)ref, &insert_para, 0, arena_alloc_callback, args); /*lint !e740*/

        if(kOk != ret)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "insert artree memtable same key alloc memory fail, key_len[%u] value_len[%u] apply_type[%d] snapid[%u] ret[%d].",
                        key_len, value_len, t->apply_type, snapid, ret);
        }
        else
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "insert artree memtable same key key_len[%u] value_len[%u] apply_type[%d] snapid[%u] ret[%d].",
                         key_len, value_len, t->apply_type, snapid, ret);
        }
        return ret;
    }
    else
    {
        /* new key, key_num++, snapid_num++*/
        t->statistical_data.key_num++;
        t->statistical_data.snapid_num++;
        
        atomic64_inc(&art_mem_stat[t->apply_type].insert_leaf_num);
        
        ART_LOGDEBUG(ART_LOG_ID_4B00, "insert artree memtable new key key_len[%u] value_len[%u] apply_type[%d] snapid[%u] succeed.",
                     key_len, value_len, t->apply_type, snapid);
    }
    UNREFERENCE_PARAM(flag);
    return kOk;
}

#endif

#if DESC("batch insert IO interface")
/*
* insert inner node,for partial_len has max len
* 1)ref is the pasition ptr of parent node
* 2)out_node is the last inner node 
* 3)return tmp is the out_node's position ptr in its parent node
* @arg t The tree
* @arg node The node maybe to be inserted
* @arg ref The reference of the node
* @arg key the key which is inserted
* @arg prefix_len the length of prefix
* @arg depth the depth of key
* @arg out_node The node to be inserted finally.
* @arg arena_alloc_callback The function of alloc memory.
* @arg args The function arg.
* @return NULL means insert failed, otherwise
*/
void** insert_prefix_inner_node(
    IN art_tree_t* t,
    IN art_node* n,
    IN art_node** ref,
    IN art_node** last_node_ref,    
    IN uint8_t* key,
    IN uint32_t prefix_len,
    IN uint32_t snapId,
    IN uint32_t* depth,
    IN art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    uint32_t            length      = prefix_len - *depth;
    uint32_t            partial_len = 0;
    art_node*           parent_node = n;
    art_node*           new_node    = NULL;

    while (length > 0)
    {
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        if (NULL == new_node)
        {
            //error printf
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node4 fail when insert inner node.");
            return NULL;
        }
        
        if (NULL != parent_node)
        {
            /*add the new_node to node, then return the new_node*/
            last_node_ref = add_child_in_new_node((void*)parent_node, key[*depth], new_node);
            art_update_node_max_snapid(parent_node, snapId);
            *ref = parent_node;
            (*depth)++;
            length--;
        }

        partial_len = (length > MAX_PREFIX_LEN) ? MAX_PREFIX_LEN : length;
      
        memtable_new_node_set_partial(new_node, (uint8_t*)(key + *depth), partial_len);
        
        length -= partial_len;
        *depth  += partial_len;

        parent_node = new_node;
    }

    art_update_node_max_snapid(parent_node, snapId);
    
    /*out_node返回待插入节点位置，返回值为接口调用前的node位置*/
	*out_node = parent_node;
    return (void**)(last_node_ref);
}

/**
 * create a root when the tree is null
 * @arg t The tree
 * @arg node The node maybe to be inserted
 * @arg ref The reference of the node
 * @arg key the key which is inserted
 * @arg prefix_len the length of prefix
 * @arg out_node The node to be inserted finally.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 */
void** insert_prefix_create_root(
    IN art_tree_t* t,
    IN art_node* node,
    IN art_node** ref,
    IN art_node** last_node_ref,    
    IN uint8_t* key,
    IN uint32_t prefix_len,
    IN uint32_t snapId,
    OUT art_node** out_node,
    IO uint32_t* depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    art_node* new_node      = NULL;
    void** ret              = NULL;

    /*when the prefix's length is 1, insert it directly*/
    if (unlikely(prefix_len == 1))
    {
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        if (NULL == new_node)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc memtable node4 fail when create root.");
            return NULL;
        }
    
        /*init root node param*/
        memtable_new_node_set_partial(new_node, key, prefix_len);

        art_update_node_max_snapid(new_node, snapId);
        
#ifndef SUPPORT_RW_CONCURRENCY
        *ref = new_node;
#else
        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, new_node);
#endif

        *depth += 1;
        
        *out_node = new_node;
        return (void**)(ref);
    }
    else
    {
        ret = insert_prefix_inner_node(t, node, ref, last_node_ref, key, prefix_len, snapId, depth, out_node, arena_alloc_callback, args);
        if(NULL == ret)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "insert inner node alloc memory failed, apply_type[%d] prefix_len[%u].",
                        t->apply_type, prefix_len);
        }
        else
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "insert a new tree apply_type[%d] prefix_len[%u] succeed.",
                         t->apply_type, prefix_len);
        }
/*
#ifndef SUPPORT_RW_CONCURRENCY
        *ref = *out_node;
#else
        ART_MEMORY_BARRIER();
        ART_ATOMIC_SET_PTR(ref, *out_node);
#endif
*/
        return ret;
    }
}

/**
 * Insert a child by prefix key
 * @arg t The tree
 * @arg node The node to be inserted
 * @arg ref The reference of the node
 * @arg insert_para The parameters of operation
 * @arg depth The depth of insert
 * @arg out_node The node to be inserted finally.
 * @arg arena_alloc_callback The function of alloc memory.
 * @arg args The function arg.
 * @return NULL means insert failed, otherwise
 * means successfully.
 */
void** art_insert_prefix_child(
    IN art_tree_t* t,
    IN art_node* n,
    IN art_node** ref,
    IN art_node** last_node_ref,      
    IN uint8_t* prefix_key,
    IN uint32_t prefix_len,
    IN uint32_t snapId,
    IN uint32_t* depth,
    IN art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    art_node*           new_node     = NULL;
    art_node*           first_node   = NULL;
    uint32_t            length       = prefix_len - *depth;
    uint32_t            origin_depth = *depth;
    art_node*           node         = n;
    uint32_t            partial_len  = 0;
    art_node**          tmp          = last_node_ref;

    while (length > 0)
    {
        new_node = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
        if (NULL == new_node)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "alloc node fail when insert child node.");
            return NULL;
        }

        if (NULL != first_node)
        {
            last_node_ref = add_child_in_new_node((void*)node, prefix_key[*depth], new_node);
            art_update_node_max_snapid(node, snapId);
        }
        else
        {
            first_node = new_node;
        }

        (*depth)++;
        length--;

        partial_len = (length > MAX_PREFIX_LEN) ? MAX_PREFIX_LEN : length;
         
        memtable_new_node_set_partial(new_node, (uint8_t*)(prefix_key + *depth), partial_len);
        length -= partial_len;
        *depth += partial_len;

        node = new_node;
    }

    /* update last node maxsnapid */
    art_update_node_max_snapid(node, snapId);
    
    // Add the new node to the parent
    if (NULL == first_node)
    {
        *out_node = node;// the parent_node of last node
    }
    else
    {
        if(last_node_ref == tmp)
        {
            //  then insert path to parent_node
            last_node_ref = add_child(t, n, (void**)ref, prefix_key[origin_depth], (void*)first_node, arena_alloc_callback, args);
        }
        else if(NULL == add_child(t, n, (void**)ref, prefix_key[origin_depth], (void*)first_node, arena_alloc_callback, args))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "add child by key[%d] key_len[%u] fail.",
                         prefix_key[prefix_len], prefix_len);
            return NULL;
        }

        *out_node = node;// the last node
    }

    art_update_node_max_snapid(*ref, snapId);
    
    return (void**)(last_node_ref);
}


static void** memtable_batch_insert_handle(
    IN art_tree_t* t,
    IN art_node* node,
    IN art_node** ref,
    IN art_node** last_node_ref,       
    IN uint8_t* prefix_key,
    IN uint32_t prefix_len,
    IN uint32_t snapId,
    IN uint32_t* depth,
    OUT art_node** out_node,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args,
    uint32_t prefix_diff)
{
    art_node*           new_node4 = NULL;
    art_node*           new_node  = NULL;
    void**              ret       = NULL;

    // Create a new node
    new_node4 = alloc_node(t, MEMTABLE_ART_NODE4, arena_alloc_callback, args);
    if (NULL == new_node4)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch insert memtable alloc node4 fail.");
        return NULL;
    }

    memtable_new_node_set_partial(new_node4, node->partial, prefix_diff);
    art_update_node_max_snapid(new_node4, snapId);
#ifndef SUPPORT_RW_CONCURRENCY         
// Adjust the prefix of the old node
    last_node_ref = add_child_in_new_node((void*)new_node4, node->partial[prefix_diff], node);
    
    node->partial_len -= (prefix_diff + 1);
    ART_MEMMOVE(node->partial, node->partial + prefix_diff + 1, node->partial_len);

    // Insert the new leaf
    *depth += prefix_diff;

    assert((uint32_t)*depth < prefix_len);
    
    return insert_prefix_inner_node(t, new_node4, ref, last_node_ref, prefix_key, prefix_len, depth, out_node, arena_alloc_callback, args);
#else
    new_node = (art_node*)alloc_node(t, node->node_type, arena_alloc_callback, args);
    if (new_node == NULL)
    {    
        reset_free_node(t, new_node4);
        ART_LOGERROR(ART_LOG_ID_4B00, "insert memtable alloc node[%d] alloc new node fail.", node->node_type);
        return NULL;
    }

    copy_node_buff(new_node, node);//get new_node same with node

    add_child_in_new_node((void*)new_node4, new_node->partial[prefix_diff], new_node);
    
    new_node->partial_len -= (uint8_t)(prefix_diff + 1);
    ART_MEMMOVE(new_node->partial, new_node->partial + prefix_diff + 1, (size_t)new_node->partial_len);
    
    *depth += prefix_diff;

    last_node_ref = ref;
    ret = insert_prefix_inner_node(t, new_node4, ref, last_node_ref, prefix_key, prefix_len, snapId, depth, out_node, arena_alloc_callback, args);

	ART_ATOMIC_SET_PTR(ref, new_node4);
    art_mem_reclamation(t, node);

    return ret;
#endif
}

/*insert the prefix*/
static void** insert_prefix_key(
    IN art_tree_t* t,
    IN art_node* node,
    IN art_node** ref,
    IN art_node** last_node_ref,
    IN artree_batch_insert_para_t* insert_para,
    OUT art_node** out_node,
    IO uint32_t* depth,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* args)
{
    uint8_t*   prefix_key  = insert_para->key[0];
    uint32_t   prefix_len  = insert_para->prefix_len;
    uint32_t   snapId      = insert_para->snapshotid;
    art_node*  cur_node    = NULL;
    art_node** child       = NULL;
    int32_t    child_idx   = 0;
    uint32_t   prefix_diff = 0;
    
    if (NULL == node)
    {
        return insert_prefix_create_root(t, node, ref, last_node_ref, prefix_key, prefix_len, snapId, out_node, depth, arena_alloc_callback, args);
    }

    cur_node = node;
    for(;;)
    {
        if(*depth > prefix_len)
        {
            return (void**)last_node_ref;
        }
                
        // determine if the prefixes differ, since we need to split
        prefix_diff = (uint32_t)check_prefix(cur_node, prefix_key, prefix_len, *depth);
        
        //has the same prefix
        if (prefix_diff == cur_node->partial_len)
        {
			*depth += cur_node->partial_len;
			if (*depth == prefix_len)
			{
				//ref = &cur_node;
				*out_node = cur_node;
				return (void**)last_node_ref;
			}

            child = memtable_node_find_child(cur_node, prefix_key[*depth], &child_idx);
            if (NULL != child)
            {
                //find a child
                (*depth)++;
                cur_node        = *child;
                ref             =  child;
                last_node_ref   = ref;
                *out_node       = *child;

                art_update_node_max_snapid(cur_node, snapId);
                continue;
            }
            // No child, node goes within us
            return art_insert_prefix_child(t, cur_node, ref, last_node_ref, prefix_key, prefix_len, snapId, depth, out_node, arena_alloc_callback, args);
        }

        return memtable_batch_insert_handle(t, cur_node, ref, last_node_ref, prefix_key, prefix_len, snapId, depth, out_node, arena_alloc_callback, args, prefix_diff);
    }
    return NULL;
}

/**
 * batch insert kvs into the ART tree
 * @arg t The tree
 * @arg insert_para the para
 * @return kOk means insert seccessfully, otherwise
 * insert failed.
 */
int32_t batch_insert(
    IN art_tree_t* t,
    IN artree_batch_insert_para_t *batch_insert_para)
{
    art_node*         last_node         = NULL;
    int32_t           ret               = kOk;
    void**            ref               = NULL;
	void**            last_node_ptr     = NULL;
    uint32_t          index             = 0;
    art_insert_para_t insert_para       = {0};
    art_node*         out_node          = NULL;
    uint32_t          depth             = 0;
    bool              last_node_expand  = false;
    art_node**        last_node_ref     = (art_node **)&(t->root.mem_addr);
    uint8_t*          prefix_key        = batch_insert_para->key[0];

    /*1.insert the prefix key and return the node to be inserted*/
	last_node_ptr = insert_prefix_key(t, (art_node *)(t->root.mem_addr), (art_node **)&(t->root.mem_addr), last_node_ref, batch_insert_para, &last_node, &depth, t->ops.alloc, batch_insert_para->arena_args);
    if (NULL == last_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch insert artree memtable fail prefix_len[%u] kv_num[%u] apply_type[%d].",
                     batch_insert_para->prefix_len, batch_insert_para->kv_num, t->apply_type);
        return kOutOfMemory;
    }

    for (index  = 0; index  < batch_insert_para->kv_num; ++index)
    {
        if(1 == memcmp(batch_insert_para->key[index], prefix_key, batch_insert_para->prefix_len))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "batch insert artree memtable param fail.");
            return kInvalidArgument;
        }
        
        insert_para.key       = (batch_insert_para->key[index] + batch_insert_para->prefix_len - last_node->partial_len);
        insert_para.key_len   = batch_insert_para->key_len - batch_insert_para->prefix_len + last_node->partial_len;
        insert_para.value     = batch_insert_para->value[index];
        insert_para.value_len = batch_insert_para->value_len[index];
        insert_para.snapid    = batch_insert_para->snapshotid;
        insert_para.type      = batch_insert_para->type;

        //首节点不扩张
        if(false == last_node_expand)
        {
            last_node_ref   = &last_node;
            last_node_expand = true;
        }
        else
        {
			*last_node_ptr = *last_node_ref;
            last_node = *last_node_ref;
        }
        
        /*插入包含key的中间节点,返回包含snap子树root的node*/
        ref = insert_memtable(t, last_node, last_node_ref, &insert_para, &out_node, t->ops.alloc, batch_insert_para->arena_args, &last_node_expand);
        if (NULL == out_node)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "insert artree memtable fail key_len[%u] value_len[%u] apply_type[%d] snapid[%u].",
                         insert_para.key_len, insert_para.value_len, t->apply_type, insert_para.snapid);
            return kOutOfMemory;
        }
        
        if (!is_art_inner_node(out_node))
        {
            /*have same key,need to insert snapId*/
            /*处理情况(2)snap inner node; (3)leaf node*/
            ret = snap_insert(t, (art_snap_node*)(void*)out_node, (art_snap_node**)ref, &insert_para, 0, t->ops.alloc, batch_insert_para->arena_args); /*lint !e740*/

            if(kOk != ret)
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "insert artree memtable same key alloc memory fail, key_len[%u] value_len[%u] apply_type[%d] snapid[%u] ret[%d].",
                             insert_para.key_len, insert_para.value_len, t->apply_type, insert_para.snapid);
            }
            else
            {
                ART_LOGDEBUG(ART_LOG_ID_4B00, "insert artree memtable same key key_len[%u] value_len[%u] apply_type[%d] snapid[%u] ret[%d].",
                             insert_para.key_len, insert_para.value_len, t->apply_type, insert_para.snapid);
            }
        }
        else
        {
            /* new key, key_num++, snapid_num++*/
            t->statistical_data.key_num++;
            t->statistical_data.snapid_num++;
            
            atomic64_inc(&art_mem_stat[t->apply_type].insert_leaf_num);
            
            ART_LOGDEBUG(ART_LOG_ID_4B00, "insert artree memtable new key key_len[%u] value_len[%u] apply_type[%d] snapid[%u] succeed.",
                         insert_para.key_len, insert_para.value_len, t->apply_type, insert_para.snapid);
        }
    }

	if (true == last_node_expand)
	{
		*last_node_ptr = *last_node_ref;
	}
    
    return ret;
}

#endif

#if DESC("get & seek IO interface")

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
int32_t memtable_io_search_key(
    IN art_tree_t* t,
    IN art_node *parent_node,
    IN const uint8_t* key,
    IN const uint32_t key_len,
    IN uint32_t snapshotid,
    IN uint32_t type,
    OUT art_snap_node** snap_node)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0; 
    uint32_t depth                          = 0;

    cur_node = parent_node;
    if (NULL == cur_node)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable search io failed the parent is null.");
        return kNotFound;
    }
    ART_MEMTABLE_IO_CHECK_ART_INNER_NODE(cur_node, kInvalidArgument);

    while(is_art_inner_node(cur_node))
    {
        /* check snapid range */
        if((GetType == type) && (kPelagoDBSystemTable != t->apply_type))
        {
            result = art_check_snapid_range(snapshotid, cur_node, t);
            if(kOk != result)
            {
                return result;
            }
        }
    
        // get next equal
        result = memtable_search_node_equal(cur_node, (uint8_t*)key, (uint32_t)key_len, depth, &next_node, &idx_in_parent);
        if(kOk != result)
        {
            ART_LOGDEBUG(ART_LOG_ID_4B00, "search key key_len[%u] failed, result [%d], depth[%u] node [%u.%u.%u].", 
                         key_len, result, depth, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }
        
        depth += cur_node->partial_len;
        if(END_POSITION_INDEX != idx_in_parent)
        {
            depth += 1;
        }
        cur_node = next_node;
    }

    if(depth != key_len)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "search key[%u] failed, longest same length is [%u].", key_len, depth);
        return kNotFound;
    }

    *snap_node = (art_snap_node*)cur_node;
    return kOk;
}
#endif

/*
*   get the prev path of snapshotid, means equal or less
*/
int32_t memtable_snap_seek_get_path(
    IN art_snap_iterator* iterator, 
    IN art_snap_node* snap_node, 
    IN artree_seek_para_t *seek_para)
{
    int32_t ret = kOk;

    /* in_param judge */
    if (NULL == snap_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "snap_node is NULL. snapid[%d]", seek_para->snapshotid);
        return kArtSnapIdNotFound;
    }

    /* judge node_type to select different branch */
    switch (snap_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:
            ret = snap_seek_get_path_node4_implement((void*)snap_node, snap_node->num_children - 1, iterator, seek_para);
            if (kArtSnapIdNotFound == ret)
            {
                /* get the prev node with parent_node in path */
                ret = memtable_snap_seek_update_path(iterator, seek_para);
            }
            break;
            
        case MEMTABLE_ART_SNAPNODE16:
            ret = snap_seek_get_path_node16_implement((void*)snap_node, snap_node->num_children - 1, iterator, seek_para);
            if (kArtSnapIdNotFound == ret)
            {
                /* get the prev node with parent_node in path */
                ret = memtable_snap_seek_update_path(iterator, seek_para);
            }
            break;

        case MEMTABLE_ART_SNAPNODE48:
            ret = snap_seek_get_path_node48_implement((void*)snap_node, NODE_256_MAX_CHILDREN_NUM - 1, iterator, seek_para);
            if (kArtSnapIdNotFound == ret)
            {
                /* get the prev node with parent_node in path */
                ret = memtable_snap_seek_update_path(iterator, seek_para);
            }
            break;

        case MEMTABLE_ART_SNAPNODE256:
            ret = snap_seek_get_path_node256_implement((void*)snap_node, NODE_256_MAX_CHILDREN_NUM - 1, iterator, seek_para);
            if (kArtSnapIdNotFound == ret)
            {
                /* get the prev node with parent_node in path */
                ret = memtable_snap_seek_update_path(iterator, seek_para);
            }
            break;

        case MEMTABLE_ART_LEAF:
            /* get leaf succ */
            update_iterator_path(iterator, snap_node);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap_node[%p] id seek get path succ. snapid[%d]", snap_node, seek_para->snapshotid);
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d", snap_node->node_type);               
            ART_ASSERT(0);
            ret = kIOError;
            break;
    }

    return ret;
}


int32_t memtable_snap_seek_update_path(
    IN art_snap_iterator* iterator, 
    IN artree_seek_para_t *seek_para)
{
    art_snap_node* parent_node = NULL;
    art_tree_t* t              = iterator->t;
    int child_idx              = 0;
    
    /*  parent node is null,return kArtSnapIdNotFound */
    if (0 == iterator->path_index)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable snap seek update path path index is 0 snapid[%u].", seek_para->snapshotid);
        return kArtSnapIdNotFound;
    }
    
    /*  get parent node
    *  1.get error path in get_path process:the last node is parent node
    *  2.find the leaf is invalid in snap_seek process:the last node is leaf node
    */
    parent_node = iterator->path[iterator->path_index - 1].node;
    if (is_art_leaf_node((void*)parent_node))
    {
        /*  the last node is leaf node,remove the leaf and again, must not clear the pos_in_parentNode 
        *   snapid_length mean the all inner node in path:partial+key    
        *   so remove leaf no need to update snapid_length
        */
        iterator->path[iterator->path_index - 1].node = NULL;
        iterator->path_index--;     
        //return memtable_snap_seek_update_path(iterator, snapshotid, read_plog_callback, args);
        parent_node = iterator->path[iterator->path_index - 1].node;
    }

    //assert(!is_art_leaf_node((void*)parent_node));
    if(is_art_leaf_node((void*)parent_node))
    {
        ART_ASSERT(0);
        return kIOError;
    }
    
    /*get old child idx*/
    child_idx = iterator->path[iterator->path_index].pos_in_parentNode - 1;

    /*remove old child key*/
    iterator->snapid_length--;
#ifdef _PCLINT_        
    if (unlikely(NULL == parent_node))
    {
        return kArtSnapIdNotFound;
    }
#endif
    /*get prev child node*/
    switch (parent_node->node_type)
    {
        case MEMTABLE_ART_SNAPNODE4:            
            return snap_seek_update_node4(t, iterator, parent_node, child_idx, seek_para);
             
        case MEMTABLE_ART_SNAPNODE16:
            return snap_seek_update_node16(t, iterator, parent_node, child_idx, seek_para);
            
        case MEMTABLE_ART_SNAPNODE48:
            return snap_seek_update_node48(t, iterator, parent_node, child_idx, seek_para);
                        
        case MEMTABLE_ART_SNAPNODE256:
            return snap_seek_update_node256(t, iterator, parent_node, child_idx, seek_para);
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", parent_node->node_type);              
            ART_ASSERT(0);
            return kIOError;
    }

    return kOk; /*lint !e527*/
}

/*
*   use iterator to complete seek()
*   1.use art_iterator_seek_snap to get the path
*   2.use art_iterator_prev to get the
*
*/
int32_t memtable_snap_seek(
    IN art_tree_t* t,
    IN art_snap_node* snap_node,
    IN artree_seek_para_t *seek_para)
{
    int32_t           ret          = kOk;
    void*             leaf         = NULL;
    art_snap_iterator iterator;

    /* special situation:only one node(leaf node) */
    if (is_art_leaf_node(snap_node))
    {
        leaf = (void*)snap_node;
        ret = art_seek_check_and_get_leaf(t, leaf, seek_para);
                 
        return ret;
    }

    init_iterator(&iterator, t);

    /* get path from memtable into iterator */
    ret =  memtable_snap_seek_get_path(&iterator, snap_node, seek_para);
    if (kOk != ret)
    {   
        /* leaf is not find, return the error code */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "the snapid[%d] of key seek fail, ret[%d].", seek_para->snapshotid, ret);
        return ret;
    }
    
    /* loop to get valid leaf */
    ret = snap_seek_get_valid_leaf(&iterator, seek_para);
    
    /* if leaf is not find, return the error code */
    return ret;
}

/*
uint8_t memtable_get_key(
    IN art_node* node,
    IN int key_idx)
{
    if(is_memtable_inner_node(node))
    {
        return memtable_node_get_key(node, key_idx);
    }
    else if(is_memtable_snap_inner_node((art_snap_node*)node))
    {
        return memtable_snap_node_get_key(node, key_idx);
    }
    else
    {
        return kOk;
    }
}*/


#if DESC("memtable get max&min key interface")
int32_t memtable_io_get_min_or_max_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN MEMTABLE_GET_NODE_LR_CHILD_FUNC art_get_lr_child_f,
    IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    uint32_t pos_in_parent                  = 0;
    int32_t result                          = kOk;

    if (start_node == NULL || start_node->node_type == MEMTABLE_ART_LEAF)
    { 
        return kOk; 
    }/*规避remove node4不会移除node，只移除leaf*/
    
    cur_node = start_node;
    ART_MEMTABLE_IO_CHECK_ART_INNER_NODE(cur_node, kInvalidArgument);
    while(is_art_inner_node(cur_node))
    {
        // node must be inner_node
        if (key_buff_overflow(key_buff_len, key_len, cur_node))
        {
            return kInvalidArgument;
        }
    
        ART_MEMCPY(key + *key_len, cur_node->partial, cur_node->partial_len);
        *key_len += cur_node->partial_len;

        result = art_get_lr_child_f(cur_node, &next_node, &pos_in_parent);
        if(kOk != result)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get left/right child failed, result %d, node [%u.%u.%u].", result, cur_node->node_type, cur_node->num_children, cur_node->partial_len);
            return result;
        }

        if(END_POSITION_INDEX != pos_in_parent)
        {
            key[*key_len] = art_get_key_by_idx(cur_node, pos_in_parent);
            (*key_len)++;
        }
        
        cur_node = next_node;
    }

    UNREFERENCE_PARAM(t);
    return kOk;
}

int32_t memtable_io_get_min_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len)
{
    int32_t result                      = kOk;
    result = memtable_io_get_min_or_max_key(t, start_node, memtable_get_node_left_child, key_buff_len, key, key_len);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get min key failed, result %d.", result);
        return result;
    }

    if(key_buff_len < *key_len)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get min key failed, buffer over flow key_buff_len[%u], key_len[%u].", key_buff_len, *key_len);
        return kIOError;
    }

    return kOk;
}


int32_t memtable_io_get_max_key(
    IN art_tree_t* t,
    IN art_node* start_node,
    IN uint32_t key_buff_len,
    IO uint8_t* key,
    IO uint32_t* key_len)
{
    int32_t result                      = kOk;
    
    result = memtable_io_get_min_or_max_key(t, start_node, memtable_get_node_right_child, key_buff_len, key, key_len);
    if(kOk != result)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get max key failed, result %d.", result);
        return result;
    }

    if(key_buff_len < *key_len)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get max key failed, buffer over flow key_buff_len[%u], key_len[%u].", key_buff_len, *key_len);
        return kIOError;
    }

    return kOk;
}

#endif

#ifdef __cplusplus
}
#endif


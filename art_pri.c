#ifndef _SCM_PCLINT_
#ifndef WIN32_DEBUG
#include <sys/time.h>
#include "logging.h"
#endif
#include "art_sstable.h"
#include "art_log.h"
#include "art_mem.h"
#include "art_epoch.h"
#include "art_snapshot_memtable.h"
#include "art_memtable_io.h"
#include "art_sstable_io.h"
#include "art_snap_path_impl.h"
#include "art_key_path_impl.h"
#include "snap_merger.h"
#include "art_path.h"

#endif



//MODULE_ID(PID_ART);
#ifdef __cplusplus
extern "C" {
#endif





/* get root node addr from tree */
int32_t get_root_node(
    IN art_tree_t* t,
    IN void *args,
    OUT void** node)
{
    READ_PLOG_FUNC read_plog_callback = t->ops.read;
    int32_t ret                       = kIOError;

    assert(is_memtable_art(t) || is_sstable_art(t));
    if (is_memtable_art(t))
    {
        *node = t->root.mem_addr;
		return kOk;
    }
    ret = read_plog_callback(args, node, t->root.scm_addr, t->root_size);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get art root node fail! ret=[%d], offset=[%llu], size=[%u]", ret, t->root.scm_addr, t->root_size);
        return ret;
    }
    
    return ret;
}


/**
 * Initializes an ART tree
 * @return 0 on success.
 */
void art_tree_init(
    IN art_tree_t* t,
    IN artree_create_para_t *create_para)
{
    t->root.mem_addr            = NULL;
    t->scm_start_offset         = 0;
    t->scm_start_buff_backup    = NULL;
    t->art_type                 = MEMTABLE_ART_TYPE;
    t->art_crc                  = ART_ROOT_CRC;
    t->art_mem_usage            = 0;    
    t->max_key_len              = 0;    
    t->min_key_len              = MAX_KEY_LEN;
    t->root_size                = 0;

    ART_MEMSET(t->free_node_list, 0, ART_NODE_BUTT * sizeof(free_node));
    epoch_init(&t->epoch_manager);
    ART_MEMSET(&t->write_buffer_manager, 0, sizeof(write_buffer_manager_t));
    ART_MEMSET(&t->statistical_data, 0, sizeof(statistical_data_t));
    t->apply_type = create_para->apply_type;
    ART_MEMCPY((void*)(&t->art_configs), (void*)&create_para->art_configs, sizeof(art_tree_config_t));
    init_artree_operations(t, &create_para->ops);
    t->art_ops = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(t)];
}

void release_free_node(IN art_tree_t* t, IN FREE_MEM_FUNC arena_destroy_callback)
{
    free_node* list_head    = NULL;
    free_node* current_node = NULL;
    free_node* next_node    = NULL;

    for (int type = MEMTABLE_ART_NODE4; type < ART_NODE_BUTT; type++)
    {
        list_head = &t->free_node_list[type];
        current_node = (free_node*)list_head->next;

        while (current_node != NULL)
        {
            next_node = (free_node*)current_node->next;
     
#ifndef SUPPORT_RW_CONCURRENCY
            arena_destroy_callback(current_node);
#else
            arena_destroy_callback(GET_EPOCH_HEAD(current_node, sizeof(epoch_head_t)));
#endif
            current_node = next_node;
        }        
        list_head->next = NULL;
    }

    return;
}

void release_epoch_node(IN art_tree_t* t, IN FREE_MEM_FUNC arena_destroy_callback)
{
    epoch_head_t* current_node               = NULL;
    epoch_head_t* next_node                  = NULL;
    epoch_based_reclamation_t* epoch_manager = &t->epoch_manager;
    epoch_head_t* list_head                  = NULL;

    for (int type = EPOCH_0; type < EPOCH_BUTT; type++)
    {
        list_head = &epoch_manager->epoch_node_list[type];
        current_node = (epoch_head_t*)list_head->next;
        
        while (NULL != current_node)
        {
            next_node = (epoch_head_t*)current_node->next;
            arena_destroy_callback(current_node);
            current_node = next_node;
        }        
        list_head->next = NULL;
    }

    return;
}

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int32_t art_tree_destroy(
    IN art_tree_t* t,
    IN FREE_MEM_FUNC arena_destroy_callback)
{
    destroy_node((art_node*)(t->root.mem_addr), arena_destroy_callback);
    t->root.mem_addr = NULL;

    release_free_node(t, arena_destroy_callback);

#ifdef SUPPORT_RW_CONCURRENCY
    release_epoch_node(t, arena_destroy_callback);
#endif

    return kOk;
}

/*
* description               : get reuse length.
* art_path                  : path which is used to batch get
* key                       : keys which need to get
* key_len                   : length of key, the len of every key is same
* snapshotid                : snapids which need to get
* return                    : the length which is reused
*/
uint32_t get_reuse_length(art_path_t *art_path, uint8_t *key, uint32_t key_len, uint8_t *snapshotid)
{
    uint32_t key_idx    = 0;
    int32_t snapid_idx  = 0;
	uint32_t idx        = 0;

    /* 针对于定长查询，要查询的key_len和path中保存的key_len一致 */
    for(key_idx = 0; (key_idx < key_len) && (key_idx < art_path->key_path.prefix_len); key_idx++)
    {
        if(key[key_idx] != art_path->key_path.prefix[key_idx])
        {
            break;
        }
    }

    if((key_idx < key_len) && (key_idx < art_path->key_path.prefix_len))
    {
        /* 返回key相同部分的长度 */
        return key_idx;
    }
    else
    {
		if (0 == art_path->snap_path.prefix_len)
		{
			return key_idx;
		}

        /* key路径全部相同，比较snap key */
        for(snapid_idx = (int32_t)art_path->snap_path.prefix_len - 1, idx = 0; snapid_idx >= 0; snapid_idx--, idx++)
        {
            /* 高位与低位对比 */
            if(snapshotid[idx] != art_path->snap_path.prefix[snapid_idx])
            {
                break;
            }
        }

        return key_idx + idx;
    }
}

/*
* description               : delete node and key from path.
* get_path                  : batch get struct, contains artree and path
* flag                      : the flag which is used to release node
* return                    : the length of node which is poped
*/
uint32_t pop_node_and_key(batch_get_path_t *get_path, bool flag)
{
    uint32_t pos_in_parent                  = 0;
    void *get_node                          = NULL;
    uint32_t snap_len                       = 0;
    uint32_t pop_len                        = 0;
	void *node								= NULL;
    art_tree_t *artree                      = get_path->artree;
    bool is_sstable                         = is_sstable_art(artree);
    art_path_t *art_path                    = &get_path->art_path;
    artree_ext_operations_t *ext_ops        = &artree->ops;

    if(!check_snap_path_empty(&art_path->snap_path))
    {
        pos_in_parent = get_last_node_child_pos_in_snap_path(&art_path->snap_path);

        /* pop node from snap path */
        node = pop_node_from_snap_path(&art_path->snap_path);        
    }
    else
    {
        pos_in_parent = get_last_node_child_pos_in_key_path(&art_path->key_path);

        /* pop node from path */
        node = pop_node_from_key_path(&art_path->key_path);
    }

    /* if node is in sstable, so need to call get_buffer interface */
	get_node = is_sstable ? (void*)art_get_buff(ext_ops->get_buffer, node): node;
	
	/* count pop length */
	pop_len = is_art_leaf_node(get_node) ? 0: 
	          END_POSITION_INDEX == pos_in_parent ? ((art_node*)get_node)->partial_len : ((art_node*)get_node)->partial_len + 1;
    
    if(true != flag)
    {
        /* if node is in sstable, so need to release node*/
        is_sstable ? art_release_node(ext_ops->release_buffer, node) : 0;
    }
    
    /* pop key from snap/key path */
    snap_len = get_prefix_len_in_snap_path(&art_path->snap_path);
    if(snap_len > 0)
    {
        pop_key_from_snap_path(&art_path->snap_path, pop_len);
    }
    else
    {
        pop_key_from_key_path(&art_path->key_path, pop_len);
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "pop node&key succ, pop_len[%d] release_flag[%d].", pop_len, flag);
    
    return pop_len;
}

/*
* description               : delete node from path.
* get_path                  : batch get struct, contains artree and path
* node                      : root node
* key                       : keys which need to get
* key_len                   : length of key, the len of every key is same
* snapshotid                : snapids which need to get
* return                    : the first node of next get
*/
void* reuse_node_impl(batch_get_path_t *get_path, art_node *node, uint8_t *key, uint32_t key_len, uint8_t *snapid)
{
    uint32_t key_len_reuse                  = 0;
    uint32_t pop_len                        = 0;
    void *return_node                       = NULL;
    uint32_t pos_in_parent                  = 0;
    uint32_t child_pos                      = 0;
    uint32_t parent_idx                     = 0;
    art_path_t *art_path                    = &get_path->art_path;
    uint32_t total_len                      = art_path->key_path.prefix_len + art_path->snap_path.prefix_len;

    /* if current path is empty, so return directly */
    if(check_key_path_empty(&art_path->key_path))
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00,"reuse node impl path is empty, key_path->index[%d].", art_path->key_path.path_index);
        return node;
    }

    ART_ASSERT(is_memtable_art((art_tree_t*)get_path->artree));
    /* get reuse legth */
    key_len_reuse = get_reuse_length(art_path, key, key_len, snapid);
    
    while((total_len > key_len_reuse) && ((art_path->key_path.prefix_len > 1) || (art_path->snap_path.prefix_len > 1)))
    {
        /* if last node in path is root, so only pop and not release */
        if(1 == (art_path->key_path.path_index + art_path->snap_path.path_index))
        {
            /* pop last node from path, but node is not release */
            pop_node_and_key(get_path, true);
            break;
        }
        
        /* pop last node from path */
        pop_len = pop_node_and_key(get_path, false);

        total_len -= pop_len;
    }
    
    /* if path is not null, so pop last node */
    if(!check_snap_path_empty(&art_path->snap_path))
    {
        /* get last node in snap path */
        return_node = get_last_node_in_snap_path(&art_path->snap_path, &pos_in_parent, &child_pos, &parent_idx);
        /* pop last node from path, but node is not release */
         pop_node_and_key(get_path, true);
    }
    else if(!check_key_path_empty(&art_path->key_path))
    {
        /* get last node in key path */
        return_node = get_last_node_in_key_path(&art_path->key_path, &pos_in_parent, &child_pos, &parent_idx);

        /* pop last node from path, but node is not release */
         pop_node_and_key(get_path, true);
    }
    else
    {
        /* all node for path is del */
        return_node = node;
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "reuse node impl reuse path succ, return_node[%p].", return_node);     
    return return_node;
}

/*
* description               : get value and callback.
* artree                    : the artree where get key
* art_path                  : path which is used to batch get
* instance                  : the param of get_callback interface
* index                     : the serial number of key which is get
* get_callback              : return value to upper
*/
void get_value_callback(art_tree_t *artree, art_path_t *art_path, void *args, void *instance, uint32_t index, ARTREE_GET_CALLBACK get_callback)
{
    void *leaf                    = NULL;
    uint32_t value_len            = 0; 
    uint8_t  leaf_op_type         = 0;
	uint8_t *value				  = NULL;
    artree_operations_t *ops      = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(artree)];

    if((art_path->key_path.path_index + art_path->snap_path.path_index) <= 0)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree get value callback art path is empty, path_index[%d] file[%p].", 
                                            (art_path->key_path.path_index + art_path->snap_path.path_index), args);
    }
    
    /* get last node in path */
    leaf = get_buff_from_origin_node((void*)artree, art_path->snap_path.path[art_path->snap_path.path_index - 1].node);
    leaf_op_type = ops->leaf_ops.get_op_type(leaf);

    //kTypeDeletion是否需要去除须跟对外接口人确认  周文
    if(kTypeValue == leaf_op_type || kTypeDeletion == leaf_op_type)
    {
        value      = ops->leaf_ops.get_value(leaf);
        value_len  = ops->leaf_ops.get_value_len(leaf);

        /* return geted value to up level */
        get_callback(instance, args, index, value, value_len, leaf_op_type, NULL, NULL);
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree get value callback succ, leaf_type[%d] value[%p], value_len[%d].", 
                                                                                 leaf_op_type, value, value_len);
        return;
    }
    else
    {
        /* return geted value to up level */
        get_callback(instance, args, index, value, value_len, leaf_op_type, NULL, NULL);
    }
    ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree get value callback fail, leaf_type[%d] value[%p], value_len[%d].", 
                                                                             leaf_op_type, value, value_len);
    return;
}

uint64_t get_run_time(struct timeval* tpend, struct timeval* tpstart)
{
    (void)gettimeofday(tpend, 0);
    return (Second_To_Us*(tpend->tv_sec - tpstart->tv_sec) + (tpend->tv_usec - tpstart->tv_usec));
}

/*check insert param is invalid*/
bool check_insert_param_invalid(void *artree_handler, artree_insert_para_t *insert_para)
{
    if (NULL == artree_handler || NULL == insert_para)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert artree param fail artree_handler[%p] insert_para[%p].",
                     artree_handler, insert_para);
        return true;
    }
    
    if (insert_para->key == NULL || insert_para->key_len == 0 || insert_para->key_len > MAX_KEY_LEN || 
        insert_para->type > KNotExist)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert artree param fail insert_para(key[%p] key_len[%u] value[%p] type[%d]).",
                     insert_para->key, insert_para->key_len, insert_para->value, insert_para->type);
        return true;
    }
    return false;
}

bool check_lunmap_value_len_vaild(art_tree_t* root , uint32_t insert_val_len)
{
    uint32_t val_len = get_value_len_by_apply_type(root->apply_type);
    if(root->art_configs.is_fusion_node_open  && (val_len < insert_val_len))
    { 
#ifndef WIN32_DEBUG
        INDEX_LOG_ERROR_LIMIT("lunmap:insert artree value_len[%d] > MAX_VALUE_LEN_IN_LUNMAP[%u].", 
			insert_val_len, val_len);
#endif
        return false;
    }
    return true;
}

bool check_key_len_vaild(art_tree_t* root , artree_insert_para_t *insert_para)
{
    int32_t apply_type = root->apply_type;
    if(((kPelagoDBLunMap == apply_type) || (kPelagoDBFPMap == apply_type) || (kPelagoDBOPMap == apply_type))
        && (root->max_key_len != 0) && (insert_para->key_len != root->max_key_len))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "key_len invalid.apply_type[%d], max_key_len[%d], insert_key_len:[%d].",
             apply_type, root->max_key_len, insert_para->key_len);
        return false;
    }
    return true;
}

/*
* description:
*     check artree params
*@arg     root                   the artree root node
*@arg     user_key               the key which is found
*@arg     key_len                the length of key which is found
*@arg     value                  the memory address of teh key
*@arg     value_len              the length of value
*@return  true:check ok     false:check failed
*/
bool check_artree_param(
            IN art_tree_t* root,
            IN const uint8_t* user_key,
            IN const uint32_t key_len,
            IN uint8_t** value,
            IN uint32_t* value_len)
{
    if (NULL == root || NULL == user_key || 0 == key_len || NULL == value || NULL == value_len)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "check artree param fail, root[%p] user_key[%p] key_len[%d] value[%p] value_len[%p].",
                     root, user_key, key_len, value, value_len);
        return false;
    }
    /* judge whether this artree is valid */
    //ART_ASSERT(root->art_crc == ART_ROOT_CRC);
    if (root->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "param invalid! art_crc = [%llu]", root->art_crc);
        return false;
    }
    return true;
}

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
int check_prefix(
    IN const art_node* n,
    IN const uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t depth)
{
    int max_cmp = get_min(n->partial_len, (int)(key_len - depth));
    int idx = 0;

    for (; idx < max_cmp; idx++)
    {
        if (n->partial[idx] != key[depth + (uint32_t)idx])
        {
            return idx;
        }
    }

    return idx;
}


/*Get the node size by node type*/
uint32_t get_mem_node_size(IN uint8_t node_type)
{
    uint32_t size = 0;

    if((node_type >= MEMTABLE_ART_NODE4) && (node_type <= MEMTABLE_ART_NODE256))
    {
        size = get_memtable_node_type_size(node_type);
    }
    else if((node_type >= MEMTABLE_ART_SNAPNODE4) && (node_type <= MEMTABLE_ART_SNAPNODE256))
    {
		size = get_memtable_snap_node_type_size(node_type);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", node_type);         
        ART_ASSERT(0);
    }
    
    return size;
}

/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
art_node* alloc_node(
    IN art_tree_t* t,
    IN uint8_t node_type,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls)
{
    art_node* node      = NULL;
    uint32_t size         = get_mem_node_size(node_type);
    uint32_t epoch_size   = sizeof(epoch_head_t);
      
    node = get_free_node(t, node_type);

#ifndef SUPPORT_RW_CONCURRENCY

    if (node == NULL)
    {
        node = (art_node*)arena_alloc_callback(pcls, size);
        
        /*update art_mem_usage*/
        t->art_mem_usage += size;
    }

    if (node != NULL)
    {
        ART_MEMSET(node, 0, size);
        node->node_type = node_type;

#ifdef ARTREE_DEBUG
        node->check_rr_flag = RELEASE_STATUS;
#endif  
    }
    
#else

    if (node != NULL)
    {
        ART_MEMSET(GET_EPOCH_HEAD(node,epoch_size), 0, epoch_size + size);
        node->node_type = node_type;
    }
    else
    {
        node = (art_node*)arena_alloc_callback(pcls, epoch_size + size); /*lint !e826*/
        if (node != NULL)
        {
            ART_MEMSET(node, 0, epoch_size + size);
            node = (art_node*)GET_ART_HEAD(node, epoch_size);
            node->node_type = node_type;

#ifdef ARTREE_DEBUG
            node->check_rr_flag = RELEASE_STATUS;
#endif            
            /*update art_mem_usage*/
            t->art_mem_usage += (epoch_size + size);
        }
    }

#endif


#ifdef ART_DEBUG
    if (unlikely(NULL != node))
    {
        node->node_crc = ART_ROOT_CRC;
    }
#endif    
    return node;
}

void copy_node_buff(IN art_node* dest, IN art_node* src)
{
    uint32_t size = 0;

    if(is_memtable_inner_node(src))
    {
        size = get_memtable_node_type_size(src->node_type);
    }
    else if(is_memtable_snap_inner_node((art_snap_node*)src))
    {
        size = get_memtable_snap_node_type_size(src->node_type);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown memtable node type %d.", src->node_type);          
        ART_ASSERT(0);
    }

    ART_MEMCPY(dest, src, size);

    return;
}


/*get free node from free list*/
art_node* get_free_node(art_tree_t* t, uint8_t node_type)
{
    free_node* list_head = NULL;
    free_node* node      = NULL;

    if(unlikely(node_type >= ART_NODE_BUTT))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "node type is invalid type = %d.", node_type);
        return NULL;
    }
    list_head = &t->free_node_list[node_type];
    node = list_head->next; 

    if (node != NULL)
    {
        list_head->next = node->next;
        list_head->num--;
    }

    return (art_node*)(void*)node;
}

char* get_buff_from_origin_node(
    IN art_tree_t* t,
    IN void* origin_node)
{   
    if (is_memtable_art(t))
    {
        return (char*)origin_node;
    }
    else
    {
        return art_get_buff(t->ops.get_buffer, origin_node);
    }
}

char* art_get_buff(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* origin_buffer)
{
    if (origin_buffer == NULL)
    {
        return NULL;
    }
    
    return get_buffer_addr(origin_buffer);
}

char* art_merge_get_buff(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* ori_buff)
{
    snap_trees_info_t* origin_buffer = (snap_trees_info_t*)ori_buff;

    if (origin_buffer == NULL || origin_buffer->origin_buff == NULL)
    {
        return NULL;
    }
    if (origin_buffer->is_memcpy_leaf)
    {
        return origin_buffer->origin_buff;
    }
    
    return get_buffer_addr(origin_buffer->origin_buff);
}

char* art_merge_get_buff2(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* origin_buffer,
    IN bool is_memcpy_leaf)
{
    if (origin_buffer == NULL)
    {
        return NULL;
    }
    
    if (is_memcpy_leaf)
    {
        return origin_buffer;
    }
    
    return get_buffer_addr(origin_buffer);
}

void art_release_node(IN RELEASE_BUFFER_FUNC release_buffer, IN void *origin_buffer)
{
    release_buffer(origin_buffer);
    return;
}

bool is_art_inner_node(IN art_node* node)
{
#ifndef _PCLINT_
    return (((node->node_type >= MEMTABLE_ART_NODE4) && (node->node_type <= MEMTABLE_ART_NODE256))
        || ((node->node_type >= SSTABLE_ART_NODE4) && (node->node_type <= SSTABLE_ART_NODE256)));
#else
    return ((node->node_type <= MEMTABLE_ART_NODE256)
        || ((node->node_type >= SSTABLE_ART_NODE4) && (node->node_type <= SSTABLE_ART_NODE256)));
#endif
}



void copy_partial_key(
    IN art_node* node,
    OUT uint8_t* key,
    OUT uint32_t* index)
{
    ART_MEMCPY(key + *index, node->partial, (uint32_t)node->partial_len);
    (*index) += node->partial_len;

    return;
}

/*node->partial_len + 1byte key*/
bool key_buff_overflow(IN uint32_t key_buff_len, IN uint32_t* index, IN art_node* node)
{
    return ((((*index) + node->partial_len) + 1) > key_buff_len);
}

/*Add the node to free list*/
void reset_free_node(art_tree_t* t, art_node* node)
{
    //assert(node->node_type < ART_NODE_BUTT);
    free_node* list_head = NULL;
    free_node* tmp_node  = NULL;

    if(node->node_type < ART_NODE_BUTT)
    {
        list_head = &t->free_node_list[node->node_type];
        tmp_node  = (free_node*)(void*)node;

        tmp_node->next = list_head->next;
        list_head->next = tmp_node;

        list_head->num++;
    }
}

int32_t longest_common_prefix(
    IN art_tree_t* t,
    IN void* l1,
    IN void* l2,
    IN int32_t depth)
{
    int32_t  max_cmp                 = SNAPID_LEN - depth;
    int32_t  begin_idx               = max_cmp - 1;
    int32_t  idx                     = 0;
    art_leaf_operations_t  *leaf_ops = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(t)].leaf_ops;
    uint32_t snapid1                 = leaf_ops->get_snap_id(l1);
    uint32_t snapid2                 = leaf_ops->get_snap_id(l2);

    for (;begin_idx >= 0;begin_idx--, idx++)
    {
        if (GET_CHAR_FROM_INT(snapid1, begin_idx) != GET_CHAR_FROM_INT(snapid2, begin_idx))
        {
            return idx;
        }
    }

    return idx;
}



/*get artree check the snapid and get the leaf info, snapid exact match*/
int32_t art_get_check_and_get_leaf(
    IN art_tree_t* t,
    IN void* leaf,
    IN artree_get_para_t *get_para)
{
    uint8_t op_type                  = 0;
    art_leaf_operations_t  *leaf_ops = &t->art_ops->leaf_ops;
	int32_t result                   = kOk;

	//sstable need to check key valid
	if(t->art_type == SSTABLE_ART_TYPE)
	{
		//verify leaf key
		result = check_key_valid(t, leaf, (uint8_t*)get_para->key, get_para->key_len);
		if (kOk != result)
		{
			return result;
		}
	}
     
    op_type = leaf_ops->get_op_type(leaf);
    /* Find the leaf, return the value & value_len in leaf */
    //op table for dus add kTypePromote, kTypePromote is a special but valid type
    if (snap_leaf_matches(t, leaf, get_para->snapshotid) && (kTypeValue == op_type || kTypeDeletion == op_type || kTypePromote == op_type))
    {
        *(get_para->value_len) = (uint32_t)leaf_ops->get_value_len(leaf);
        *(get_para->value)     = leaf_ops->get_value(leaf);

        ART_LOGDEBUG(ART_LOG_ID_4B00, "get artree leaf key key_len[%u] snapid[%u] value_len[%u] op_type[%d] succeed.",
                     get_para->key_len, get_para->snapshotid, *(get_para->value_len), op_type);
        return (kTypeValue == op_type || kTypePromote == op_type) ? kOk : kArtGetIsDeleted;
    }
    
    ART_LOGDEBUG(ART_LOG_ID_4B00, "get artree leaf key key_len[%u] snapid[%u] value_len[%u] op_type[%d] snapid not matched.",
                   get_para->key_len, get_para->snapshotid, *(get_para->value_len), op_type);
    return kArtSnapIdNotFound;
}

/*check the snapid and get the leaf info*/
int32_t art_seek_check_and_get_leaf(
    IN art_tree_t* t,
    IN void* leaf,
    IN artree_seek_para_t *seek_para)
{
     uint8_t  leaf_op_type            = 0;
     uint32_t leaf_snapid             = 0;
     art_leaf_operations_t  *leaf_ops = &t->art_ops->leaf_ops;
	 int32_t result                   = kOk;

	 //sstable need to check key valid
	if(t->art_type == SSTABLE_ART_TYPE)
	{
		//verify leaf key
		result = check_key_valid(t, leaf, (uint8_t*)seek_para->key, seek_para->key_len);
		if (kOk != result)
		{
		 return result;
		}
	}
     
     leaf_op_type = leaf_ops->get_op_type(leaf);
     leaf_snapid  = leaf_ops->get_snap_id(leaf);
        
    if (leaf_snapid <= seek_para->snapshotid && (kTypeValue == leaf_op_type || kTypeDeletion == leaf_op_type|| kTypePromote == leaf_op_type))
    {
        /* Find the leaf, return the value & value_len in leaf */
        *(seek_para->value)        = leaf_ops->get_value(leaf);
        *(seek_para->value_len)    = (uint32_t)(leaf_ops->get_value_len(leaf));
        *(seek_para->snapid_found) = leaf_snapid;

        ART_LOGDEBUG(ART_LOG_ID_4B00, "seek artree leaf key key_len[%u] seek snapid[%u] leaf_snapid[%u] value_len[%u] op_type[%d] succeed.",
                     seek_para->key_len, seek_para->snapshotid, leaf_snapid, *(seek_para->value_len), leaf_op_type);
        return ((kTypeValue == leaf_op_type|| kTypePromote == leaf_op_type) ? kOk : kArtGetIsDeleted);
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "seek artree leaf key key_len[%u] seek snapid[%u] leaf_snapid[%u] value_len[%u] op_type[%d] snapid not matched.",
                   seek_para->key_len, seek_para->snapshotid, leaf_snapid, *(seek_para->value_len), leaf_op_type);
    return kArtSnapIdNotFound;
}


uint8_t art_get_key_by_idx(
    IN art_node* node,
    IN int idx)
{
    uint8_t key  = 0;
    
    if(is_memtable_inner_node(node))
    {
        key = memtable_node_get_key_by_idx(node,idx);
    }
    else if(is_sstable_inner_node(node))
    {
        key = art_sstable_get_key_by_idx(node, idx);
    }
    else if(is_sstable_fusion_node((void*)node))
    {
        key = art_sstable_fusion_node_get_key_by_idx(node, idx);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown inner node type %d.", node->node_type);          
        ART_ASSERT(0);
    }

    return key;
}
int32_t get_art_child_addr_by_index(
    IN art_node* node,
    IN int idx,
    OUT void** child,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args)
{
    int32_t ret = kOk;

    if(is_memtable_inner_node(node))
    {
        *child = memtable_node_get_child_by_child_idx(node,idx);
        if (*child == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "memtable get child is NULL,idx=[%d].", idx);
            ART_ASSERT(0);
            ret = kIOError;
        }
    }
    else if(is_sstable_inner_node(node))
    {
        ret = art_sstable_child_addr_by_index(node, idx, read_plog_callback, args, child);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", node->node_type);  
        ART_ASSERT(0);
        ret = kIOError;
    }
 
    return ret;
}

int32_t get_snap_child_addr_by_index(  
    IN art_snap_node* node,
    IN int idx,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args,
    OUT void** child)
{
    //void* child = NULL;
    int32_t ret = kArtSnapIdNotFound;
    
    if(is_memtable_snap_inner_node(node))
    {
        *child = memtable_get_snap_child_addr_by_index(node, idx);
        if (*child == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "memtable get snap child is NULL,idx=[%d].", idx);
            ART_ASSERT(0);
            return kIOError;
        }
        return kOk;
    }
    else if(is_sstable_snap_inner_node(node))
    {
        ret = sstable_get_snap_child_addr_by_index(node, idx, read_plog_callback, args, child);
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "unknown snap inner node type %d.", node->node_type); 
        ART_ASSERT(0);
        ret = kIOError;
    }

    
    return ret;
}

bool is_memtable_art(IN art_tree_t* t)
{
    return t->art_type == MEMTABLE_ART_TYPE;
}

bool is_sstable_art(IN art_tree_t* t)
{
    return t->art_type == SSTABLE_ART_TYPE;
}

bool check_art_empty(IN art_tree_t* t)
{
    if(is_memtable_art(t))
    {
		return (NULL == t->root.mem_addr);
    }

    return 0 == t->root_size;
}

artree_ext_operations_t* get_artree_ext_ops(art_tree_t* t)
{
    return &t->ops;
}


/*upate max_snapid of the node*/
void art_update_node_max_snapid(art_node* node, uint32_t snapid)
{
    if (art_get_node_max_snapid(node) >= snapid)
    {
        return;
    }
    
    art_set_node_max_snapid(node, snapid);
}

/*upate max_snapid of the node*/
void art_set_node_max_snapid(art_node* node, uint32_t snapid)
{
    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            ((memtable_art_node4*)node)->max_snapid = snapid;
            break;

        case MEMTABLE_ART_NODE16:
            ((memtable_art_node16*)node)->max_snapid = snapid;
            break;

        case MEMTABLE_ART_NODE48:
            ((memtable_art_node48*)node)->max_snapid = snapid;
            break;

        case MEMTABLE_ART_NODE256:
            ((memtable_art_node256*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_NODE4:
            ((sstable_art_node4*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_NODE16:
            ((sstable_art_node16*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_NODE48:
            ((sstable_art_node48*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_NODE256:
            ((sstable_art_node256*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_FUSION_NODE1:
            ((art_sstable_fusion_node1*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_FUSION_NODE4:
            ((art_sstable_fusion_node4*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_FUSION_NODE16:
            ((art_sstable_fusion_node16*)node)->max_snapid = snapid;
            break;

        case SSTABLE_ART_FUSION_NODE256:
            ((art_sstable_fusion_node256*)node)->max_snapid = snapid;
            break;
        
        case MERGE_ART_PATH_NODE256:
            ((art_path_node256*)node)->max_snapid = snapid;
            break;

        case MERGE_ART_SNAP_PATH_NODE256:
            ((art_snap_path_node256*)node)->max_snapid = snapid;
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "set max snapid this node type %d has no max_snapid.", node->node_type);                
            ART_ASSERT(0);
            break;
    }
}


/*get max_snapid of the node*/
uint32_t art_get_node_max_snapid(art_node* node)
{
    uint32_t snapid = 0;
    
    switch (node->node_type)
    {
        case MEMTABLE_ART_NODE4:
            snapid = ((memtable_art_node4*)node)->max_snapid;
            break;

        case MEMTABLE_ART_NODE16:
            snapid = ((memtable_art_node16*)node)->max_snapid;
            break;

        case MEMTABLE_ART_NODE48:
            snapid = ((memtable_art_node48*)node)->max_snapid;
            break;

        case MEMTABLE_ART_NODE256:
            snapid = ((memtable_art_node256*)node)->max_snapid;
            break;

        case SSTABLE_ART_NODE4:
            snapid = ((sstable_art_node4*)node)->max_snapid;
            break;

        case SSTABLE_ART_NODE16:
            snapid = ((sstable_art_node16*)node)->max_snapid;
            break;

        case SSTABLE_ART_NODE48:
            snapid = ((sstable_art_node48*)node)->max_snapid;
            break;

        case SSTABLE_ART_NODE256:
            snapid = ((sstable_art_node256*)node)->max_snapid;
            break;

        case SSTABLE_ART_FUSION_NODE1:
            snapid = ((art_sstable_fusion_node1*)node)->max_snapid;
            break;

        case SSTABLE_ART_FUSION_NODE4:
            snapid = ((art_sstable_fusion_node4*)node)->max_snapid;
            break;

        case SSTABLE_ART_FUSION_NODE16:
            snapid = ((art_sstable_fusion_node16*)node)->max_snapid;
            break;

        case SSTABLE_ART_FUSION_NODE256:
            snapid = ((art_sstable_fusion_node256*)node)->max_snapid;
            break;
        
        case MERGE_ART_PATH_NODE256:
            snapid = ((art_path_node256*)node)->max_snapid;
            break;

        case MERGE_ART_SNAP_PATH_NODE256:
            snapid = ((art_snap_path_node256*)node)->max_snapid;
            break;
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "get max snapid this node type %d has no max_snapid.", node->node_type);                
            ART_ASSERT(0);
            break;
    }

    return snapid;
}

/*check snapid range*/
int32_t art_check_snapid_range(uint32_t found_snapid, art_node* node, art_tree_t* tree)
{
    int32_t ret         = kOk;
    uint32_t maxSnapid  = 0;
    
    maxSnapid = art_get_node_max_snapid(node);

    /* check snapid range */
    if(found_snapid > maxSnapid)
    {
#ifdef ARTREE_DEBUG
        tree->optim_number++;
#endif
        ART_LOGDEBUG(ART_LOG_ID_4B00, "art check snapid range foundSnapid[%u] is more than maxSnapid[%u].",
                     found_snapid, maxSnapid);
        return kArtSnapIdNotFound;
    }

    return ret;
}


//get verify key and key_len
void art_sstable_get_verify_key_and_len(IN art_tree_t* t, IN uint8_t* key, IN uint32_t key_len, OUT uint8_t** out_key, OUT uint32_t* out_key_len)
{
    if(TRUE == GET_ARTREE_FUSION_NODE_OPEN_STATE(t))
    {
        *out_key     = key + (key_len - ARTREE_FUISON_LEAF_VERIFY_LEN);
        *out_key_len = ARTREE_FUISON_LEAF_VERIFY_LEN; 
    }
    else
    {
        *out_key     = key;
        *out_key_len = key_len;
    }

    return;
}

/*check leaf valild*/
int32_t check_key_valid(art_tree_t* t, void* node, uint8_t * key, uint32_t key_len)
{
    uint8_t* verify_key     = NULL;
    uint32_t verify_key_len = 0;
    int32_t  ret            = kOk;
    
    if (NULL == node || NULL == key)
    {
        return kIOError;
    }

    ART_ASSERT(key_len >= ARTREE_FUISON_LEAF_VERIFY_LEN);
    
    //fusion node
    if (is_sstable_fusion_node(node))
    {
        ret = sstable_fusion_node_verify_key(t, node, key, key_len);
        return ret;
    }
    else if (is_art_leaf_node(node))
    {
        //verify leaf key
        if(GET_ARTREE_FUSION_NODE_OPEN_STATE(t))
        {
            verify_key     = key + (key_len - ARTREE_FUISON_LEAF_VERIFY_LEN);
            verify_key_len = ARTREE_FUISON_LEAF_VERIFY_LEN; 
        }
        else
        {
            verify_key     = key;
            verify_key_len = key_len;
        }

        if(true != t->art_ops->leaf_ops.check_key_valid(node, verify_key, verify_key_len))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "[VERIFY]apply type[%d] get artree check leaf key invalid leaf key[%p] key_len[%u] verify_key[%p] verify_key_len[%u].",
                         t->apply_type, t->art_ops->leaf_ops.get_leaf_key(node), t->art_ops->leaf_ops.get_leaf_key_len(node), verify_key, verify_key_len);
            return kArtVerifyKeyInvalid;
        }
    }
    else
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "node type invalid.", *((uint8_t*)node));
        return kIOError;
    }

    return kOk;
}

#ifdef __cplusplus
}
#endif


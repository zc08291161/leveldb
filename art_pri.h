#ifndef ART_PRIVATE_H_
#define ART_PRIVATE_H_

#include "art_node_type.h"
#include "art_define.h"
#include "errcode.h"
#include "art_leaf.h"

#ifdef WIN32_DEBUG
#include <time.h>
#include <windows.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct timeval timeval_t;

#ifdef WIN32_DEBUG
#ifndef BITS_PER_BYTE
#define BITS_PER_BYTE (8)
#endif
#endif

//#define BITS_PER_BYTE (8)

// search equal or greator in node
typedef enum
{
    ART_COMPARE_RESULT_EQUAL            = 0,
    ART_COMPARE_RESULT_GREATER          = 1,
    ART_COMPARE_RESULT_LESS             = 2,
    ART_COMPARE_RESULT_ERR              = 3, // unexpected error
}ART_COMPARE_RESULT;


typedef enum
{
    EPOCH_0 = 0,
    EPOCH_1,    
    EPOCH_2,
    EPOCH_BUTT
} EPOCH_TYPE;


typedef struct
{
    void                    *next;
} epoch_head_t;


typedef struct
{
    epoch_head_t            epoch_node_list[EPOCH_BUTT];
    EPOCH_TYPE              local_epoches[N_THREADS];
    EPOCH_TYPE              globla_epoch;
    int                     g_try_free_count;
    int                     total_num;
} epoch_based_reclamation_t;


// struct for the art node reuse
typedef struct
{
    void                    *next;
    int                     num;
} free_node;


//call back function for iter
typedef int(*art_callback)(void* data, void* usrkey, uint32_t usrkey_len, const uint32_t snapid, uint32_t snapid_len, void* value, uint32_t value_len);

/*the memtable and sstable both use ART structrue
*MEMTABLE_ART means the art is memtable,
*SSTABLE_ART means the art is sstable
*/
typedef enum
{
    MEMTABLE_ART_TYPE = 0,
    SSTABLE_ART_TYPE,
    ART_TYPE_BUTT
} ART_TYPE;


/*
*child addr,use union structure
*memtable use mem_addr,sstable use scm_addr
*/
typedef union art_child
{
    uint64_t                    scm_addr;    
    void                        *mem_addr;
} art_child_t; /*scm地址8字节，跟内存指针共用空间*/


/**
 * This struct type is included as part
 * of all the art various node sizes
 */
typedef struct
{
    uint8_t                     node_type;/*必须放在头部*/

#ifdef ARTREE_DEBUG
    uint8_t                     check_rr_flag;// use for check read() and release() flags
#endif
    
    uint8_t                     partial_len: 7; /*lint !e46*/
    uint8_t                     end_valid: 1; /*lint !e46*/
    uint16_t                    num_children;
    uint8_t                     partial[MAX_PREFIX_LEN];
    
#ifdef ART_DEBUG
    uint64_t                    node_crc;
#endif
} art_node;


/**
 * This structtype is included as part
 * of all the art_snap various node sizes
 */
typedef struct
{
    uint8_t                     node_type;/*必须放在头部*/
    
#ifdef ARTREE_DEBUG
    uint8_t                     check_rr_flag;// use for check read() and release() flags
#endif

    uint8_t                     partial_len;
    uint16_t                    num_children;
    uint8_t                     partial[SNAPID_LEN];
} art_snap_node;



typedef struct
{
    void                        *group_write_buffer;
    uint64_t                    group_write_buffer_offset;
} write_buffer_manager_t;


typedef struct
{
    uint64_t                    total_size;    /* total size of the tree*/
    uint64_t                    inner_size;    /* total size of the inner node*/
    uint64_t                    leaf_size;     /* total size of the leaf node*/

    uint64_t                    max_snapid_num_with_key; // TODO:
    uint64_t                    average_snapid_num_with_key; // TODO:

    uint64_t                    key_num;       /* num of the usekey */
    uint64_t                    snapid_num;    /* num of the snapid:leaf_num*/

    uint64_t                    max_tree_depth;
    uint64_t                    averrage_tree_depth;
    uint64_t                    total_tree_depth; /*averrage_tree_deep = total_tree_deep / key_num*/

    uint64_t                    last_level_node_num[ART_NODE_BUTT];
    uint64_t                    last_level_child_num_in_node[ART_NODE_BUTT];

    uint64_t                    node_num[ART_NODE_BUTT];
    uint64_t                    node_size[ART_NODE_BUTT];
    uint64_t                    child_num_in_node[ART_NODE_BUTT]; /* calculat to occupancy rate of inner node*/

    uint64_t                    merge_tree_num;        /* merge:num of the merge trees, the tree all key discard tree num also need +1 */
    uint64_t                    merge_tree_leaf_num;   /* merge:leaf num of the merge trees*/

    uint64_t                    merge_cost_time;  /*merge used time, uint:us */
    uint32_t                    max_key_len;      /*merge max key len*/    
    uint32_t                    min_key_len;      /*merge min key len*/

    uint64_t                    read_plog_num;
    uint64_t                    read_plog_time_per_compaction;
    uint64_t                    write_plog_num;
    uint64_t                    write_plog_time_per_compaction;
    uint64_t                    yeild_num;
    uint64_t                    yeild_time;

    uint64_t                    buff_full_wait_time;
    uint64_t                    buff_full_wait_num;

    uint64_t                    async_flush_wait_time;
    uint64_t                    async_flush_wait_num;
    //stats for compatcion callback

    uint64_t                    arbitrate_num;
    uint64_t                    arbitrate_time_per_compaction;

    uint64_t                    gc_record_num;
    uint64_t                    gc_record_time_pef_compaction;
    uint64_t                    promote_num;
    uint64_t                    promote_time_pef_compaction;
    uint64_t                    migration_num;
    uint64_t                    migration_time_per_compaction;

    //for dus
    uint64_t                    promote_remove_num;
    uint64_t                    promote_remove_leaf_num;
} statistical_data_t;


/* artree external operation set */
typedef struct
{
    ALLOC_MEM_FUNC              alloc;  //dpmm内存申请
    FREE_MEM_FUNC               free;   //dpmm内存释放

    READ_PLOG_FUNC              read;   //从P层获取节点并缓存在mem中
    DIRECT_READ_PLOG_FUNC       direct_read;//直接从P层读取数据，不经过rocache
    BATCH_READ_PLOG_FUNC        batch_read; //批量从P层获取
    GET_BUFFER_FUNC             get_buffer; //盘上地址到内存地址的转换
    RELEASE_BUFFER_FUNC         release_buffer; //内存释放，和get_buffer配合使用

    BATCH_WRITE_TABLE_ASYNC     write;  //向P层写数据

    RECORD_VALID_SIZE_FUNC      record_valid_size;      //gc场景使用
    HBF_ADD_KEY_FUNC            hbf_add_key;    //向hbf中添加key信息
 
    GC_RECORD_FUNC              gc_record;
    
    ARBITRATE_FUNC              arbitrate; 

    COMPUTE_SHARDING_GROUP_FUNC compute_sharding_group; //计算key所在的sharding_group
    PROMOTER_FUNC               promoter_func;  //向指纹表promoter

    SHOULD_STOP                 should_stop;    //故障场景，主动停止compaction过程

    GET_SHARD_ID_CPT_KEY_LEN_FUNC get_cond_len;     //shard迁移场景中，计算shard部分key长度
    
    //GET_SHARD_ID_BY_KEY_FUNC      get_shardid_by_key;
    NEED_MIGRATION_CALC      need_migration_calc;// 计算哪些key需要迁移
    MIGRATED_KEY_CALC            migrated_key_calc;// 计算哪些key需要commit
    MIGRATION_EXTRACTED_KEY      migration_extracted_key;//迁移时 记录key value
    //art merge qos callback
    ART_MERGE_YIELD_FUNC         art_merge_yield;

    // lun delete
    GET_LUN_DELETE_KEY_LEN_FUNC  get_lun_delete_key_len;
    NEED_LUN_DELETE_CALC         need_lun_delete_calc; // judge this key location lun need delete

    SERIAL_PROGRESS_FUNC        serial_report;      // report the serialize progress
    COMPACT_PROGRESS_FUNC       compact_report;     // report the compaction progress

}artree_ext_operations_t;

struct artree_operations_tag;
struct art_tree_tag;
typedef struct art_tree_tag art_tree_t;
typedef struct artree_operations_tag artree_operations_t;

/* artree internal operation set */
struct artree_operations_tag
{
    uint32_t                    (*calc_sstable_node_actual_size)(art_tree_t *artree, art_node *node);
    art_leaf_operations_t       leaf_ops;
};

/**
 * artree disk struct, points to root.
 */
struct art_tree_tag
{
    uint64_t                    art_crc;
    int32_t                     apply_type; // lunmap, fingerprint map, system table, etc.
    
    uint64_t                    scm_start_offset;
    void                        *scm_start_buff_backup;
    uint8_t                     art_type; // memtable or sstable

    art_child_t                 root;
    uint32_t                    root_size;
    
    uint32_t                      art_mem_usage;
    uint32_t                    max_key_len;
    uint32_t                    min_key_len;

    //uint8_t                     key[MAX_KEY_LEN];
    //uint32_t                    key_len;

    write_buffer_manager_t      write_buffer_manager;

    free_node                   free_node_list[ART_NODE_BUTT];
    
    epoch_based_reclamation_t   epoch_manager;

    statistical_data_t          statistical_data;
    
    art_tree_config_t           art_configs;
    artree_ext_operations_t     ops; // external operation set
    artree_operations_t         *art_ops; // internal artree operation set

#ifdef ARTREE_DEBUG
    uint64_t                    optim_number; //getDelta optim number                    
#endif
};

/**
 * artree mem struct, points to root.
 */
 // TODO:
typedef struct
{
    uint64_t                    art_crc;
    int32_t                     apply_type; // lunmap, fingerprint map, system table, etc.
    uint32_t                    value_len; // valid when artree with fix length of value. lunmap, fingerprint map, etc.
    
    uint64_t                    scm_start_offset;
    //uint64_t                    scm_start_offset_backup;
    void                        *scm_start_buff_backup;
    uint8_t                     art_type; // memtable or sstable

    art_child_t                 root;
    uint32_t                    root_size;
    
    uint32_t                      art_mem_usage;
    uint32_t                    max_key_len;

    uint8_t                     key[MAX_KEY_LEN];
    uint32_t                    key_len;

    write_buffer_manager_t      write_buffer_manager;

    free_node                   free_node_list[ART_NODE_BUTT];
    
    epoch_based_reclamation_t   epoch_manager;

    statistical_data_t          statistical_data;
} art_tree_mem_t;


/* callback func input args */
typedef struct
{
    void                        **source_files; // args for read plog
    void                        *write_args; // args for write plog
    void                        *sys_table; // system table instance
    void                        *dbimpl; // db instance
    void                        *hbf;  // bloom filter instance
}artree_ext_opt_args_t;


typedef struct
{
    const uint8_t               *key;
    uint32_t                    key_len;
    uint32_t                    snapid;
    uint8_t                     type;
    const uint8_t               *value;
    uint32_t                    value_len;
} art_insert_para_t;


typedef struct
{
    /* rocache batch read param */
    uint64_t                    offset[ART_BATCH_READ_NUMBER];
    uint32_t                    size[ART_BATCH_READ_NUMBER];
    void*                       node[ART_BATCH_READ_NUMBER];
    uint32_t                    depth[ART_BATCH_READ_NUMBER];
    uint32_t                    snap_depth[ART_BATCH_READ_NUMBER];
    uint32_t                    index;          //batch get node number
    uint8_t                     bitmap[(ART_BATCH_READ_NUMBER+BITS_PER_BYTE-1)/BITS_PER_BYTE];
    uint32_t                    complete_key_num;
}art_batch_read_para_t;

/* internal operation set */
extern artree_operations_t g_artree_operations[KEY_VALUE_TYPE_BUTT];

/*
 * TODO:
 * This is a temperary solution.
 *
 * In the final implementation, ART invokes DB's union callback interface and 
 * passes the callback_type, and art_workspace.
 *
 * As the callback refector has not been finished at DB side, we put this proxy here.
 * When we finished to refactor the callback at DB side, this proxy will be replaced
 */
void callback_proxy(artree_compaction_para_t *param, enum CallbackType callback_type, void * art_workspace, statistical_data_t* stat);

#if DESC("artree interface")

bool is_memtable_art(IN art_tree_t* t);

bool is_sstable_art(IN art_tree_t* t);

bool check_art_empty(IN art_tree_t* t);

artree_ext_operations_t* get_artree_ext_ops(art_tree_t* t);

int32_t get_root_node(
    IN art_tree_t* t,
    IN void *args,
    OUT void** node);

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
void art_tree_init(
    IN art_tree_t* t,
    IN artree_create_para_t *create_para);

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int32_t art_tree_destroy(
    IN art_tree_t* t,
    IN FREE_MEM_FUNC arena_destroy_callback);
#endif




#if DESC("add interface declaration")
/**
 * Allocates a node of the given type,
 * initializes to zero and sets the type.
 */
art_node* alloc_node(
    IN art_tree_t* t,
    IN uint8_t node_type,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls);

void copy_node_buff(IN art_node* dest, IN art_node* src);

bool check_insert_param_invalid(void *artree_handler, artree_insert_para_t *insert_para);

bool check_lunmap_value_len_vaild(art_tree_t* root , uint32_t insert_val_len);

bool check_key_len_vaild(art_tree_t* root , artree_insert_para_t *insert_para);
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
            IN uint32_t* value_len);

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
    IN void* pcls);
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
    IN void* pcls);

int32_t add_snap_child4_2_new_node(
    IN art_tree_t* t,
    IN art_snap_node* node,
    IN art_snap_node** ref,
    IN art_insert_para_t* insert_para,
    IN int depth,
    IN int prefix_diff,
    IN ALLOC_MEM_FUNC arena_alloc_callback,
    IN void* pcls);

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
    IN void* args);

#endif    
/*get free node from free list*/
art_node* get_free_node(art_tree_t* t, uint8_t node_type);
uint64_t get_run_time(struct timeval* tpend, struct timeval* tpstart);

#define ART_START_TIME_POINT(tpstart)   \
    (void)gettimeofday(tpstart,0);      \


#define ART_RUNTIME(tpend, tpstart)     \
    get_run_time(tpend, tpstart);       \
    ART_MEMSET(tpstart, 0, sizeof(struct timeval)); \
    

char* get_buff_from_origin_node(
    IN art_tree_t* t,
    IN void* origin_node);

char* art_get_buff(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* origin_buffer);

char* art_merge_get_buff(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* ori_buff);

char* art_merge_get_buff2(
    IN GET_BUFFER_FUNC get_buffer_addr,
    IN void* origin_buffer,
    IN bool is_memcpy_leaf);

void art_release_node(IN RELEASE_BUFFER_FUNC release_buffer, IN void *buffer);

bool is_art_inner_node(IN art_node* node);





uint8_t art_get_key_by_idx(
    IN art_node* node,
    IN int idx);

int32_t get_art_child_addr_by_index(
    IN art_node* node,
    IN int idx,
    OUT void** child,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args);

int32_t get_snap_child_addr_by_index(
	IN art_snap_node* node,
	IN int idx,
	IN READ_PLOG_FUNC read_plog_callback,
	IN void *args,
	OUT void** child);

/**
 * Returns the number of prefix characters shared between
 * the key and node.
 */
int check_prefix(
    IN const art_node* n,
    IN const uint8_t* key,
    IN uint32_t key_len,
    IN uint32_t depth);

void copy_partial_key(
    IN art_node* node,
    OUT uint8_t* key,
    OUT uint32_t* index);

// Simple inlined if
static inline int get_min(IN int a, IN int b)
{
    return (a < b) ? a: b;
}

/*node->partial_len + 1byte key*/
bool key_buff_overflow(IN uint32_t key_buff_len, IN uint32_t* index, IN art_node* node);

void reset_free_node(art_tree_t* t, art_node* node);




int32_t longest_common_prefix(
    IN art_tree_t* t,
    IN void* l1,
    IN void* l2,
    IN int32_t depth);


#if DESC("get value interface")

/*get artree check the snapid and get the leaf info, snapid exact match*/
int32_t art_get_check_and_get_leaf(
    IN art_tree_t* t,
    IN void* leaf,
    IN artree_get_para_t *get_para);
    
/*seek artree check the snapid and get the leaf info, snapid fuzzy match*/
int32_t art_seek_check_and_get_leaf(
    IN art_tree_t* t,
    IN void* leaf,
    IN artree_seek_para_t *seek_para);

/*update max_snapid of the node*/
void art_update_node_max_snapid(art_node* node, uint32_t snapid);

/*set max_snapid of the node*/
void art_set_node_max_snapid(art_node* node, uint32_t snapid);

/*get max_snapid of the node*/
uint32_t art_get_node_max_snapid(art_node* node);

/*rollback the max_snapid*/
void art_rollback_node_max_snapid(art_node* node);

/*check snapid range*/
int32_t art_check_snapid_range(uint32_t found_snapid, art_node* node, art_tree_t* tree);

/*get verify key and key_len*/
void art_sstable_get_verify_key_and_len(IN art_tree_t* t, IN uint8_t* key, IN uint32_t key_len, OUT uint8_t** out_key, OUT uint32_t* out_key_len);

/*check leaf valild*/
int32_t check_key_valid(art_tree_t* t, void* node, uint8_t* key, uint32_t key_len);

#endif

#ifdef __cplusplus
}
#endif
#endif


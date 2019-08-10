#ifndef ART_MEM_H_
#define ART_MEM_H_

#include "dpmm_errno.h"
#include "dpmm.h"
#ifdef WIN32_DEBUG
#include "type.h"
#else
#include <stdint.h>
#include <stddef.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define ART_MAX_MEMORY_NAME             (32)

typedef enum
{
    //rebuild artree art_tree_t
    REBUILD_ARTREE = 0,

    //iterator art_iterator
    ART_ITERATOR,

    //serialize serialize_flush_paths_t
    SERIALIZE_FLUSH_PATH,
    //serialize buffer manager
    SERIALIZE_FLUSH_BUFF,
    //serialize write buff for flush
    SERIALIZE_WRITE_BUFF,
    //serialize serialize_path_node_t
    SERIALIZE_PATH_NODE,
    // serialize fusion node256
    SERIALIZE_NODE_STORE,
    // serialize art_tree_t
    SERIALIZE_ARTREE,

    //merge art_merge_memcpy_leaf just for lun map db
    MERGE_MEMCPY_LEAF,
    //merge art_path_node256
    MERGE_PATH_NODE256,
    //merge target_path
    MERGE_TARGET_PATH,
    //merge merge_buffer_manager_t
    MERGE_BUFF_MANGAGER,
    //merge group_write_buffer
    MERGE_GROUP_WRITE_BUFF,
    //merge target_snap_path
    MERGE_TARTGET_SNAP_PATH,
    //merge art_snap_path_node256
    MERGE_SNAP_PATH_NODE256,
    //merge origin_path
    MERGE_ORIGIN_PATH,
    //merge origin_snap_path
    MERGE_ORIGIN_SNAP_PATH,
    //merge art_tree_t
    MERGE_ARTREE,
    //merge tokens Just for fp 
    MERGE_TOKENS,
    //merge token_lens just for fp
    MERGE_TOKEN_LENS,

    // for adapter max tree num 1024
    MERGE_MIN_KEY_IDEX,
    MERGE_SNAP_TREES_INFO,
    //batchget artree
    BATCH_GET_ARTREE,
    BATCH_GET_ARTREE_PATH,
    
    //for flush
    ART_FLUSH_CONTEXT,

    //for iterator opt
    ART_ITER_READ_BUFF,
    
    ART_MEMORY_END  //加入新的类型请加入到该变量之前
} ART_MEMORY_TYPE_E;

/*
* description:
*    init dpmm
*/
int32_t art_dpmm_init(uint32_t apply_type, uint32_t inst_num, uint32_t key_len, uint32_t val_len);

/*
* description:
*    exit dpmm
*/
void art_dpmm_exit(int32_t apply_type);

/*动态申请内存*/
void* art_alloc_structure(int32_t apply_type, int32_t type, uint32_t count, uint32_t size, uint32_t level);

/*动态申请内存without memset*/
void* art_alloc_structure_non_memset(int32_t apply_type, int32_t type, uint32_t count, uint32_t size, uint32_t level);

/*释放内存*/
void art_free_structure(int32_t apply_type, int32_t type, void* obj);

uint32_t get_value_len_by_apply_type(int32_t apply_type);

uint32_t get_key_len_by_apply_type(int32_t apply_type);

int32_t check_is_fusion_node_open(uint32_t apply_type);

#ifdef ARTREE_DEBUG_MEM
void art_detect_mem(void);
#endif
#ifdef __cplusplus
}
#endif
#endif


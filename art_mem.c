#ifndef _SCM_PCLINT_
#include "art_iterator.h"
#include "art_serialize.h"
#include "snap_merger.h"
#include "art_mem.h"
#include "art_log.h"
#include "securec.h"
#endif
#ifndef WIN32_DEBUG
#include "dpax_atomic_redef.h"
#include <infrastructure/lwt/dplwt_api.h>
#endif
#ifdef __cplusplus
extern "C" {
#endif

#define ARTREE_SLAB_MAX_TIMES       (100)
#define ARTREE_THREAD_CACHE_MIN_NUM (4096)

#define MEM_NORMAL_RETRY_NUM 1
#define MEM_SERIALIZE_RETRY_NUM 2
#define MEM_COMPACTION_L0_RETRY_NUM 3
#define MEM_COMPACTION_L1_RETRY_NUM 4
#define MEM_COMPACTION_L2_RETRY_NUM 5

#define ARTREE_THREAD_CACHE_SLAB_MAX_SIZE (4096)
/* create */
#define CREATE_ART_SLAB(dpmm_slab_name, obj_size, min_num, max_num, dpmm_slab_id, need_open_local_cache)\
do \
{\
	int32_t ret = 0; \
	dpmm_slab_param_t dpmm_slab_param = {{0}}; \
	ret = dpmm_slab_param_init(&dpmm_slab_param); \
	if(DPMM_ERR_SUCCESS != ret) \
	{\
		dpmm_slab_id = DPMM_SLAB_ID_INVALID; \
		break;\
	}\
	dpmm_slab_param.module_id = MY_PID; \
	dpmm_slab_param.min_number = min_num; \
	dpmm_slab_param.max_number = max_num * ARTREE_SLAB_MAX_TIMES; \
	dpmm_slab_param.object_size = obj_size; \
	dpmm_slab_param.attribute = DPMM_ATTR_MAGIC; \
    if (obj_size <= ARTREE_THREAD_CACHE_SLAB_MAX_SIZE || need_open_local_cache) \
    {\
        dpmm_slab_param.attribute |= DPMM_ATTR_THREAD_CACHE; \
    }\
	snprintf_s(dpmm_slab_param.name, sizeof(dpmm_slab_param.name), sizeof(dpmm_slab_param.name), "%s", dpmm_slab_name); \
	/* 创建dpmm slab */ \
	dpmm_slab_id = dpmm_create_slab(&dpmm_slab_param, MY_PID, __FUNCTION__); \
}while(0)

/* alloc */
#define ART_SLAB_OBJECT_ALLOC(dpmm_slab_id) dpmm_slab_malloc(dpmm_slab_id, MY_PID, __FUNCTION__)

/*free */
#define ART_SLAB_OBJECT_FREE(dpmm_slab_id, object) dpmm_slab_free(dpmm_slab_id, object, MY_PID, __FUNCTION__)

/* destroy */
#define ART_DESTROY_SLAB(dpmm_slab_id) dpmm_destory_slab(dpmm_slab_id, MY_PID, __FUNCTION__)

/*the config of artree memory*/
typedef struct
{
    char           name[DPMM_SLAB_NAME_LEN];
    uint32_t       art_obj_num;
    uint32_t         art_obj_size;
}art_mem_conf;

#define PELAGODB_BUSSINESS_BUTT (kPelagoDBBusinessTypeButt)
/*manager the dpmm slab id*/
dpmm_slab_id_t art_dpmm_slab_id[PELAGODB_BUSSINESS_BUTT][ART_MEMORY_END] = {{0}};
#ifdef ARTREE_DEBUG_MEM
/*detect the memory alloc and free, just for debug*/
atomic64_t art_mem_detect[PELAGODB_BUSSINESS_BUTT][ART_MEMORY_END];
#endif

/*db slab has been destroy*/
bool db_slab_is_destory[PELAGODB_BUSSINESS_BUTT];

/*db name*/
char* art_db_name [kPelagoDBBusinessTypeButt] = 
{
    "PD_",
    "FP_",
    "OP_",
    "ST_",
    "FI_",
    "DT_"
};

#define ART_SLAB_PREFIX_LEN  (3)
/*manage the info of the db*/
typedef struct 
{
    uint32_t apply_type;
    uint32_t inst_num;
    uint32_t key_len;
    uint32_t val_len;
}art_db_mgr_info_t;
    
art_db_mgr_info_t art_db_mgr_info[PELAGODB_BUSSINESS_BUTT];

/*iterator slab config for db*/
typedef struct
{
    uint32_t level_num;
    uint32_t tree_num_per_level;
}art_iter_slab_config_t;

art_iter_slab_config_t art_iter_slab_config[PELAGODB_BUSSINESS_BUTT] = 
{
    //kPelagoDB 
    {1, 5},
        
    //kDusFP
    {1, 5},
        
    //kDusOP
    {0, 0},
        
    //kSystemTable
    {3, 16},
        
    //kDusFPInUse
    {3, 16},
        
    //kDeltaTable
    {0, 0},
};

art_mem_conf art_conf[ART_MEMORY_END] = 
{
    /*rebuild artree*/
    {"rebuild art_tree",         L0_MAX_NUM + L1_MAX_NUM + L2_MAX_NUM,                             sizeof(art_tree_t)},

    /*iterator*/
    {"art_iterator",             MAX_CONCURRENCY_PER_LEVEL * ART_MAX_LEVEL + MAX_MEMTABLE_ART_NUM, sizeof(art_iterator)},

    /*serialize*/
    {"serialize_flush_path",     MAX_ART_SERIALZIE * 3,                                            sizeof(serialize_flush_path_t)},
    {"serialize_flush_buff",     MAX_ART_SERIALZIE * 3,                                            sizeof(flush_buff_t)},
    {"serialize_write_buff",     MAX_ART_SERIALZIE * 3,                                            SERIALIZE_BUFF_SIZE},
    {"serialize_path_node",      MAX_ART_SERIALZIE,                                                sizeof(serialize_path_node_t)},
    {"node_store",               MAX_ART_SERIALZIE,                                                sizeof(art_sstable_fusion_node256)},
    {"serialize art_tree",       MAX_ART_SERIALZIE,                                                sizeof(art_tree_t)},

    {"merge_memcpy_leaf",        ART_MAX_LEVEL,                                                    sizeof(art_leaf)},
    {"path_node256",             ART_MAX_LEVEL,                                                    sizeof(art_path_node256)},
    {"target_path",              ART_MAX_LEVEL * 3,                                                sizeof(target_path)},
    {"merge_buffer_manager",     ART_MAX_LEVEL * 2,                                                sizeof(merge_buffer_manager_t)},
    {"group_write_buffer",       ART_MAX_LEVEL * 2,                                                MERGE_BUFF_SIZE},
    {"target_snap_path",         ART_MAX_LEVEL * 3,                                                sizeof(target_snap_path)},
    {"snap_path_node256",        ART_MAX_LEVEL,                                                    sizeof(art_snap_path_node256)},
    {"origin_path",              ART_MAX_LEVEL,                                                    sizeof(origin_path)},
    {"origin_snap_path",         ART_MAX_LEVEL,                                                    sizeof(origin_snap_path)},
    {"merge art_tree",           ART_MAX_LEVEL * 3,                                                sizeof(art_tree_t)},
    {"merge tokens",             ART_MAX_LEVEL,                                                    sizeof(char*)},
    {"merge token_lens",         ART_MAX_LEVEL,                                                    sizeof(uint32_t)},
    {"merge_min_key_idex",       ART_MAX_LEVEL*2,                                                  sizeof(uint32_t)*MAX_MERGED_TREE_NUM},
    {"merge_snap_trees_info",    ART_MAX_LEVEL,                                                    sizeof(snap_trees_info_t)*MAX_MERGED_TREE_NUM},

    /*batch get artree*/
    {"batch get artree",         MAX_ART_BATCH_GET,                                                sizeof(art_batch_read_para_t)},
    {"batch get artree path",    MAX_ART_BATCH_GET,                                                sizeof(batch_get_path_t)},    

    //for flush
    {"flush art context",        ART_MAX_LEVEL,                                                    sizeof(art_flush_ctx) },

    //for iterator opt
    {"iterator opt",             ART_MAX_LEVEL,                                                    ARTREE_ITER_READ_BUFF_SIZE + sizeof(iter_buff_node_t) },
};
/*
size and num of slabs are dependent on db_type;
obj_num_min is reservation when initial of modual
obj_num_max is max num needed on worst condition, is 8 times of obj_num_min;
*/
static void art_get_slab_info(
    uint32_t apply_type,
    uint32_t mem_type,
    uint32_t inst_num,
    uint32_t key_len,
    uint32_t val_len,
    bool is_fusion_node_open,
    OUT uint32_t *slab_size,
    OUT uint32_t *obj_num_min,
    OUT uint32_t *obj_num_max,
	OUT bool* need_open_local_cache)
{
    art_mem_conf* conf      = NULL;

    conf           = &art_conf[mem_type];
    *obj_num_min   = conf->art_obj_num * inst_num;
    *obj_num_max   = conf->art_obj_num * inst_num * 8;
    *slab_size     = conf->art_obj_size;
    *need_open_local_cache = false;
    switch(mem_type)
    {
        case SERIALIZE_PATH_NODE:
            //reserve path node mem needed to serialize
            *slab_size = conf->art_obj_size * (key_len + SNAPID_LEN) * MAX_CHILDREN_NUM;
            break;

        case SERIALIZE_NODE_STORE:
            //slab size is dependent on whether fusion_node feature is open
            //obj_num_min reserve all temporary mem needed to serialize a art_tree for each type of database 
            *slab_size = get_max_sstable_node_size(val_len, is_fusion_node_open);
            *obj_num_min = SERIALIZE_PATH_NODE_NUM_MIN(key_len);
            *obj_num_max = SERIALIZE_PATH_NODE_NUM_MAX(key_len) * inst_num * 8;
	        *need_open_local_cache = true;
            break;

        case MERGE_MEMCPY_LEAF:
            //obj_num_min reserve temporary leaf node mem needed to merge a art_tree for each type of database
            *obj_num_min  = MAX_SHARD_NUM * (MAX_CHILDREN_NUM + 1) * MAX_CHILDREN_NUM;
            *obj_num_max  = MAX_SHARD_NUM * (MAX_CHILDREN_NUM + 1) * MAX_CHILDREN_NUM * inst_num * 8;
            *slab_size = conf->art_obj_size + val_len + key_len;
            break;

        case MERGE_PATH_NODE256:
            //obj_num_min reserve temporary inner node mem needed to merge a art_tree for each type of database
            *obj_num_min = (key_len - 1) * MERGE_AVG_CHILD_NUM + MERGE_MAX_CHILD_NUM;
            *obj_num_max =  key_len  * MAX_CHILDREN_NUM * inst_num * 8;
            *need_open_local_cache = true;
            break;

        case MERGE_SNAP_PATH_NODE256:
            //obj_num_min reserve temporary snap inner node mem needed to merge a art_tree for each type of database
            *obj_num_min = (MAX_SNAP_NODE_NUM + FLUSH_BUFFER_NUM_THRESHOLD);
            *obj_num_max = (MAX_SNAP_NODE_NUM + FLUSH_BUFFER_NUM_THRESHOLD)* inst_num * 8;
			*need_open_local_cache = true;
            break;

        case MERGE_ORIGIN_PATH:
        case MERGE_ORIGIN_SNAP_PATH:
            *slab_size = MAX_MERGED_TREE_NUM * conf->art_obj_size;
            break;

        case MERGE_TOKENS:
        case MERGE_TOKEN_LENS:
            *slab_size = MAX_SNAP_TREE_NUM * conf->art_obj_size;
            *need_open_local_cache = true;
            break;
        case MERGE_MIN_KEY_IDEX:
            *need_open_local_cache = true;
            break;

        case ART_FLUSH_CONTEXT:
            *obj_num_min = (MERGE_BUFF_SIZE / ARTREE_TRIGGER_FLUSH_THRESHOLD);
            *obj_num_max = (MERGE_BUFF_SIZE / ARTREE_TRIGGER_FLUSH_THRESHOLD)* inst_num * 8;
            break;
        case ART_ITER_READ_BUFF:
            *obj_num_min = (key_len + SNAPID_LEN + ARTREE_ITER_BUFF_RESERVED_LEN) * art_iter_slab_config[apply_type].level_num * art_iter_slab_config[apply_type].tree_num_per_level * inst_num;
            *obj_num_max = *obj_num_min * 8;
        default:
            break;
    }
}

int32_t check_is_fusion_node_open(uint32_t apply_type)
{
    if(apply_type == 0)  //only Pelagodb open fusion node switch up to now
    {
        return true;
    }
    else
    {
        return false;
    }
        
}
static void art_create_slab(uint32_t apply_type, uint32_t inst_num, uint32_t key_len, uint32_t val_len)
{
    int32_t       i                   = 0;
    uint32_t      slab_size           = 0;
    uint32_t      obj_num_min         = 0;
    uint32_t      obj_num_max         = 0;
    bool          is_fusion_node_open = 0;
	bool        need_open_local_cache = false;
#ifndef WIN32_DEBUG
    char          slab_name[DPMM_SLAB_NAME_LEN];
#endif

    /* adjust slab size according to apply type*/
    is_fusion_node_open = check_is_fusion_node_open(apply_type);
    for (i = 0; i < ART_MEMORY_END; ++i)
    {
        if (MERGE_TOKENS == i || MERGE_TOKEN_LENS == i)
        {
            if (!(kPelagoDBFPMap == apply_type || kPelagoDBOPMap == apply_type))
            {
                continue;
            }
        }

        if (ART_ITER_READ_BUFF == i)
        {
            // optable and deltatable need't iterator
            if ((kPelagoDBOPMap == apply_type) || (kPelagoDBBusinessOther == apply_type))
            {
                continue;
            }
        }
        art_get_slab_info(apply_type, i, inst_num, key_len, val_len, is_fusion_node_open, &slab_size, &obj_num_min, &obj_num_max, &need_open_local_cache);

#ifndef WIN32_DEBUG
        ART_MEMCPY(slab_name, art_db_name[apply_type], ART_SLAB_PREFIX_LEN);
        ART_MEMCPY(slab_name + ART_SLAB_PREFIX_LEN, art_conf[i].name, strlen(art_conf[i].name) + 1);
        CREATE_ART_SLAB(slab_name, slab_size, obj_num_min, obj_num_max, art_dpmm_slab_id[apply_type][i], need_open_local_cache);
#endif
    }
}
/*
* description:
*    init dpmm
*/
int32_t art_dpmm_init(uint32_t apply_type, uint32_t inst_num, uint32_t key_len, uint32_t val_len)
{
    if (apply_type >= PELAGODB_BUSSINESS_BUTT || 0 == inst_num || 0 == key_len || 0 == val_len)
    {
        return kInvalidArgument;
    }
    if (kPelagoDBSystemTable != apply_type)
    {
        art_create_slab(apply_type, inst_num, key_len, val_len);
    }
#ifdef ARTREE_DEBUG_MEM
    for (int32_t i = 0; i < ART_MEMORY_END; ++i)
    {
        atomic64_set(&art_mem_detect[apply_type][i], 0);
    }
#endif

    db_slab_is_destory[apply_type]         = false;
    art_db_mgr_info[apply_type].apply_type = apply_type;
    art_db_mgr_info[apply_type].key_len    = key_len;
    art_db_mgr_info[apply_type].val_len    = val_len;
    art_db_mgr_info[apply_type].inst_num   = inst_num;
    return kOk;
}

/*
* description:
*    exit dpmm
*/
void art_dpmm_exit(int32_t apply_type)
{
    int32_t i = 0;
    if (apply_type >= PELAGODB_BUSSINESS_BUTT)
    {
        return;
    }
    if (kPelagoDBSystemTable != apply_type)
    {
        for (i = 0; i < ART_MEMORY_END; ++i)
        {
#ifdef ARTREE_DEBUG_MEM
        atomic64_set(&art_mem_detect[apply_type][i], 0);
#endif
            if (MERGE_TOKENS == i || MERGE_TOKEN_LENS == i)
            {
                if (!(kPelagoDBFPMap == apply_type || kPelagoDBOPMap == apply_type))
                {
                    continue;
                }
            }
            if (DPMM_SLAB_ID_INVALID == art_dpmm_slab_id[apply_type][i])
            {
                return;
            }
#ifndef WIN32_DEBUG
            ART_DESTROY_SLAB(art_dpmm_slab_id[apply_type][i]);
            art_dpmm_slab_id[apply_type][i] = DPMM_SLAB_ID_INVALID;
#endif
        }
    }
    db_slab_is_destory[apply_type] = true;
    ART_MEMSET(&art_db_mgr_info[apply_type], 0, sizeof(art_db_mgr_info_t));
    
#ifdef ARTREE_DEBUG_MEM
    for (int32_t i = 0; i < ART_MEMORY_END; ++i)
    {
        atomic64_set(&art_mem_detect[apply_type][i], 0);
    }
#endif
    for(i = kPelagoDBLunMap; i < PELAGODB_BUSSINESS_BUTT; i++)
    {
        if(!db_slab_is_destory[i])
        {
            break;
        }
    }
#ifndef WIN32_DEBUG
    if (i == PELAGODB_BUSSINESS_BUTT)
    {
        dpmm_exit();
    }
#endif
    return;
}

//通过level设置重试次数
uint32_t get_retry_number(uint32_t level)
{
    switch(level)
    {
        case Level0ToLevel1:
            return MEM_COMPACTION_L0_RETRY_NUM;
        case Level1ToLevel2:
            return MEM_COMPACTION_L1_RETRY_NUM;
        case Level2ToLevel2:
            return MEM_COMPACTION_L2_RETRY_NUM;
        case FlushLevelType_Normal:
            return MEM_NORMAL_RETRY_NUM;
        case FlushLevelType_BUTT:
            return MEM_SERIALIZE_RETRY_NUM;
        default:
            return MEM_NORMAL_RETRY_NUM;
    }
}

/*动态申请内存*/
void* art_alloc_structure(int32_t apply_type, int32_t type, uint32_t count, uint32_t size, uint32_t level)
{
    void* mem_ptr = NULL;
    uint32_t retry_num = 0;
    
#ifndef WIN32_DEBUG
    retry_num = get_retry_number(level);
    while(retry_num--)
    {
        if (kPelagoDBSystemTable == apply_type)
        {
            mem_ptr = dpmm_malloc(count * size, MY_PID, __FUNCTION__);
        }
        else
        {
            if (DPMM_SLAB_ID_INVALID == art_dpmm_slab_id[apply_type][type])
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "alloc struct failed slab id is invalid apply_type=[%d], type=[%u]", apply_type, type);
                return NULL;
            }
            mem_ptr = ART_SLAB_OBJECT_ALLOC(art_dpmm_slab_id[apply_type][type]);
        }
        //ART_ASSERT(NULL != mem_ptr);
        if(NULL == mem_ptr)
        {
            DPLWT_SLEEP(1);
            continue;
        }
        
        ART_MEMSET(mem_ptr, 0, count * size);
		break;
    }
#else
    mem_ptr = calloc(count, size);
#endif
#ifdef ARTREE_DEBUG_MEM
    atomic64_inc(&art_mem_detect[apply_type][type]);
#endif
    return mem_ptr;
}

/*动态申请内存without memset*/
void* art_alloc_structure_non_memset(int32_t apply_type, int32_t type, uint32_t count, uint32_t size, uint32_t level)
{
    void* mem_ptr       = NULL;
    uint32_t retry_num  = 0;
    
#ifndef WIN32_DEBUG
    retry_num = get_retry_number(level);
    while(retry_num--)
    {
        if (kSystemTable == apply_type)
        {
            mem_ptr = dpmm_malloc(count * size, MY_PID, __FUNCTION__);
        }
        else
        {
            if (DPMM_SLAB_ID_INVALID == art_dpmm_slab_id[apply_type][type])
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "alloc struct failed slab id is invalid apply_type=[%d], type=[%u]", apply_type, type);
                return NULL;
            }
            mem_ptr = ART_SLAB_OBJECT_ALLOC(art_dpmm_slab_id[apply_type][type]);
        }
        //ART_ASSERT(NULL != mem_ptr);
        if(NULL == mem_ptr)
        {
            DPLWT_SLEEP(1);
            continue;
        }
        
        break;
    }
#else
    mem_ptr = calloc(count, size);
#endif
#ifdef ARTREE_DEBUG_MEM
    atomic64_inc(&art_mem_detect[apply_type][type]);
#endif
    return mem_ptr;
}


/*释放内存*/
void art_free_structure(int32_t apply_type, int32_t type, void* obj)
{
#ifndef WIN32_DEBUG
    if (kPelagoDBSystemTable == apply_type)
    {
        dpmm_free(obj, MY_PID, __FUNCTION__);
    }
    else
    {
        if (DPMM_SLAB_ID_INVALID == art_dpmm_slab_id[apply_type][type])
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "free struct failed slab id is invalid apply_type=[%d], type=[%u]", apply_type, type);
            return;
        }
        ART_SLAB_OBJECT_FREE(art_dpmm_slab_id[apply_type][type], obj);
    }
#else
    free(obj);
#endif
#ifdef ARTREE_DEBUG_MEM
    atomic64_dec(&art_mem_detect[apply_type][type]);
#endif
    return;
}

uint32_t get_value_len_by_apply_type(int32_t apply_type)
{
    if (apply_type >= PELAGODB_BUSSINESS_BUTT)
    {
        return 0;
    }

    if (0 == art_db_mgr_info[apply_type].val_len)
    {
        return MAX_ART_VALUE_LEN;
    }

    return art_db_mgr_info[apply_type].val_len;
}

uint32_t get_key_len_by_apply_type(int32_t apply_type)
{
    if (apply_type >= PELAGODB_BUSSINESS_BUTT)
    {
        return 0;
    }

    if (0 == art_db_mgr_info[apply_type].key_len)
    {
        return MAX_KEY_LEN;
    }

    return art_db_mgr_info[apply_type].key_len;
}

#ifdef ARTREE_DEBUG_MEM
void art_detect_mem(void)
{
    for (int32_t i = 0; i < PELAGODB_BUSSINESS_BUTT; ++i)
    {
        for (int32_t j = 0; j < ART_MEMORY_END; ++j)
        {
            ART_ASSERT(0 == atomic64_read(&art_mem_detect[i][j]));
        }
    }
}
#endif
#ifdef __cplusplus
}
#endif


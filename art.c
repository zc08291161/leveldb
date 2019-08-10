#include <assert.h>
#include <stdlib.h>
#include <malloc.h>

#include "art_mem.h"
#include "art_log.h"
#ifndef _SCM_PCLINT_
#ifndef WIN32
#include "logging.h"
#endif
#include "art_sfmea.h"
#include "art_path.h"
#include "art_memtable.h"
#include "art_ftds.h"
#include "art_sstable.h"
#include "art_iterator.h"
#include "art_serialize.h"
#include "art_memtable_io.h"
#include "art_sstable_io.h"
#include "art_memtable_snap_io.h"
#include "art_snap_path_impl.h"
#include "art_snap_path_memtable.h"
#include "art_diagnose_base.h"
#include "art_stat.h"
#endif
#ifdef WIN32_DEBUG
#include <time.h>
#include <windows.h>
#endif

#ifndef ARTREE_DEBUG
#include "dpmm.h"
#ifndef _SCM_PCLINT_
#include "../../../debug/common_ftds.h"
#include "errcode.h"
#endif
#endif


#ifdef __cplusplus
extern "C" {
#endif

#if DESC("init & reg & create & free & destroy interface")

// internal operation set 
artree_operations_t g_artree_operations[KEY_VALUE_TYPE_BUTT] = {{0}};

static void init_internal_operation(void)
{
    g_artree_operations[ART_FKEY_VALUE_1B].calc_sstable_node_actual_size        = calc_sstable_svalue_node_actual_size;
    g_artree_operations[ART_FKEY_VALUE_4B].calc_sstable_node_actual_size        = calc_sstable_vvalue_node_actual_size;
    g_artree_operations[ART_VKEY_VALUE_4B].calc_sstable_node_actual_size        = calc_sstable_vvalue_node_actual_size;

    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_op_type    = get_svalue_leaf_op_type;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_snap_id    = get_svalue_leaf_snapid;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_value_len  = get_svalue_leaf_value_len;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_value      = get_svalue_leaf_value;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.set_op_type    = set_svalue_leaf_op_type;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.set_snap_id    = set_svalue_leaf_snapid;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.set_value      = set_svalue_leaf_value;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.calc_leaf_size = calc_svalue_leaf_size;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.check_key_valid= check_svalue_leaf_key_valid;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.set_leaf_key   = set_svalue_leaf_key;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_leaf_key   = get_svalue_leaf_key;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.set_leaf_key_len = NULL;
    g_artree_operations[ART_FKEY_VALUE_1B].leaf_ops.get_leaf_key_len = get_svalue_leaf_key_len;    

    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_op_type    = get_leaf_op_type;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_snap_id    = get_leaf_snapid;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_value_len  = get_leaf_value_len;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_value      = get_leaf_value;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.set_op_type    = set_leaf_op_type;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.set_snap_id    = set_leaf_snapid;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.set_value      = set_leaf_value;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.calc_leaf_size = calc_leaf_size;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.check_key_valid= check_leaf_key_valid;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.set_leaf_key   = set_leaf_key;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_leaf_key   = get_leaf_key;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.set_leaf_key_len = set_leaf_key_len;
    g_artree_operations[ART_FKEY_VALUE_4B].leaf_ops.get_leaf_key_len = get_leaf_key_len;  

    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_op_type    = get_leaf_op_type;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_snap_id    = get_leaf_snapid;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_value_len  = get_leaf_value_len;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_value      = get_leaf_value;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.set_op_type    = set_leaf_op_type;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.set_snap_id    = set_leaf_snapid;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.set_value      = set_leaf_value;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.calc_leaf_size = calc_leaf_size;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.check_key_valid= check_leaf_key_valid;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.set_leaf_key   = set_leaf_key;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_leaf_key   = get_leaf_key;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.set_leaf_key_len = set_leaf_key_len;
    g_artree_operations[ART_VKEY_VALUE_4B].leaf_ops.get_leaf_key_len = get_leaf_key_len; 
}



/*
* description               : artree module init
*/
int32_t init_artree_module(uint32_t apply_type, uint32_t inst_num, uint32_t key_len, uint32_t val_len)
{
    int32_t ret = kOk;
    
#ifndef WIN32_DEBUG
    ret = dpmm_init(0);
    if (DPMM_ERR_SUCCESS != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "dpmm init fail apply_type %d] inst_num[%u] key_len[%u] val_len[%u] ret[%d].",
                     apply_type, inst_num, key_len, val_len, ret);
        return ret;
    }

#endif

    /*初始化内部操作函数集*/
    init_internal_operation();

    ret = art_dpmm_init(apply_type, inst_num, key_len, val_len);
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "init artree dpmm fail apply_type[%d] inst_num[%u] key_len[%u] val_len[%u] ret[%d].",
                     apply_type, inst_num, key_len, val_len, ret);
        return ret;
    }

    ret = regArtDiagCmd();
    if(unlikely(kOk != ret))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "reg art diagnose failed, ret[%d].", ret);
        return ret;        
    }

    ret = initArtSFMEA();
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "init artree sfmea, ret[%d].", ret);
        return ret;
    }
    
    init_art_stat_info(apply_type);

    atomic64_set(&art_tolerate_snap_disorder, 1);
    
    ART_LOGINFO(ART_LOG_ID_4B00, "init artree module apply_type[%d] inst_num[%u] key_len[%u] val_len[%u] succeed.",
                apply_type, inst_num, key_len, val_len);
    
    return kOk;
}
/*
* description               : init the artree module 
* return                    : kOk means init successfully.
*/
void exit_artree_module(uint32_t apply_type)
{
    /* unreg art diagnose */
    unregArtDiagCmd();
    
    /*exit dpmm slab*/
    art_dpmm_exit(apply_type);
    
    ART_LOGINFO(ART_LOG_ID_4B00, "exit artree apply_type[%u] module succeed.", apply_type);

    return;
}

/*
* description               : register operation set for different bussiness type of art 
* artree_handler            : art tree handler
* reg_para                  : registered operation set
*/
void init_artree_operations(void* artree_handler, artree_operations_reg_para_t *reg_para)
{
    art_tree_t *t = (art_tree_t *)artree_handler;

    /*t->ops.alloc                  = reg_para->alloc;
    t->ops.free                   = reg_para->free;
    t->ops.read                   = reg_para->read;
    t->ops.batch_read             = reg_para->batch_read;
    t->ops.get_buffer             = reg_para->get_buffer;
    t->ops.release_buffer         = reg_para->release_buffer;
    t->ops.write                  = reg_para->write;
    t->ops.record_valid_size      = reg_para->gc_record;
    t->ops.hbf_add_key            = reg_para->hbf_add_key;
    t->ops.gc_record              = reg_para->gc_record;
    t->ops.arbitrate              = reg_para->arbitrate;
    t->ops.compute_sharding_group = reg_para->compute_sharding_group;
    t->ops.promoter_func          = reg_para->promoter_func;
    t->ops.should_stop            = reg_para->should_stop;
    t->ops.get_cond_len           = reg_para->get_cond_len;
    t->ops.get_shardid_by_key     = reg_para->get_shardid_by_key;
    t->ops.migrated_key_calc      = reg_para->migrated_key_calc;
    t->ops.migration_extracted_key= reg_para->migration_extracted_key;
    t->ops.need_migration_calc    = reg_para->need_migration_calc;*/
	ART_MEMCPY(&(t->ops), reg_para, sizeof(artree_operations_reg_para_t));
    return;
}


/*
* description               : create a artree
* create_para               : create para
* artree_handler            : output artree handler
* return                    : kOk if success, otherwise other errcode
*/
int32_t create_artree(artree_create_para_t *create_para, void** artree_handler)
{
    art_tree_t* t                    = NULL;
    artree_operations_reg_para_t *op = NULL;
    
    if (NULL == create_para || NULL == artree_handler)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "create artree param fail create_para[%p] artree_handler[%p].",
                     create_para, artree_handler);
        return kInvalidArgument;
    }
    
    op = &create_para->ops;

	LVOS_TP_START(ART_TP_CREATE_ALLOC_MEM, &t)
	t = (art_tree_t*)op->alloc(create_para->args, sizeof(art_tree_t));
	LVOS_TP_END

	if (NULL == t)
	{
		ART_LOGERROR(ART_LOG_ID_4B00, "alloc artree memory fail.");
		return kOutOfMemory;
	}

    art_tree_init(t, create_para);

    *artree_handler = (void*)t;

    ART_LOGDEBUG(ART_LOG_ID_4B00, "create artree apply_type[%d] succeed.", t->apply_type);
    return kOk;
}


/*
* description               : destroy the whole artree
* artree_handler            : artree handler
*/
void free_artree(void *artree_handler)
{
    art_tree_t* t   = NULL;
    artree_ext_operations_t *op = NULL;
    int32_t ret = kOk;
    
    if (NULL == artree_handler)
    {
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree to be free is null artree_handler[%p].", artree_handler);
        return;
    }
    
    t  = (art_tree_t*)artree_handler;
    op = &t->ops;

    ret = art_tree_destroy(t, op->free);
    if (kOk !=  ret)
    {
        ART_LOGWARNING(ART_LOG_ID_4B00, "artree destroy fail ret=%d.", ret);
        return;
    }

    ART_ASSERT(t != NULL);
    op->free(t);

    ART_LOGDEBUG(ART_LOG_ID_4B00, "destory artree apply_type[%d] succeed.", t->apply_type);
    return;
}

/*
* description               : rebuild atree. 
* rebuild_para              : rebuild_para para
* return                    : artree handler
*/
int32_t rebuild_artree(IN artree_rebuild_para_t *rebuild_para, void** tree)
{
    void *pnode                         = NULL;
    void *buff                          = NULL;
    art_tree_t *artree                  = NULL; 
    artree_operations_reg_para_t *op    = NULL;
    int32_t ret                         = kOk;
    art_tree_t *tmp_buff                = NULL;
    
    if (NULL == rebuild_para)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "rebuild artree param fail rebuild_para[%p].",
                     rebuild_para);
        return kInvalidArgument;
    }
    
    op = &rebuild_para->ops;
    LVOS_TP_START(ART_TP_REBUILD_READPLOG_FILE_NOT_FOUND, &ret)
    ret = op->read(rebuild_para->file, &pnode, rebuild_para->offset, sizeof(art_tree_t));
    LVOS_TP_END
    if(ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "rebuild artree read plog fail offset[%llu] size[%d] ret[%d].",
             rebuild_para->offset, sizeof(art_tree_t), ret);
        return ret;
    }

    buff = (void*)art_get_buff(op->get_buffer, pnode);
    if (NULL == buff)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "rebuild artree get buff fail.");
        return kIOError;
    }
    ART_LOGDEBUG(ART_LOG_ID_4B00,"rebuild artree read plog file=[%p], offset[%llu] size[%d].",rebuild_para->file,
                     rebuild_para->offset, sizeof(art_tree_t));
    tmp_buff = (art_tree_t*)buff;
    ART_LOGDEBUG(ART_LOG_ID_4B00,"rebuild artree buff=[tmp_buff->apply_type=[%u], tmp_buff->art_crc=[%llu], tmp_buff->art_type=[%u], tmp_buff->root_size=[%u], tmp_buff->scm_start_offset=[%llu], tmp_buff->min_key_len=[%u], tmp_buff->max_key_len=[%u]].",
                     tmp_buff->apply_type, tmp_buff->art_crc, tmp_buff->art_type, tmp_buff->root_size,
                     tmp_buff->scm_start_offset, tmp_buff->min_key_len, tmp_buff->max_key_len);  
    if(tmp_buff->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "rebuild artree fail, [%llu].", tmp_buff->art_crc);
        art_release_node(op->release_buffer, pnode);
        return kInvalidArgument;
    } 
    LVOS_TP_START(ART_TP_REBUILD_ALLOC_MEM, &artree)
    artree = (art_tree_t*)art_alloc_structure(((art_tree_t*)buff)->apply_type, REBUILD_ARTREE, 1, sizeof(art_tree_t), FlushLevelType_Normal);
    LVOS_TP_END
    if (NULL == artree)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "rebuild artree alloc memory fail.");
        art_release_node(op->release_buffer, pnode);
        return kOutOfMemory;
    }
		
    ART_MEMCPY(artree, buff, sizeof(art_tree_t));
    init_artree_operations(artree, &rebuild_para->ops);
    artree->art_ops = &g_artree_operations[GET_ARTREE_KEY_VALUE_TYPE(artree)];
    art_release_node(op->release_buffer, pnode);
    
    ART_ASSERT(artree->art_crc == ART_ROOT_CRC);

    ART_LOGINFO(ART_LOG_ID_4B00, "rebuild artree apply_type[%d] succeed.", artree->apply_type);
    *tree = (void*)artree;
    return kOk;

}


/*
* description               : only destroy the root of a artree
* artree_handler            : artree handler
*/
void destroy_artree(void* tree)
{
    if (NULL != tree)
    {
        art_free_structure(((art_tree_t*)tree)->apply_type, REBUILD_ARTREE, tree);
    }

    ART_LOGDEBUG(ART_LOG_ID_4B00, "destory artree succeed.");
    return;
}

#endif

#if DESC("get art max & min key")

/*
* description               : get min key. 
* artree                    : artree handler
* return                    : kOk if success, otherwise other errcode.
*/
int32_t art_get_min_key(void *artree_handler, uint8_t* key, uint32_t* key_len, void *file)
{
    art_tree_t* t                = NULL;
    art_node*   node             = NULL;
    uint32_t    key_buff_len     = *key_len;
    int32_t     ret              = kOk;
    artree_ext_operations_t *ops = NULL;
    
    *key_len = 0;
    t = (art_tree_t*)artree_handler;
    if (t == NULL || key == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "param invalid! artree_handler = [%p], key = [%p]", artree_handler, key);
        return kInvalidArgument;
    }
    if (t->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "param invalid! art_crc = [%lu]", t->art_crc);
        return kIOError;
    }
    if (is_memtable_art(t) && t->root.mem_addr == NULL)
    {
        ART_LOGINFO(ART_LOG_ID_4B00, "memtable: root is NULL");
        return kOk;
    }
    ops = &t->ops;
    ret = get_root_node(t, file, (void**)&node);
    if (ret != kOk)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "root is NULL!");
        return ret;
    }
    if (is_memtable_art(t))
    {
        ret = memtable_io_get_min_key(t, node, key_buff_len, key, key_len);
    }else
    {
        ret = sstable_min_key_io(t, (void*)node, key_buff_len, ops->read, file, key, key_len);
    }
    
    if (ret != kOk) 
    {
        *key_len = key_buff_len;
        return ret;
    }

    return kOk;
}


/*
* description               : get max key. 
* artree                    : artree handler
* return                    : kOk if success, otherwise other errcode.
*/
int32_t art_get_max_key(void *artree_handler, uint8_t *key, uint32_t *key_len, void *file)
{
    art_tree_t* t                = NULL;
    art_node*   node             = NULL;
    uint32_t    key_buff_len     = *key_len;
    int32_t     ret              = kOk;
    artree_ext_operations_t *ops = NULL;
    
    *key_len = 0;
    t = (art_tree_t*)artree_handler;
    if (t == NULL || key == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "param invalid! artree_handler = [%p], key = [%p]", artree_handler, key);
        return kInvalidArgument;
    }

    if (t->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "param invalid! art_crc = [%lu]", t->art_crc);
        return kIOError;
    }

    if (is_memtable_art(t) && t->root.mem_addr == NULL)
    {
        ART_LOGINFO(ART_LOG_ID_4B00, "memtable: root is NULL");
        return kOk;
    }
    ops = &t->ops;
    
    ret = get_root_node(t, file, (void**)&node);
	if (ret != kOk)
	{
		ART_LOGERROR(ART_LOG_ID_4B00, "root is NULL!");
		return ret;
	}

    if (is_memtable_art(t))
    {
        ret = memtable_io_get_max_key(t, node, key_buff_len, key, key_len);
    }
    else
    {
        ret = sstable_io_get_max_key((void*)t, node, key_buff_len, ops->read, file, key, key_len);
    }

    if (ret != kOk) 
    {
        *key_len = key_buff_len;
        return ret;
    }

    return kOk;
}

#endif

#if DESC("get art memory useage")

/*
* description:
*    return mem usage
*/
uint32_t get_art_memory_usage(void *artree_handler)
{
	art_tree_t* t = (art_tree_t *)artree_handler;
    return t->art_mem_usage;
}

#endif

/*
* description               : insert KV to artree
* artree_handler            : artree handler
* insert_para               : insert para
* return                    : kOk if success, otherwise other errcode.
*/
int32_t insert_artree(void *artree_handler, artree_insert_para_t *insert_para)
{
    art_tree_t* root = NULL;
    int32_t     ret  = kOk;
    
    if (check_insert_param_invalid(artree_handler, insert_para))
    {
        /*has printed error info in check func*/
        return kInvalidArgument;
    }
    
    root = (art_tree_t *)artree_handler;

    if(!check_lunmap_value_len_vaild(root, insert_para->value_len))
    {
        return kInvalidArgument;
    }

    if(!check_key_len_vaild(root, insert_para))
    {
        return kInvalidArgument;
    }
    
    ret = art_insert(root, insert_para->key, insert_para->key_len, 
                     insert_para->snapshotid, insert_para->type, 
                     insert_para->value, insert_para->value_len, 
                     root->ops.alloc, insert_para->arena_args);
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "insert artree fail ret = %d.", ret);
        return ret;
    }
    
    /* update max_key_len and min_key_len of tree */
    if (insert_para->key_len > root->max_key_len)
    {
        root->max_key_len = insert_para->key_len;
    }
    
    if (insert_para->key_len < root->min_key_len)
    {
        root->min_key_len = insert_para->key_len;
    }
    
    return kOk;
}

/*check batch insert param is invalid*/
bool check_batch_insert_param_invalid(void *artree_handler, artree_batch_insert_para_t *insert_para)
{
    if (NULL == artree_handler || NULL == insert_para)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch insert artree param fail artree_handler[%p] insert_para[%p].",
                     artree_handler, insert_para);
        return true;
    }
    
    if (insert_para->key == NULL || insert_para->key_len == 0  || insert_para->key_len > MAX_KEY_LEN || 
        insert_para->kv_num == 0 || insert_para->value == NULL || insert_para->value_len == NULL)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch insert artree param fail insert_para(key[%p] key_len[%u] kv_num [%i] value[%p] value_len[%u]).",
                     insert_para->key, insert_para->key_len, insert_para->kv_num, insert_para->value, insert_para->value_len);
        return true;
    }
    return false;
}
int32_t pretend_batch_insert(void *artree_handler, artree_batch_insert_para_t *batch_insert_para)
{
    artree_insert_para_t insert_para = { 0 };
    uint32_t index = 0;
    int32_t  ret   = kOk;

    for (index = 0; index < batch_insert_para->kv_num; ++index)
    {
        insert_para.key        = batch_insert_para->key[index];
        insert_para.key_len    = batch_insert_para->key_len;
        insert_para.value      = batch_insert_para->value[index];
        insert_para.value_len  = batch_insert_para->value_len[index];
        insert_para.snapshotid = batch_insert_para->snapshotid;
        insert_para.type       = batch_insert_para->type;
        insert_para.arena_args = batch_insert_para->arena_args;

        ret = insert_artree(artree_handler, &insert_para);
        if (kOk != ret)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "pretend batch insert artree fail kv_num %u key_len %u val_len %u snapid %u type %d ret %d.",
                         batch_insert_para->kv_num, insert_para.key_len, insert_para.value_len, insert_para.snapshotid, insert_para.type, ret);
            return ret;
        }

    }

    return kOk;
}

/*
* description               : insert KVs to artree
* artree_handler            : artree handler
* insert_para               : insert para, see artree_batch_insert_para_t
* return                    : kOk if success, otherwise other errcode.
*/
int32_t batch_insert_artree(void *artree_handler, artree_batch_insert_para_t *insert_para)
{
    art_tree_t* root = (art_tree_t*)artree_handler;
    int32_t     ret  = kOk;
    
    if (check_batch_insert_param_invalid(artree_handler, insert_para))
    {
        /*has printed error info in check func*/
        return kInvalidArgument;
    }
    //prefix is 0, circularly call the old interface to insert 
    if (0 == insert_para->prefix_len)
    {
        return pretend_batch_insert(artree_handler, insert_para);
    }

    ret = batch_insert(root, insert_para);
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch insert artree fail ret = %d.", ret);
        return ret;
    }
    
    /* update max_key_len and min_key_len of tree */
    if (insert_para->key_len > root->max_key_len)
    {
        root->max_key_len = insert_para->key_len;
    }
    
    if (insert_para->key_len < root->min_key_len)
    {
        root->min_key_len = insert_para->key_len;
    }
    
    return kOk;
}

/*
* description               : accurately query. 
* artree_handler            : artree handler
* get_para                  : get para
* constraint                : the output value can not be modified by user
* return                    : kOk if find value with identical key&snapshotid, otherwise other errcode.
*/
int32_t get_artree2(void *artree_handler, artree_get_para_t *get_para)
{
    int32_t     ret                 = kOk;
    uint64_t    time                = 0;
    void*       origin_node         = NULL;
    void*       fusion_origin_node  = NULL;
    art_node*   fusion_node         = NULL;
    art_tree_t* root                = NULL;
    art_data*   data                = NULL;
    bool        is_fusion_node_leaf = false;
    uint32_t    value_len           = 0;
    
    root = (art_tree_t*)artree_handler;
    /* in_param judgment */
    if (!check_artree_param(root, get_para->key, get_para->key_len, get_para->value, get_para->value_len))
    {
        /*has print error info in check_artree_param*/
        return kInvalidArgument;
    }

    /* init out_param */
    *(get_para->value)              = NULL;
    *(get_para->value_len)          = 0;
    get_para->buffer                = NULL;
    get_para->release_buffer_func   = root->ops.release_buffer;

    start_comm_ftds(ART_GET_IN_ONE_TREE, &time);
    if (is_memtable_art(root))
    {
        ret = memtable_io_search_key(root, root->root.mem_addr, get_para->key, get_para->key_len, get_para->snapshotid, GetType, (art_snap_node**)&origin_node);
        if (kOk != ret)
        {            
            end_comm_ftds(ART_GET_IN_ONE_TREE, (uint32_t)(kNotFound == ret ? kOk : ret), time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable get artree key key_len[%u] snapid[%u] fail.",
                         get_para->key_len, get_para->snapshotid);
            return ret;
        }
        ret = memtable_snap_search(root, (art_snap_node*)origin_node, get_para);
    }
    else
    {
        ret = art_sstable_search_io(root, get_para->key, get_para->key_len, get_para->file, get_para->snapshotid, GetType, &data, 
                                    &is_fusion_node_leaf, &fusion_origin_node, &value_len, (art_snap_node**)&origin_node);
        if (kOk != ret)
        {            
            end_comm_ftds(ART_GET_IN_ONE_TREE, (uint32_t)(kNotFound == ret ? kOk : ret), time);
 
            if(kNotFound == ret)
            {
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable get artree key key_len[%u] snapid[%u] not found.",
                             get_para->key_len, get_para->snapshotid);
                return ret;
            }
            
            ART_LOGERROR(ART_LOG_ID_4B00, "sstable get artree key key_len[%u] snapid[%u] fail.",
                        get_para->key_len, get_para->snapshotid);   
            return ret;
        }
        
        if (NULL != data)
        {
            fusion_node = (art_node*)art_get_buff(root->ops.get_buffer, fusion_origin_node);
            ret = sstable_fusion_node_verify_key(root, fusion_node, (uint8_t*)get_para->key, get_para->key_len);
            if(kOk != ret)
            {
                //release fusion_origin_node
                art_release_node(get_para->release_buffer_func, fusion_origin_node);
                
                //key verify fail, return kArtVerifyKeyInvalid
                return ret;
            }
            
            if (data->snapid == get_para->snapshotid)
            {
                *(get_para->value)      = data->value;
                //*(get_para->value_len) = root->value_len;
                *(get_para->value_len)  = value_len;
                end_comm_ftds(ART_GET_IN_ONE_TREE, (uint32_t)(kNotFound == ret ? kOk : ret), time);

                get_para->buffer = fusion_origin_node;
                /*leaf节点不需要释放*/
                if (origin_node != NULL && !is_fusion_node_leaf)
                {
                    //release origin_node
                    art_release_node(get_para->release_buffer_func, origin_node);
                }
                //DTS2019013109783返回删除的key类型，需要释放buff
                if (kTypeDeletion == data->op_type)
                {
                    art_release_node(get_para->release_buffer_func, fusion_origin_node);
                    get_para->buffer = NULL;
                }
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable get artree leaf key key_len[%u] snapid[%u] value_len[%u] op_type[%d] succeed.",
                             get_para->key_len, get_para->snapshotid, *(get_para->value_len), data->op_type);
                
                return kTypeValue == data->op_type ? kOk : kArtGetIsDeleted;
            } 
        }
        
        if (is_fusion_node_leaf)
        {      
            end_comm_ftds(ART_GET_IN_ONE_TREE, ret, time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable get artree snap id not equal key_len[%u] snapid[%u].",
                           get_para->key_len, get_para->snapshotid);

            //release fusion_origin_node
            art_release_node(get_para->release_buffer_func, fusion_origin_node);
            return kArtSnapIdNotFound;
        }

        if (origin_node == NULL)
        {
            end_comm_ftds(ART_GET_IN_ONE_TREE, ret, time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable get artree key not found key_len[%u] snapid[%u].",
                           get_para->key_len, get_para->snapshotid);
            return kNotFound;
        }

        if (NULL != data)
        {
            art_release_node(get_para->release_buffer_func, fusion_origin_node);
        }

        ret = sstable_snap_search((void*)root, origin_node, get_para);
    }

    //get leaf is deletion
    if(kArtGetIsDeleted == ret)
    {
        INDEX_LOG_INFO_LIMIT("sstable get artree key succ, ret[%d] key_len[%u] snapid[%u].",
                            ret, get_para->key_len, get_para->snapshotid);
    }
    
    end_comm_ftds(ART_GET_IN_ONE_TREE, (uint32_t)((kNotFound == ret || kArtSnapIdNotFound == ret || kArtGetIsDeleted == ret)? kOk : ret), time);
    return ret;
}

/*
int32_t memtable_io_get_equal_key_le_snapid(
    IN art_tree_t *artree,
    IN art_node *start_node,
    IN uint8_t *key,
    IN uint32_t key_len,
    IN uint32_t snapid,
    IN artree_operations_t *ops)
{
    art_node *cur_node                      = NULL;
    art_node *next_node                     = NULL;

    int32_t result                          = kOk;
    uint32_t idx_in_parent                  = 0;
    uint32_t depth                          = 0;
    art_snap_path_t snap_path;

    cur_node = start_node;
    while(is_art_inner_node(cur_node))
    {
        // get next equal
        result = memtable_search_node_equal(cur_node, key, key_len, depth, &next_node, &idx_in_parent);
        if(kOk != result)
        {
            return result;
        }

        depth += cur_node->partial_len;
        if(END_POSITION_INDEX != idx_in_parent)
        {
            depth += 1;
        }

        // set child current node
        cur_node = next_node;
    }

    if(depth != key_len)
    {
        return kNotFound;
    }

    // init snap path
    init_snap_path(&snap_path);
    assign_snap_path_internal_ops(&snap_path, ops);
    memtable_snap_path_get_smaller_or_equal(&snap_path, (art_snap_node *)cur_node, idx_in_parent, snapid);
    return kOk;
}
*/

/*
* description               : fuzzy query. 
* artree_handler            : artree handler
* seek_para                 : seek para
* constraint                : the output value can not be modified by user
* return                    : kOk if find value with identical key and the snapshoted <= input.snapshotid, otherwise other errcode.
*/
int32_t seek_artree2(void *artree_handler, artree_seek_para_t *seek_para)
{
    int32_t     ret                 = kOk;
    uint64_t    time                = 0;
    void*       origin_node         = NULL;
    art_tree_t* root                = NULL;
    art_data*   data                = NULL;
    bool        is_fusion_node_leaf = false;
    void*       fusion_origin_node  = NULL;
    art_node*   fusion_node         = NULL;
    uint32_t    value_len           = 0;

    root = (art_tree_t*)artree_handler;
    /* in_param judgment */
    if (!check_artree_param(root, seek_para->key, seek_para->key_len, seek_para->value, seek_para->value_len))
    {
        return kInvalidArgument;
    }

    /* init out_param */
    *(seek_para->value)             = NULL;
    *(seek_para->value_len)         = 0;
    seek_para->buffer               = NULL;
    seek_para->release_buffer_func  = root->ops.release_buffer;

    start_comm_ftds(ART_SEEK_IN_ONE_TREE, &time);
    if (is_memtable_art(root))
    {
        ret = memtable_io_search_key(root, root->root.mem_addr, seek_para->key, seek_para->key_len, seek_para->snapshotid, SeekType, (art_snap_node**)&origin_node);
        if (kOk != ret)
        {            
            end_comm_ftds(ART_SEEK_IN_ONE_TREE, (uint32_t)(kNotFound == ret ? kOk : ret), time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable seek artree key key_len[%u] snapid[%u] fail.",
                         seek_para->key_len, seek_para->snapshotid);
            return ret;
        }
        ret = memtable_snap_seek(root, (art_snap_node*)origin_node, seek_para);
    }
    else
    {
        ret = art_sstable_search_io(root, seek_para->key, seek_para->key_len, seek_para->file,seek_para->snapshotid, SeekType, &data, 
                                    &is_fusion_node_leaf, &fusion_origin_node, &value_len, (art_snap_node**)&origin_node);
        if (kOk != ret)
        {            
            end_comm_ftds(ART_SEEK_IN_ONE_TREE, (uint32_t)(kNotFound == ret ? kOk : ret), time);
            if(kIOError == ret)
            {
                ART_LOGERROR(ART_LOG_ID_4B00, "sstable seek artree key key_len[%u] snapid[%u] fail.",
                             seek_para->key_len, seek_para->snapshotid);                
            }
            else
            {
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable seek artree key key_len[%u] snapid[%u] fail.",
                             seek_para->key_len, seek_para->snapshotid);
            }
            return ret;
        }
        
        if (NULL != data)
        {
            fusion_node = (art_node*)art_get_buff(root->ops.get_buffer, fusion_origin_node);
            ret = sstable_fusion_node_verify_key(root, fusion_node, (uint8_t*)seek_para->key, seek_para->key_len);
            if(kOk != ret)
            {
                //release fusion_origin_node
                art_release_node(seek_para->release_buffer_func, fusion_origin_node);
                
                //key verify fail, return kArtVerifyKeyInvalid
                return ret;
            }
            
            if (data->snapid <= seek_para->snapshotid)
            {
                *(seek_para->value)        = data->value;
                //*(seek_para->value_len)    = root->value_len;
                *(seek_para->value_len)    = value_len;
                *(seek_para->snapid_found) = data->snapid;
                end_comm_ftds(ART_SEEK_IN_ONE_TREE, ret, time);
                
                seek_para->buffer = fusion_origin_node;
                /*leaf节点不需要释放*/
                if (origin_node != NULL && !is_fusion_node_leaf)
                {
                    art_release_node(seek_para->release_buffer_func, origin_node);
                }
                //DTS2019013109783返回删除的key类型，需要释放buff
                if (kTypeDeletion == data->op_type)
                {
                    art_release_node(seek_para->release_buffer_func, fusion_origin_node);
                    seek_para->buffer = NULL;
                }
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable get artree leaf key key_len[%u] seek snapid[%u] find snapid[%u] value_len[%u] op_type[%d] succeed.",
                             seek_para->key_len, seek_para->snapshotid, data->snapid, *(seek_para->value_len), data->op_type);
                return kTypeValue == data->op_type ? kOk : kArtGetIsDeleted;
            } 
        }
        if (is_fusion_node_leaf)
        {       
            end_comm_ftds(ART_SEEK_IN_ONE_TREE, ret, time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable seek artree snap id not equal key_len[%u] snapid[%u].",
                           seek_para->key_len, seek_para->snapshotid);

            art_release_node(seek_para->release_buffer_func, fusion_origin_node);

            return kArtSnapIdNotFound;
        }

        if (origin_node == NULL)
        {    
            end_comm_ftds(ART_SEEK_IN_ONE_TREE, ret, time);
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable seek artree key not found key_len[%u] snapid[%u].",
                           seek_para->key_len, seek_para->snapshotid);
            return kNotFound;
        }       

        if (NULL != data)
        {
            art_release_node(seek_para->release_buffer_func, fusion_origin_node);
        }
        ret = sstable_snap_seek(root, origin_node, seek_para);
    }

    //seek leaf is deletion
    if(kArtGetIsDeleted == ret)
    {
        INDEX_LOG_INFO_LIMIT("sstable seek artree key succ, ret[%d] key_len[%u] snapid[%u].",
                            ret, seek_para->key_len, seek_para->snapshotid);
    }
    
    end_comm_ftds(ART_SEEK_IN_ONE_TREE, (uint32_t)((kNotFound == ret || kArtSnapIdNotFound == ret || kArtGetIsDeleted == ret) ? kOk : ret), time);
    
    return ret;
}

/*
* description               : clone the entire tree to the plog. 
* tree_handler              : artree handler
* serialize_para            : the param of serialize
* return                    : root or root.node is NULL, return 0
*/
int32_t serialize(void *artree_handler, artree_serialize_para_t *serialize_para)
{
    art_tree_t *art_tree = (art_tree_t*)artree_handler;

    /* param check */
    if ((artree_handler == NULL) || (serialize_para == NULL))
    {
        ART_LOGERROR(ART_LOG_ID_4B00,"serialize param fail, artree_handler[%p] or serialize_para[%p] is NULL.", artree_handler, serialize_para);
        return kInvalidArgument;
    }

    if((serialize_para->plog_addr == NULL)
        || (serialize_para->write_args == NULL) || (serialize_para->plog_size == 0))
    {
        ART_LOGERROR(ART_LOG_ID_4B00,"serialize param fail, hbf[%p] or plog_addr[%p] or write_args[%p] is NULL, plog_size[%lu] is zero.", 
                     serialize_para->hbf, serialize_para->plog_addr, serialize_para->write_args, serialize_para->plog_size);
        return kInvalidArgument;
    }
    
    if (art_tree->art_crc != ART_ROOT_CRC)
    {
        ART_LOGERROR(ART_LOG_ID_4B00,"serialize io error, art_crc[%lu] error.", art_tree->art_crc);
        return  kIOError; 
    }

    /* return root addr in scm */
    return flush_art_tree(art_tree, serialize_para);
}

/*
* description               : get multiple keys in one operation. 
* tree_handler              : artree handler
* get_para                  : batch get para
* get_callback              : return value to upper
* return                    : batch get successful return kOk, otherwise return errcode
*/
int32_t batch_get_artree(void *tree_handler, artree_batch_get_para_t *get_para, ARTREE_GET_CALLBACK get_callback)
{
    art_tree_t *artree                  = (art_tree_t*)tree_handler;
    int32_t ret                         = kOk;
    art_node *root_node                 = NULL;
	batch_get_path_t *get_path          = NULL;
    uint64_t time                       = 0;

    /* input param check */
    if((NULL == tree_handler) || 
       (NULL == get_para) || 
       (NULL == get_callback))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree param fail, artree_handlers[%p] get_para[%p] get_callback[%p].",
                     tree_handler, get_para, get_callback);
        return kInvalidArgument;
    }

    /* get_para check */
    if((NULL == get_para->keys) || 
       (NULL == get_para->snapshotid) || 
       (0 == get_para->keys_num) || 
       (0 == get_para->key_len))
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree param fail, keys[%p] snapid[%p] keys_num[%d] key_len[%d].",
                     get_para->keys, get_para->snapshotid, get_para->keys_num, get_para->key_len);
        return kInvalidArgument;
    }

    /* empty tree check */
    if(check_art_empty(artree))
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree artree[%p] is empty, root_size[%d] is zero.", 
                     artree->root.mem_addr, artree->root_size);
        return kNotFound;
    }
/*    
#ifndef ARTREE_DEBUG
    if(ARTreeApplyTypeLunMap != artree->apply_type)
    {
        ART_LOGERROR(ART_LOG_ID_4B00,"batch get artree param fail, the applt_type[%d] is not lunmap.", artree->apply_type);
        return kInvalidArgument;
    }
#endif*/
    
    //alloc get path memory
    LVOS_TP_START(ART_TP_BATCH_GET_ALLOC_PATH_MEM, &get_path)
    LVOS_TP_START(ART_TP_BATCH_GET_ALLOC_MEM, &get_path)
    get_path = (batch_get_path_t *)art_alloc_structure(artree->apply_type, BATCH_GET_ARTREE_PATH, 1, sizeof(batch_get_path_t), FlushLevelType_Normal);
    LVOS_TP_END
    LVOS_TP_END
    if(NULL == get_path)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree alloc batch_get_path mem fail.");
        return kOutOfMemory;
    }
    
    /* init batch_get_path para */
    init_art_path(&get_path->art_path, artree);
    get_path->artree = artree;

    /* get root node from artree */
    LVOS_TP_START(ART_TP_BATCH_GET_ROOT_READPLOG_FILE_NOT_FOUND, &ret)
    ret = get_root_node(artree, get_para->file, (void**)&root_node);
    LVOS_TP_END
    if(kOk != ret)
    {
        //free memory
        art_free_structure(artree->apply_type, BATCH_GET_ARTREE_PATH, (void*)get_path);
            
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree get root node fail, root node is null, file[%p] ret[%d].", get_para->file, ret);
        return ret;
    }

    start_comm_ftds(ART_BATCH_GET_KEYS, &time);
        
    if(is_memtable_art(artree))
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree go into memtable branch, artree_type[%d].", artree->art_type);
        
        /* get in memtable */
        ret = memtable_batch_get(get_path, root_node, get_para, get_callback);
    }
    else
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree go into sstable branch, artree_type[%d].", artree->art_type);
        
        /* get in sstable */
        ret = sstable_batch_get(artree, root_node, get_para, get_callback, artree->ops.batch_read);
    }

    end_comm_ftds(ART_BATCH_GET_KEYS, (uint32_t)ret, time);
           
    /* clear path for batch get */
    art_clear_path(get_path);

    //free memory
    art_free_structure(artree->apply_type, BATCH_GET_ARTREE_PATH, (void*)get_path);

    ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree complete, artree[%p] keys[%p] key_len[%d] keys_num[%d] ret[%d].", 
                 tree_handler, get_para->keys, get_para->key_len, get_para->keys_num, ret);
    return ret;
}

/*
* description               : get inject nodes info for test. 
* artree_handler            : artree handler
* node_type                 : the node type
* node_num                  : how much node number need return 
* offset                    : offsets of the return nodes
* return                    : seek successful return kOk, otherwise return errcode.
*/
int32_t test_art_get_inject_nodes_info(void *tree_handler, INJECT_NODE_TYPE node_type, uint32_t node_num, uint64_t** offset, void* plog_file)
{
    art_tree_t* artree = NULL;
    
    if (NULL == tree_handler || node_type >= INJECT_NODE_TYPE_BUTT || NULL == offset || NULL == plog_file)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get artree get inject nodes info fail, artree_handlers[%p] node type[%d] offset[%p] plog file[%p].",
                     tree_handler, node_type, offset, plog_file);
        return kInvalidArgument;
    }

    artree = (art_tree_t*)tree_handler;
    /*just for sstable*/
    if (SSTABLE_ART_TYPE != artree->art_type)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get artree get inject nodes info fail art type is invalid[%d", artree->art_type);
        return kInvalidArgument;
    }

    return kOk;
}

#ifdef __cplusplus
}
#endif


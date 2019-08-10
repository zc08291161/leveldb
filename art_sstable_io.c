#ifndef WIN32_DEBUG
#include "logging.h"
#endif
#include "art_sstable.h"
#include "art_log.h"
#include "art_ftds.h"

#ifndef _SCM_PCLINT_
#include "art_memtable_io.h"
#include "art_sstable_io.h"
#include "art_key_path_impl.h"
#include "art_snap_path_impl.h"
#include "art_iterator.h"
#include "art_mem.h"
#include "art_sfmea.h"
#endif

#ifndef ARTREE_DEBUG
#ifndef _SCM_PCLINT_
#include "../../../../debug/common_ftds.h"
#endif
#endif

//MODULE_ID(PID_ART);

#ifdef __cplusplus
extern "C" {
#endif


#if DESC("sstable for batch get interface")

/*
* description               : check nodes contains null.
* num                       : the node num get from rocache
* node                      : the node get from rocache
*/
int32_t check_node_valid(uint32_t num, void** node, RELEASE_BUFFER_FUNC release_buffer)
{
    uint32_t idx = 0;

    while(idx < num)
    {
        if(NULL == node[idx])
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable get plog fail, the idx[%d] node is null.", idx);
            break;
        }

        idx++;
    }
    if(idx != num)
    {
        for(idx = 0; idx < num; idx++)
        {
            if(NULL != node[idx])
            {
                release_buffer(node[idx]);
            }
        }
        
    }

    return kOk;
}

/*
* description               : search fusion node impl.
* artree                    : get next level node from the artree
* root_node                 : if get first level node, so get from root node; otherwise get from batch_read->node
* cur_node                  : search key from cur_node
* key                       : one key of batch get keys
* key_len                   : key_len of key
* key_pos                   : the position of key in batch get
* snapshotid                : the snapid of key
* instance                  : the para of get_callbeck
* args                      : the para of get_callback
* index                     : the positon of batch get node, use to release node
* batch_read                : the para of batch get(keys, key_num, key_len etc)
* get_callback              : get value callback to upper layer
*/
static void sstable_search_fusion_node_impl(
                                IN art_tree_t *artree,
                                IN art_node *root_node,
                                IN art_node *cur_node,
                                IN uint8_t *key, 
                                IN uint32_t key_len,
                                IN uint32_t key_pos,
                                IN uint32_t snapshotid,
                                IN void* instance,
                                IN void *args,
                                IN uint32_t index,
                                IO art_batch_read_para_t* batch_read, 
                                ARTREE_GET_CALLBACK get_callback)
{
    int32_t ret                         = kNotFound;
    uint8_t *value                      = NULL;
    uint32_t value_len                  = 0;
    bool is_fusion_node_leaf            = false;
    void *origin_child                  = NULL;
    artree_ext_operations_t *ext_ops    = &artree->ops;
    art_data *data                      = NULL;
    bool snap_flag                      = false;
    uint32_t idx                        = batch_read->index;
    bool release_flag                   = false;
    
    /* search in fusion node and snap tree */
    ret = search_fusion_node_info(cur_node, &origin_child, 
								key, key_len, &is_fusion_node_leaf,
								&batch_read->depth[key_pos], &data, &value_len, 
								&batch_read->offset[batch_read->index], &batch_read->size[batch_read->index],
                                (uint32_t*)&batch_read->index);
    
    if(kOk != ret)
    {
        /* return callback */
        get_callback(instance, args, key_pos, value, value_len, KNotExist, NULL, ext_ops->release_buffer);
    }
    else
    {
        /* set depth */
        batch_read->depth[key_pos]++;
        
        if ((NULL != data) && (data->snapid == snapshotid) && ((kTypeValue == data->op_type) || (kTypeDeletion == data->op_type)))
        {
            get_callback(instance, args, key_pos, data->value, value_len, data->op_type, batch_read->node[index], ext_ops->release_buffer);
            release_flag = true;
        }
        else if (is_fusion_node_leaf)
        {   
            /* return callback */
            get_callback(instance, args, key_pos, value, value_len, KNotExist, NULL, ext_ops->release_buffer);
            
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree sstable get artree snap id not equal, is_fusion_node_leaf[%d].", is_fusion_node_leaf);
        }      
        else
        {
            /* set snap_tree flag */
            snap_flag = true;
        }
    }

    if((NULL == root_node) && (release_flag == false))
    {
        /* release node */
        art_release_node(ext_ops->release_buffer, batch_read->node[index]);
    }
    
    if(true != snap_flag)
    {
        /* this key search complete, make this node NULL */
        batch_read->node[index] = NULL;

        /* set bitmap */
        art_set_bitmap(batch_read->bitmap, key_pos, true);

        /* set complete_key number */
        batch_read->complete_key_num++;

        if(idx != batch_read->index)
        {
            batch_read->index--;
        }
    }
                    
    return;    
}

/*
* description               : sstable get leaf impl.
* artree                    : get next level node from the artree
* root_node                 : if get first level node, so get from root node; otherwise get from batch_read->node
* cur_node                  : search key from cur_node
* snapshotid                : the snapid of key
* instance                  : the para of get_callbeck
* args                      : the para of get_callback
* key_pos                   : the position of key in batch get
* index                     : the positon of batch get node, use to release node
* batch_read                : the para of batch get(keys, key_num, key_len etc)
* get_callback              : get value callback to upper layer
*/
static void sstable_get_leaf_impl(
                                IN art_tree_t *artree,
                                IN art_node *root_node,
                                IN art_node *cur_node,
                                IN uint32_t snapshotid,
                                IN void* instance,
                                IN void *args,
                                IN int32_t key_pos,
                                IN uint32_t index,
                                IO art_batch_read_para_t* batch_read, 
                                IN ARTREE_GET_CALLBACK get_callback)
{
    uint8_t leaf_op_type                = KNotExist;      
    uint8_t *value                      = NULL;
    uint32_t value_len                  = 0;
    int32_t found_snap_id               = 0;
    bool release_flag                   = false;
    artree_ext_operations_t *ext_ops    = &artree->ops;
    artree_operations_t *art_ops        = artree->art_ops;
    art_leaf_operations_t *leaf_ops     = &art_ops->leaf_ops;
    
    found_snap_id = leaf_ops->get_snap_id(cur_node);

    if(found_snap_id == snapshotid)
    {
        /* get key succ */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree sstable get leaf succ(snapid is same), snapshotid[%d] key_pos[%d].", 
                     snapshotid, key_pos);        

        leaf_op_type = art_ops->leaf_ops.get_op_type((void*)cur_node);
         
        if(kTypeValue == leaf_op_type || kTypeDeletion == leaf_op_type)
        {
            value      = art_ops->leaf_ops.get_value((void*)cur_node);
            value_len  = art_ops->leaf_ops.get_value_len((void*)cur_node);

            /* return geted value to up level */
            get_callback(instance, args, key_pos, value, value_len, leaf_op_type, batch_read->node[index], ext_ops->release_buffer);
            release_flag = true;
            ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree get value callback succ, snapshotid[%d] key_pos[%d] leaf_type[%d] value[%p] value_len[%d].", 
                         snapshotid, key_pos, leaf_op_type, value, value_len);
        }
        else
        {
            /* return geted value to up level */
            get_callback(instance, args, key_pos, value, value_len, leaf_op_type, NULL, ext_ops->release_buffer);
        }
    }
    else
    {
        //return callback
        get_callback(instance, args, key_pos, value, value_len, leaf_op_type, NULL, ext_ops->release_buffer);
        
        /* get key fail */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "batch get artree sstable get leaf fail(snapid is diff), found_snapid[%d] snapid[%d] key_pos[%d].", 
                     found_snap_id, snapshotid, key_pos);
    }

    if((NULL == root_node) && (release_flag == false))
    {
        art_release_node(ext_ops->release_buffer, batch_read->node[index]);
    }
        
    /* this key search complete, make this node NULL */
    batch_read->node[index] = NULL;

    /* set bitmap */
    art_set_bitmap(batch_read->bitmap, key_pos, true);

    /* set complete_key number */
    batch_read->complete_key_num++;

    return;
}


/*
* description               : get next node in sstable.
* artree                    : get next level node from the artree
* root_node                 : if get first level node, so get from root node; otherwise get from batch_read->node
* keys                      : the keys of batch get
* key_len                   : key_len of key(the len of every key is same)
* snapid                    : the snapid of batch get
* key_num                   : the num of batch get key
* instance                  : the para of get_callbeck
* args                      : the para of get_callback
* batch_read                : the para of batch get(keys, key_num, key_len etc)
* get_callback              : get value callback to upper layer
*/
void sstable_get_next_node(
                    IN art_tree_t *artree,
                    IN art_node *root_node,
                    IN uint8_t **keys, 
                    IN uint32_t key_len,
                    IN uint32_t *snapid,
                    IN uint32_t key_num,
                    IN void* instance,
                    IN void *args,
                    IO art_batch_read_para_t* batch_read, 
                    IN ARTREE_GET_CALLBACK get_callback)
{
    artree_ext_operations_t *ext_ops    = &artree->ops;
    art_node *cur_node			        = NULL;
    uint32_t key_pos                    = 0;
    int32_t ret                         = kNotFound;
    uint8_t *value                      = NULL;
    uint32_t value_len                  = 0;
    uint8_t *key                        = NULL;
    uint32_t snapshotid                 = 0;
    uint32_t index                      = 0;    //use to search node one by one in batch_read->node
    
    for(key_pos = 0; key_pos < key_num; key_pos++)
    {
        key         = keys[key_pos];
        snapshotid  = snapid[key_pos];
        
        /* if the bitmap is 1, so continue */
        if(art_check_bitmap(batch_read->bitmap, key_pos) && (NULL == root_node))
        {
            continue;
        }
        
        if(NULL != root_node)
        {
            /* search the child of root_node */
            cur_node = (art_node *)art_get_buff(ext_ops->get_buffer, root_node);
        }
        else
        {
            /* search the child of node which is contained in batch_read */
            cur_node = (art_node *)art_get_buff(ext_ops->get_buffer, batch_read->node[index]);
        }

        /* search next level node from fusion_node */
        if(is_sstable_fusion_node((void *)cur_node))
        {
            (void)sstable_search_fusion_node_impl(artree, root_node, cur_node, key, key_len, key_pos, snapshotid, instance, args, index, batch_read, get_callback);
			index++;
            continue;
        }
        /* search value from leaf */
        else if(is_art_leaf_node(cur_node))
        {
            (void)sstable_get_leaf_impl(artree, root_node, cur_node, snapshotid, instance, args, key_pos, index, batch_read, get_callback);
			index++;
            continue;            
        }
        /* search next level node from sstable inner node */
        else if(is_sstable_inner_node(cur_node))
        {
            ret = sstable_search_node_info(cur_node, key, key_len, &batch_read->depth[key_pos], batch_read->offset, batch_read->size, &batch_read->index);

            if(NULL == root_node)
            {
                art_release_node(ext_ops->release_buffer, batch_read->node[index]);
            }
            
            if(kOk != ret)
            {
                /* this key search complete, make this node NULL */
                batch_read->node[index] = NULL;

                /* set bitmap */
                art_set_bitmap(batch_read->bitmap, key_pos, true);

                /* set complete_key number */
                batch_read->complete_key_num++;
                
                /* return callback */
                get_callback(instance, args, key_pos, value, value_len, KNotExist, NULL, ext_ops->release_buffer);

				//update index
				index++;

				continue;
            }

			//update index
			index++;

            /* set depth */
            batch_read->depth[key_pos]++;
            continue;
        }
        /* search next level node from sstable snap inner node */
        else if(is_sstable_snap_inner_node((art_snap_node*)cur_node))
        {       
            ret = sstable_snap_search_node_info((art_snap_node*)cur_node, snapshotid, SNAPID_LEN, &batch_read->snap_depth[key_pos],
                                                &batch_read->offset[batch_read->index], &batch_read->size[batch_read->index],
                                                &batch_read->index);

            if(NULL == root_node)
            {
                art_release_node(ext_ops->release_buffer, batch_read->node[index]);
            }
            
            if(kOk != ret)
            {
                /* this key search complete, make this node NULL */
                batch_read->node[index] = NULL;

                /* set bitmap */
                art_set_bitmap(batch_read->bitmap, key_pos, true);

                /* set complete_key number */
                batch_read->complete_key_num++;
            
                /* return callback */
                get_callback(instance, args, key_pos, value, value_len, KNotExist, NULL, ext_ops->release_buffer);

				//update index
				index++;

				continue;
            }

			//update index
			index++;

            /* set depth */
            batch_read->snap_depth[key_pos]++;
            continue;            
        }
        else
        {
#ifdef WIN32_DEBUG
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", cur_node->node_type);              
#else
            INDEX_LOG_ERROR_LIMIT("ART unknown node type %d.", cur_node->node_type);
#endif
            /* FAIL */
            ART_ASSERT(0);
        }
    }
}

/*
* description               : batch search in sstable.
* artree                    : batch get in artree
* node                      : batch get from the node in artree
* para                      : the para from gc module
* get_callback              : return value to upper
* batch_read_plog_callback  : interface of batch get node from rocache
*/
int32_t sstable_batch_get(
            art_tree_t *artree, 
            art_node *node, 
            artree_batch_get_para_t *para, 
            ARTREE_GET_CALLBACK get_callback,
            BATCH_READ_PLOG_FUNC batch_read_plog_callback)
{ 
   int32_t ret                      = kOk;
   uint32_t number                  = para->keys_num; 
   uint32_t key_len                 = para->key_len;
   uint64_t time                    = 0;
   art_batch_read_para_t *batch_read = NULL;
   artree_ext_operations_t *ext_ops = &artree->ops;

   start_comm_ftds(ART_BATCH_GET_SS_KEYS, &time);

   LVOS_TP_START(ART_TP_BATCH_GET_ALLOC_READ_MEM, &batch_read)
   LVOS_TP_START(ART_TP_BATCH_GET_ALLOC_MEM, &batch_read)
   batch_read = (art_batch_read_para_t *)art_alloc_structure(artree->apply_type, BATCH_GET_ARTREE, 1, sizeof(art_batch_read_para_t), FlushLevelType_Normal);
   LVOS_TP_END
   LVOS_TP_END
   if(NULL == batch_read)
   {
    	art_release_node(ext_ops->release_buffer, node);
        end_comm_ftds(ART_BATCH_GET_SS_KEYS, kOutOfMemory, time);
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable alloc batch_read_para mem fail.");
        return kOutOfMemory;
   }
   
   /* use root_node to search next level node */
   sstable_get_next_node(artree, node, para->keys, key_len, para->snapshotid, number, 
                    para->instance, para->args, batch_read, get_callback);

    /* non_complete_key_num(batch_read->index) + complete_key_num = keys_number(number) 
    * if not equal, so get next node logical err
    */
    if(batch_read->index + batch_read->complete_key_num != number)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree batch_read->index(%u) add complete_key_num(%u) is not same, number.", 
                    batch_read->index, batch_read->complete_key_num, number);
    }
        
   /* some keys not found, need to continue to batch get */
   if(batch_read->complete_key_num < number)
   {
        /* batch read node from rocache */
        LVOS_TP_START(ART_TP_BATCH_GET_READPLOG_FILE_NOT_FOUND, &ret)
        ret = batch_read_plog_callback(para->file, batch_read->node, batch_read->offset, batch_read->size, batch_read->index);
        LVOS_TP_END
        if(kOk != ret)
        {
        	 ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable first get node get plog fail, file[%p] ret[%d].", para->file, ret);
             art_release_node(ext_ops->release_buffer, node);
	         art_free_structure(artree->apply_type, BATCH_GET_ARTREE, (void*)batch_read);
             end_comm_ftds(ART_BATCH_GET_SS_KEYS, (uint32_t)ret, time);            
             return ret;            
        }
        
        /* check whether node is valid */
        ret = check_node_valid(batch_read->index, batch_read->node, ext_ops->release_buffer);
        if(kOk != ret)
        {
        	 ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable first get node get plog fail, file[%p], batch_read->index[%u].", para->file, batch_read->index);
             art_release_node(ext_ops->release_buffer, node);
	         art_free_structure(artree->apply_type, BATCH_GET_ARTREE, (void*)batch_read);
             end_comm_ftds(ART_BATCH_GET_SS_KEYS, (uint32_t)ret, time);            
             return ret;
        }
   }

   while(batch_read->complete_key_num < number)
   {
        /* batch read num set */
        batch_read->index = 0;
           
        /* use batch_read->node to search next level node */
        sstable_get_next_node(artree, NULL, para->keys, key_len, para->snapshotid, number, 
                        para->instance, para->args, batch_read, get_callback);

        /* non_complete_key_num(batch_read->index) + complete_key_num = keys_number(number) 
        * if not equal, so get next node logical err
        */
        if(batch_read->index + batch_read->complete_key_num != number)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree batch_read->index(%u) add complete_key_num(%u) is not same, number.", 
                        batch_read->index, batch_read->complete_key_num, number);
        }
        
		if (batch_read->complete_key_num < number)
		{
		   /* batch read node from rocache */
		   ret = batch_read_plog_callback(para->file, batch_read->node, batch_read->offset, batch_read->size, batch_read->index);
           if(kOk != ret)
           {
            	ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable first get node get plog fail, file[%p], ret[%d].", para->file, ret);
                art_release_node(ext_ops->release_buffer, node);
    	        art_free_structure(artree->apply_type, BATCH_GET_ARTREE, (void*)batch_read);
                end_comm_ftds(ART_BATCH_GET_SS_KEYS, (uint32_t)ret, time);            
                return ret;            
           }
                   
           /* check whether node is valid */
           ret = check_node_valid(batch_read->index, batch_read->node, ext_ops->release_buffer);
           if(kOk != ret)
           {
        	   ART_LOGERROR(ART_LOG_ID_4B00, "batch get artree sstable first get node get plog fail, file[%p], batch_read->index[%u].", para->file, batch_read->index);            
               art_release_node(ext_ops->release_buffer, node);
			   art_free_structure(artree->apply_type, BATCH_GET_ARTREE, (void*)batch_read);
               end_comm_ftds(ART_BATCH_GET_SS_KEYS, (uint32_t)ret, time);               
               return ret;
           }
        }
   }

   art_release_node(ext_ops->release_buffer, node);
   art_free_structure(artree->apply_type, BATCH_GET_ARTREE, (void*)batch_read); 
   end_comm_ftds(ART_BATCH_GET_SS_KEYS, (uint32_t)ret, time);
    
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
int32_t art_sstable_search_io(
    IN art_tree_t* t,
    IN const uint8_t* key,
    IN uint32_t key_len,
    IN void *args,
    IN uint32_t snapshotid,
    IN uint32_t type,
    OUT art_data** data,
    OUT bool* is_fusion_node_leaf,
    OUT void** fusion_origin_node,
    OUT uint32_t* value_len,
    OUT art_snap_node** snap_node)
{
    uint32_t  prefix_len         = 0;
    int32_t   depth              = 0;
    art_node* node               = NULL;
    int       child_idx          = 0;
    void*     origin_node        = NULL;
    void*     origin_child       = NULL;
    int32_t   ret                = kOk;
    artree_ext_operations_t *ops = NULL;
    
    ops = get_artree_ext_ops(t);
    LVOS_TP_START(ART_YP_GET_ROOT_READPLOG_FILE_NOT_FOUND, &ret)
    LVOS_TP_START(ART_TP_GET_AND_SEEK_INNERNODE_READPLOG_FILE_NOT_FOUND, &ret)    
    ret = get_root_node(t, args, &origin_node);
    LVOS_TP_END
    LVOS_TP_END    
    if (kOk != ret)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "get root fail ret=%d", ret);
        return ret;
    }
    node = (art_node*)art_get_buff(ops->get_buffer, origin_node);
    
    while (node)
    {
        /*search into a fusion node*/
        if (is_sstable_fusion_node(node))
        {
            LVOS_TP_START(ART_TP_GET_FUSION_READPLOG_FILE_NOT_FOUND, &ret)
            LVOS_TP_START(ART_TP_GET_AND_SEEK_FUSIONNODE_READPLOG_FILE_NOT_FOUND, &ret)
            ret = search_fusion_node(node, &origin_child, key, key_len, is_fusion_node_leaf, &depth, data, value_len, ops->read, args);
            LVOS_TP_END
            LVOS_TP_END    
            if (ret != kOk)
            {
                art_release_node(ops->release_buffer, origin_node);
                INDEX_LOG_INFO_LIMIT("sstable search fusion node read plog fail or not found key_len[%u] value_len[%u] depth[%d], ret[%d].", 
                                     key_len, value_len, depth, ret);
                return ret;
            }
            
            *fusion_origin_node = origin_node;
            //art_release_node(ops->release_buffer, origin_node);
            *snap_node = origin_child;
            break;
        }
        
        /* search into snap_tree */
        if ((!is_art_inner_node(node)) && (depth == key_len))
        {            
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable search node[%d] key_len[%u] succ in snap tree.", node->node_type, key_len);
            *snap_node = origin_child;
            break;
        }
        
#ifdef ART_DEBUG
        ART_ASSERT(node->node_crc == ART_ROOT_CRC);
#endif
		/* check snapid range */
		if ((GetType == type) && (is_art_inner_node(node)) && (kPelagoDBSystemTable != t->apply_type))
		{
			ret = art_check_snapid_range(snapshotid, node, t);
			if (kOk != ret)
			{
                art_release_node(ops->release_buffer, origin_node);
				ART_LOGDEBUG(ART_LOG_ID_4B00, "memtable search key not found, foundSnapid[%u] is more than maxSnapid key_len[%u] depth[%u].",
					snapshotid, key_len, depth);
				return ret;
			}
		}

        if (node->partial_len)
        {
            prefix_len = (uint32_t)check_prefix(node, key, key_len, depth);

            /* the prefix does not match */
            if (prefix_len != node->partial_len)
            {
                /* not find, return NULL */
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable the common prefix_len[%d] is not equal as node->partial_len[%d].", 
                             prefix_len, node->partial_len);
                
                art_release_node(ops->release_buffer, origin_node);
                ret = kNotFound;
                break;
            }

            depth = depth + node->partial_len;
        }
        /*we get an sstable end child*/
        if (depth == key_len)
        {
            LVOS_TP_START(ART_TP_GET_END_READPLOG_FILE_NOT_FOUND, &ret)
            LVOS_TP_START(ART_TP_GET_AND_SEEK_INNERNODE_READPLOG_FILE_NOT_FOUND, &ret)    
            ret = sstable_get_end_child(node, ops->read, args, &origin_child);
            LVOS_TP_END
            LVOS_TP_END    
			if (ret != kOk)
			{
                art_release_node(ops->release_buffer, origin_node);
				ART_LOGERROR(ART_LOG_ID_4B00, "sstable search end node read plog or not found fail, ret[%d].", ret);
				return ret;
			}

            art_release_node(ops->release_buffer, origin_node);
            *snap_node = origin_child;
            ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable find an end child key_len[%u] succeed.", 
                         key_len);
            break;
        }
        
        /* Recursively search, return first pointer */
        LVOS_TP_START(ART_TP_GET_INNER_READPLOG_FILE_NOT_FOUND, &ret)
        LVOS_TP_START(ART_TP_GET_AND_SEEK_INNERNODE_READPLOG_FILE_NOT_FOUND, &ret)
        ret = sstable_find_child(node, key[depth], ops->read, args, &origin_child, &child_idx);
        LVOS_TP_END
        LVOS_TP_END
        if (ret != kOk)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "sstable find child is NULL,child_idx=[%d].", child_idx);
            art_release_node(ops->release_buffer, origin_node); 
            return ret;
        }
        art_release_node(ops->release_buffer, origin_node);           
        origin_node = origin_child;
        node = (art_node*)art_get_buff(ops->get_buffer, origin_node);

        depth++;
    }
    return ret;
}

int32_t sstable_snap_search(
    IN art_tree_t* t,
    IN void* node,
    artree_get_para_t *get_para)
{
    art_snap_node* snap_node     = NULL;
    int32_t        prefix_len    = 0;
    int32_t        depth         = 0;
    void*          leaf          = NULL;
    int32_t        child_idx     = 0;
    void*          origin_node   = NULL;
    void*          origin_child  = NULL;
    int32_t        ret           = kOk;
    artree_ext_operations_t *ops = NULL;

	ops = get_artree_ext_ops(t);
    origin_node = node;
    
    snap_node = (art_snap_node*)art_get_buff(ops->get_buffer, origin_node); 
    while (snap_node)
    {
        /* the snap node is leaf */
        if (is_art_leaf_node((void*)snap_node))
        {
            leaf = (void*)snap_node;
            ret = art_get_check_and_get_leaf(t, leaf, get_para);
            if (kArtSnapIdNotFound == ret || kArtGetIsDeleted == ret)
            {
                art_release_node(ops->release_buffer, origin_node);
                return ret;
            }                            
            /*find then return*/
            get_para->buffer = origin_node;
            return ret;
        }
        
        if (snap_node->partial_len)
        {
            prefix_len = snap_check_prefix(snap_node, get_para->snapshotid, SNAPID_LEN, depth);

            /* the prefix does not match */
            if (prefix_len != snap_node->partial_len)
            {
                art_release_node(ops->release_buffer, origin_node);
                
                ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable snap search snapid[%u] fail snap prefix is not match prefix_len[%d] snap_node->partial_len[%d].",
                               get_para->snapshotid, prefix_len, snap_node->partial_len);
                return kArtSnapIdNotFound;
            }

            depth = depth + snap_node->partial_len;
        }

        LVOS_TP_START(ART_TP_GET_SNAP_READPLOG_FILE_NOT_FOUND, &ret)
        ret = snap_sstable_find_child(t, snap_node, GET_CHAR_FROM_INT(get_para->snapshotid, ((SNAPID_LEN - 1) - depth)), ops->read, get_para->file, &origin_child, &child_idx);
        LVOS_TP_END
        if (ret != kOk)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "snap sstable find child is NULL,child_idx=[%d].", child_idx);
            art_release_node(ops->release_buffer, origin_node); 
            return ret;
        }

        art_release_node(ops->release_buffer, origin_node); 
        
        origin_node = origin_child;
        snap_node = (art_snap_node*)art_get_buff(ops->get_buffer, origin_node);
        depth++;
    }
    
    ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable snap search snapid[%u] fail find a null snap child.",
                   get_para->snapshotid);
    return kArtSnapIdNotFound;
}

#endif

#if DESC("sstable get max&min key interface")

int32_t sstable_min_key_io(
    IN art_tree_t* t,
    IN void* origin_node,
    IN uint32_t key_buff_len,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args,
    OUT uint8_t* key,
    OUT uint32_t* index)
{
    int32_t   ret                       = kOk;
    uint32_t  idx_in_parent             = 0;
    art_node* node                      = NULL;
    void*     child_origin_node         = NULL;
    void*     tmp_origin_node           = origin_node;
	artree_ext_operations_t* ops        = get_artree_ext_ops(t);
    
    while (NULL != tmp_origin_node)
    {
        node = (art_node*)get_buff_from_origin_node(t, tmp_origin_node);

        if (node == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get buff return NULL.");
            return kIOError;
        }
        
        if (!(node->node_type >= SSTABLE_ART_NODE4 && node->node_type <= SSTABLE_ART_FUSION_NODE256))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "sstable_min_key_recursive failed, node_type %d.", node->node_type);
            return kIOError;
        }

        //  if child is leaf or snap_inner_node,return
        if (is_art_leaf_node(node)||is_snap_inner_node((art_snap_node*)node))
        {        
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kOk; 
        }

        // node must be inner_node
        if (key_buff_overflow(key_buff_len, index, node))
        {
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kInvalidArgument;
        }
                
        copy_partial_key(node, key, index);
        if (is_sstable_fusion_node(node))
        {
            key[*index] = sstable_min_key_fusion_node(node);
            (*index)++;
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kOk;
        }
        ret = sstable_get_node_left_child(node, read_plog_callback, args, &child_origin_node, &idx_in_parent);
        if (kOk != ret)
        {
            art_release_node(ops->release_buffer, tmp_origin_node);
            return ret;
        }
        if (END_POSITION_INDEX != idx_in_parent)
        {
            key[*index] = art_get_key_by_idx(node, idx_in_parent);
            (*index)++;
        }
        
        if (child_origin_node == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "read plog return NULL.");
            return kIOError;
        }
        
        art_release_node(ops->release_buffer, tmp_origin_node);

        tmp_origin_node = child_origin_node;
    }
    
    return kOk;

}

int32_t sstable_io_get_max_key(
    IN art_tree_t* t,
    IN void* origin_node,
    IN uint32_t key_buff_len,
    IN READ_PLOG_FUNC read_plog_callback,
    IN void *args,
    OUT uint8_t* key,
    OUT uint32_t* index)
{
    int32_t   ret                       = kOk;
    uint32_t  idx_in_parent             = 0;
    art_node* node                      = NULL;
    void* child_origin_node             = NULL;
    void* tmp_origin_node               = origin_node;
	artree_ext_operations_t* ops        = get_artree_ext_ops(t);

    while (NULL != tmp_origin_node)
    {
        node = (art_node*)get_buff_from_origin_node(t, tmp_origin_node);
        if (node == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "get buff return NULL.");
            return kIOError;
        }
        
        if (!(node->node_type >= SSTABLE_ART_NODE4 && node->node_type <= SSTABLE_ART_FUSION_NODE256))
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "sstable_max_key_recursive failed, node_type %d.", node->node_type);
            return kIOError;
        }

        //  if child is leaf or snap_inner_node,return
        if (is_art_leaf_node(node)||is_snap_inner_node((art_snap_node*)node))
        {        
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kOk; 
        }

        // node must be inner_node
        if (key_buff_overflow(key_buff_len, index, node))
        {
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kInvalidArgument;
        }
                
        copy_partial_key(node, key, index);
        if (is_sstable_fusion_node(node))
        {
            key[*index] = sstable_max_key_fusion_node(node);
            (*index)++;
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kOk;
        }
        ret = sstable_get_node_right_child(node, read_plog_callback, args, &child_origin_node, &idx_in_parent);
        if (kOk != ret)
        {
            art_release_node(ops->release_buffer, tmp_origin_node);
            return ret;
        }
        
        if (END_POSITION_INDEX != idx_in_parent)
        {
            key[*index] = art_get_key_by_idx(node, idx_in_parent);
            (*index)++;
        }
        
        if (child_origin_node == NULL)
        {
            ART_LOGERROR(ART_LOG_ID_4B00, "read plog return NULL.");
            art_release_node(ops->release_buffer, tmp_origin_node);
            return kIOError;
        }
        
        art_release_node(ops->release_buffer, tmp_origin_node);

        tmp_origin_node = child_origin_node;
    }
    
    return kOk;
}

#endif

int32_t snap_seek_update_path(
    IN art_snap_iterator* iterator, 
    IN artree_seek_para_t *seek_para)
{
    art_tree_t* t = iterator->t;
    /* judge tree_type to select the branch */ 
    if (is_memtable_art(t))
    {
        return memtable_snap_seek_update_path(iterator, seek_para);
    }else
    {
        return sstable_snap_seek_update_path(iterator, seek_para);
    }

    return kOk; /*lint !e527*/
}

int32_t snap_seek_get_valid_leaf(
    IN art_snap_iterator* iterator,
    IN artree_seek_para_t *seek_para)
{
    int32_t  ret				= kArtSnapIdNotFound;
    void*    leaf				= NULL;

    //  loop to get valid leaf
    while (0 != iterator->path_index)
    {
        //  if is valid value,return,otherwise continue to get prev snapid
		leaf = get_buff_from_origin_node((void*)iterator->t, iterator->path[iterator->path_index - 1].node);
		ret  = art_seek_check_and_get_leaf((void*)iterator->t, leaf, seek_para);
        if (kArtSnapIdNotFound != ret)
        {
            return ret;
        }
        else
        {
            //  the leaf is invalid,continue to prev            
            ret = snap_seek_update_path(iterator, seek_para);
            if (ret != kOk)
            {
                return ret;
            }
        }
    }
    return ret;
}

void snap_seek_release_path(
    IN RELEASE_BUFFER_FUNC release_buffer,
    IN art_snap_iterator* iterator)
{
    uint32_t index              = 0;

    for (; index < iterator->path_index; index++)        
    {
        release_buffer(iterator->path[index].node);
    }
    
    return;
}

/*
*   get the prev path of snapshotid, means equal or less
*
*
*/
int32_t sstable_snap_seek_get_path(
    IN art_snap_iterator* iterator, 
    IN void* origin_node,    
    IN artree_seek_para_t *seek_para)
{
    int32_t ret              = kOk;
    art_snap_node* snap_node = (art_snap_node*)get_buff_from_origin_node(iterator->t, origin_node);
    artree_ext_operations_t* ops = &(iterator->t->ops);
    
    /* in_param judge */
    if (NULL == snap_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "snap_node is NULL. snapid[%d]", seek_para->snapshotid);
        return kArtSnapIdNotFound;
    }
    
    /* judge node_type to select different branch */
    LVOS_TP_START(ART_TP_SEEK_SNAP_READPLOG_FILE_NOT_FOUND, &ret)
    switch (snap_node->node_type)
    {
        case SSTABLE_ART_SNAPNODE4:
            ret = snap_seek_get_path_node4_implement(origin_node, snap_node->num_children - 1, iterator, seek_para);
            break;
            
        case SSTABLE_ART_SNAPNODE16:
            ret = snap_seek_get_path_node16_implement(origin_node, snap_node->num_children - 1, iterator, seek_para);
            break;

        case SSTABLE_ART_SNAPNODE48:
            ret = snap_seek_get_path_node48_implement(origin_node, NODE_256_MAX_CHILDREN_NUM - 1, iterator, seek_para);
            break;

        case SSTABLE_ART_SNAPNODE256:
            ret = snap_seek_get_path_node256_implement(origin_node, NODE_256_MAX_CHILDREN_NUM - 1, iterator, seek_para);
            break;

        case MEMTABLE_ART_LEAF:
            iterator->path[iterator->path_index].node = origin_node;
            iterator->path_index++;
            ART_LOGDEBUG(ART_LOG_ID_4B00, "snap_node[%p] id seek get path succ. snapid[%d]", snap_node, seek_para->snapshotid);
            break;

        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknown node type %d.", snap_node->node_type);              
            ART_ASSERT(0);
            ret = kIOError;
            break;
    }
    LVOS_TP_END
    if (kArtSnapIdNotFound == ret)
    {
        /* get the prev node with parent_node in path */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "snap_node[%d] id is not found in node. snapid[%d]", snap_node->node_type, seek_para->snapshotid);
        ret = sstable_snap_seek_update_path(iterator, seek_para);
        return ret;
    }
    /* P layer error */
    if(kOk != ret)
    {
        ops->release_buffer(origin_node);
    }
    return ret;
}

int32_t sstable_snap_seek_update_path(
    IN art_snap_iterator* iterator, 
    IN artree_seek_para_t *seek_para)
{
    art_snap_node* parent_node  = NULL;
    art_tree_t* t               = iterator->t;
    int child_idx               = 0;
    void* origin_node           = NULL;
    
    /*  parent node is null,return kArtSnapIdNotFound */
    if (0 == iterator->path_index)
    {
        ART_LOGDEBUG(ART_LOG_ID_4B00, "sstable seek update path param fail. snapid[%d]", seek_para->snapshotid);
        return kArtSnapIdNotFound;
    }
    
    /*  get parent node
    *  1.get error path in get_path process:the last node is parent node
    *  2.find the leaf is invalid in snap_seek process:the last node is leaf node
    */
    //parent_node = iterator->path[iterator->path_index - 1].node;
    origin_node = iterator->path[iterator->path_index - 1].node;
    parent_node = (art_snap_node*)get_buff_from_origin_node((void*)iterator->t, origin_node);
    if (is_art_leaf_node((void*)parent_node))
    {
        /*  the last node is leaf node,remove the leaf and again, must not clear the pos_in_parentNode 
        *   snapid_length mean the all inner node in path:partial+key    
        *   so remove leaf no need to update snapid_length
        */
        art_release_node(t->ops.release_buffer, origin_node);
        iterator->path[iterator->path_index - 1].node = NULL;
        iterator->path_index--;     
        //return sstable_snap_seek_update_path(iterator, snapshotid, read_plog_callback, args);        
        origin_node = iterator->path[iterator->path_index - 1].node;
        parent_node = (art_snap_node*)get_buff_from_origin_node(iterator->t, origin_node);
    }

    if(is_art_leaf_node((void*)parent_node))
    {
        ART_ASSERT(0);
        return kIOError;
    }
    //assert(!is_art_leaf_node((void*)parent_node));
    
    /* get old child idx */
    child_idx = iterator->path[iterator->path_index].pos_in_parentNode;
    child_idx--;

    /* remove old child key */
    iterator->snapid_length--;
    
    /* get prev child node */
    switch (parent_node->node_type)
    {
        case SSTABLE_ART_SNAPNODE4:            
            return snap_seek_update_node4(t, iterator, parent_node, child_idx, seek_para);
             
        case SSTABLE_ART_SNAPNODE16:
            return snap_seek_update_node16(t, iterator, parent_node, child_idx, seek_para);
            
        case SSTABLE_ART_SNAPNODE48:
            return snap_seek_update_node48(t, iterator, parent_node, child_idx, seek_para);
                        
        case SSTABLE_ART_SNAPNODE256:
            return snap_seek_update_node256(t, iterator, parent_node, child_idx, seek_para);
            
        default:
            ART_LOGERROR(ART_LOG_ID_4B00, "unknow node tyep %d.", parent_node->node_type);
            ART_ASSERT(0);
            return kIOError;
    }

    return kOk; 
}

int32_t sstable_seek_leaf_node(
    IN art_tree_t* t,
    IN void* leaf, 
    IN void* origin_node, 
    IN artree_seek_para_t *seek_para)
{
    int32_t ret = kOk;

    ret = art_seek_check_and_get_leaf(t, leaf, seek_para);
    if (kArtSnapIdNotFound == ret || kArtGetIsDeleted == ret)
    {
        /* leaf is not find, return the error code */
        art_release_node(t->ops.release_buffer, origin_node);
        seek_para->buffer = NULL;
        return ret;
    }
    
    seek_para->buffer = origin_node;
    
    return ret;
}

/*
*   use iterator to complete seek()
*   1.use art_iterator_seek_snap to get the path
*   2.use art_iterator_prev to get the
*
*/
int32_t sstable_snap_seek(
    IN art_tree_t* t,
    IN void* node,
    IN artree_seek_para_t *seek_para)
{
    art_snap_node*    snap_node  = NULL;
    artree_ext_operations_t* ops = NULL;
    void*    origin_node         = node;
    int32_t  ret                 = kOk;
    art_snap_iterator iterator;
    
    ops = &t->ops;
    snap_node = (art_snap_node*)art_get_buff(ops->get_buffer, origin_node);
    if (NULL == snap_node)
    {
        ART_LOGERROR(ART_LOG_ID_4B00, "sstable snap seek get buff[%p] fail.", snap_node);
        return kArtSnapIdNotFound;
    }
    /* special situation:only one node(leaf node) */
    if (is_art_leaf_node((void*)snap_node))
    {
        return sstable_seek_leaf_node(t, (void*)snap_node, origin_node, seek_para);
    }
    
    /* init iterator */
    init_iterator(&iterator, t);

    /* get path from sstable into iterator */
    ret = sstable_snap_seek_get_path(&iterator, origin_node, seek_para);
    if (kOk != ret)
    {   
        /* leaf is not find, return the error code */
        ART_LOGDEBUG(ART_LOG_ID_4B00, "the snapid[%d] of key seek fail, ret[%d].", seek_para->snapshotid, ret);
        snap_seek_release_path(ops->release_buffer, (void*)&iterator);
        return ret;
    }
    
    /* loop to get valid leaf */
    ret = snap_seek_get_valid_leaf((void*)&iterator, seek_para);
    if (kOk == ret)
    {
        ART_ASSERT(iterator.path_index >= 1);
        if (0 == iterator.path_index)
        {
            return kIOError;
        }
        seek_para->buffer = iterator.path[iterator.path_index - 1].node;
        iterator.path_index--;
    }
    
    snap_seek_release_path(ops->release_buffer, (void*)&iterator);
    /* if leaf is not find, return the error code */
    return ret;
}

#ifdef __cplusplus
}
#endif

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "art_pub.h"
#include "art_pri.h"
#ifndef _SCM_PCLINT_
#include "art_epoch.h"
#include <assert.h>
#endif


#ifdef __cplusplus
extern "C" {
#endif

/*
*
*   Epoch Based Reclamation
*
*/

/*  thread index全局，只需要create_thread时注册一下
*   其它每个tree维护，放在树的管理结构体里面
*/
int g_register_thread_num = 0;

/***************************************************************/

extern void reset_free_node(art_tree_t* tree, art_node* node);


void epoch_init(IN epoch_based_reclamation_t* epoch_manager)
{
    epoch_manager->g_try_free_count = 0;
    epoch_manager->globla_epoch = EPOCH_0;
    epoch_manager->total_num = 0;
    
    ART_MEMSET(epoch_manager->epoch_node_list, 0, sizeof(epoch_head_t)*EPOCH_BUTT);
    ART_MEMSET(epoch_manager->local_epoches, 0, sizeof(EPOCH_TYPE)*N_THREADS);

    return;
}

/*
*   move freed node to epoch list, wait tobe gc
*   only one write thread, no need lock/unlock
*/
void epoch_move_node(IN art_tree_t* t, IN art_node* node)
{
    epoch_head_t* epoch_node = NULL;
    epoch_based_reclamation_t* epoch_manager = &t->epoch_manager;
    epoch_head_t* list_head = &epoch_manager->epoch_node_list[epoch_manager->globla_epoch];
    
    epoch_node = (epoch_head_t*)GET_EPOCH_HEAD(node, sizeof(epoch_head_t));

    epoch_node->next = list_head->next;
    list_head->next = (void*)epoch_node;

    epoch_manager->total_num++;
    return;    
}

/*  handle the freed node in write thread*/
void art_mem_reclamation(IN art_tree_t* t, IN art_node* node)
{   
    //bool ret = TRUE;
    //epoch_based_reclamation_t* epoch_manager = &t->epoch_manager;

    
    /*  move nod to epoch list,wait tobe gc*/
    epoch_move_node(t, node);

    /*  the added mem about 0.5 ratio, so no need to recycle*/
    
    /*  try to delay gc，make a gc with free PUSH_GLOBLA_EPOCH_COUNT node*/
    /*if (PUSH_GLOBLA_EPOCH_COUNT == epoch_manager->g_try_free_count)
    {
        ret = epoch_try_update_globla_epoch(t);
        if (TRUE == ret)
        {
            //  if globla epoch++,try to gc
            epoch_try_gc(t);
        }
    }
    
    epoch_manager->g_try_free_count = (epoch_manager->g_try_free_count+1)% (PUSH_GLOBLA_EPOCH_COUNT + 1);
    */
}

#ifdef __cplusplus
}
#endif

